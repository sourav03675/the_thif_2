"""
DriveSeed link scraper API  (HTTP-first, Playwright fallback)

Speed strategy
--------------
1. Try to do every hop with plain HTTP (curl_cffi, Chrome TLS fingerprint).
   Each hop takes ~0.2-0.5 s instead of several seconds in a real browser.
2. If HTTP fails (Cloudflare challenge, JS-built form, redirect not in HTML ...)
   fall back to a headless Chromium that is launched lazily, blocks
   images/CSS/fonts/trackers and uses waits instead of fixed sleeps.
3. Variants are fetched in parallel; only the ones that failed are retried
   in the browser.
4. Successful results are cached for CACHE_TTL seconds.

Debugging
---------
Every request records a step-by-step trace (timings, status codes, URLs,
which rule found the redirect, why a variant failed ...).
  ?debug=1  -> include the trace in the response
  ?debug=2  -> also include a screenshot when the browser path fails
  errors and partial failures always include the trace automatically
  ?mode=auto|http|browser -> force a path (handy for comparing results)
  ?nocache=1 -> skip the cache
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import re
import time
import urllib.parse

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from quart import Quart, jsonify, request
from quart_cors import cors

try:
    from curl_cffi.requests import AsyncSession

    HAVE_CURL = True
except Exception:  # curl_cffi missing -> browser-only mode
    AsyncSession = None
    HAVE_CURL = False


# --------------------------------------------------------------------------- #
# Config (all overridable with environment variables)
# --------------------------------------------------------------------------- #
def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return float(default)


PROXY = os.environ.get("PROXY", "none").strip()
USE_PROXY = bool(PROXY) and PROXY.lower() != "none"
HTTP_FIRST = os.environ.get("HTTP_FIRST", "1") != "0"
VARIANT_BASE = os.environ.get("VARIANT_BASE", "https://driveseed.org").rstrip("/")
ALLOWED_HOSTS = [h.strip().lower() for h in os.environ.get("ALLOWED_HOSTS", "").split(",") if h.strip()]

HTTP_TIMEOUT = _env_float("HTTP_TIMEOUT", 6)            # seconds per HTTP request
NAV_TIMEOUT_MS = int(_env_float("NAV_TIMEOUT", 15) * 1000)  # browser navigation
LANDING_WAIT_MS = int(_env_float("LANDING_WAIT", 2) * 1000)  # wait for #landing form
REDIRECT_WAIT = _env_float("REDIRECT_WAIT", 10)          # browser: wait for redirect URL
SETTLE = _env_float("SETTLE", 0.1)                       # browser: wait for JS redirects
TOTAL_TIMEOUT = _env_float("TOTAL_TIMEOUT", 45)          # whole request
CACHE_TTL = _env_float("CACHE_TTL", 300)                 # seconds, 0 disables
CACHE_MAX = 256
MAX_BROWSER_PAGES = int(_env_float("MAX_BROWSER_PAGES", 4))
PRELAUNCH = os.environ.get("PRELAUNCH_BROWSER", "1") == "1"
VARIANT_TIMEOUT = _env_float("VARIANT_TIMEOUT", 4)        # HTTP variant fetch timeout
VARIANT_DEADLINE = _env_float("VARIANT_DEADLINE", 5)      # max time to wait for slow variants
BROWSER_VARIANT_RETRY = os.environ.get("BROWSER_VARIANT_RETRY", "0") == "1"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

CHROME_ARGS = [
    "--disable-gpu",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled",
    "--blink-settings=imagesEnabled=false",
    "--disable-extensions",
    "--disable-background-networking",
    "--mute-audio",
]

BLOCKED_TYPES = {"image", "stylesheet", "font", "media"}
BLOCKED_HOSTS = (
    "doubleclick.net", "googlesyndication.com", "google-analytics.com",
    "googletagmanager.com", "facebook.net", "hotjar.com", "clarity.ms",
)

VARIANTS = {
    "zfile": ("/zfile/{id}", "#cf_captcha > div.card-body > div > a"),
    "wfile_type1": ("/wfile/{id}?type=1", "body > div > div > div.card-body > div > div > a"),
    "wfile_type2": ("/wfile/{id}?type=2", "body > div > div > div.card-body > div > div > a"),
}

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("scraper")

app = Quart(__name__)
app = cors(app, allow_origin="*")

_pw = None
_browser = None
_context = None
_browser_lock: asyncio.Lock | None = None
_page_sem: asyncio.Semaphore | None = None
_cache: dict[str, tuple[float, dict]] = {}
_http_bad: dict[str, float] = {}   # host -> time HTTP hops last failed there
HTTP_BAD_TTL = _env_float("HTTP_BAD_TTL", 900)


# --------------------------------------------------------------------------- #
# Debug trace + errors
# --------------------------------------------------------------------------- #
class Trace:
    """Collects timed steps so a failed request can be explained."""

    def __init__(self, want_screenshot: bool = False):
        self.t0 = time.perf_counter()
        self.steps: list[dict] = []
        self.want_screenshot = want_screenshot
        self.screenshot: str | None = None

    def add(self, step: str, **info):
        entry = {"t_ms": self.elapsed_ms, "step": step}
        entry.update({k: v for k, v in info.items() if v is not None})
        self.steps.append(entry)
        log.debug("%s", entry)

    @property
    def elapsed_ms(self) -> int:
        return int((time.perf_counter() - self.t0) * 1000)

    def export(self) -> dict:
        out = {"steps": self.steps}
        if self.screenshot:
            out["screenshot_jpeg_base64"] = self.screenshot
        return out


class ScrapeError(Exception):
    def __init__(self, code: str, message: str, http_status: int = 502, **info):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.info = info


def snippet(text: str | None, n: int = 300) -> str:
    return re.sub(r"\s+", " ", text or "")[:n]


def err_text(e: BaseException) -> str:
    return f"{type(e).__name__}: {e}"[:300]


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #
_RE_SETATTR = re.compile(r'c\.setAttribute\("href","([^"]+)"\)')
_RE_LOCATION = re.compile(r'window\.location(?:\.href)?\s*=\s*"([^"]+)"')
_RE_ASSIGN = re.compile(r"""location\.assign\(["']([^"']+)["']\)""")
_SCRIPT_RULES = (
    ("script:setAttribute", _RE_SETATTR),
    ("script:window.location", _RE_LOCATION),
    ("script:location.assign", _RE_ASSIGN),
)
_RE_FILE_IN_TEXT = re.compile(
    r"""["'`]((?:https?:)?//[^"'`\s]+/(?:file|zfile|wfile)/[A-Za-z0-9_-]+/?|/(?:file|zfile|wfile)/[A-Za-z0-9_-]+/?)["'`]"""
)
_ANCHOR_HINTS = ("driveseed.org", "/zfile/", "/wfile/", "/file/")

_CHALLENGE_MARKERS = (
    "just a moment", "_cf_chl_opt", "cf-chl-bypass",
    "attention required! | cloudflare",
    "enable javascript and cookies to continue",
)


def is_challenge(status: int | None, text: str) -> bool:
    low = (text or "")[:6000].lower()
    if any(m in low for m in _CHALLENGE_MARKERS):
        return True
    return status in (403, 429, 503) and "cloudflare" in low


def clean_url(raw: str, base: str | None = None) -> str:
    u = (raw or "").strip().replace("\\/", "/")
    return urllib.parse.urljoin(base, u) if base else u


def file_id_from(url: str) -> str:
    return urllib.parse.urlparse(url).path.rstrip("/").split("/")[-1]


_RE_FILE_URL = re.compile(r"^/(?:file|zfile|wfile)/[^/]+/?$")
MAX_EXTRA_HOPS = 4


def is_file_url(url: str) -> bool:
    """True only for real file pages like /file/<id>, not intermediates like /r?key=..."""
    return bool(_RE_FILE_URL.match(urllib.parse.urlparse(url).path))


def find_redirect(soup: BeautifulSoup, strict: bool = False):
    """Python port of the in-page JS: returns (raw_url, rule_name) or None.
    strict=True only accepts anchors that look like file pages (used for
    follow-up hops so the site logo / nav links are never picked)."""
    for s in soup.find_all("script"):
        text = s.string or s.get_text() or ""
        for name, rx in _SCRIPT_RULES:
            m = rx.search(text)
            if m:
                return m.group(1), name
    el = soup.find(id="c")
    if el is not None and el.get("href"):
        return el["href"], "element#c"
    if strict:  # timer/countdown pages usually carry the target URL in a script
        for s in soup.find_all("script"):
            m = _RE_FILE_IN_TEXT.search(s.string or s.get_text() or "")
            if m:
                return m.group(1), "script:file_url"
    hints = _ANCHOR_HINTS[1:] if strict else _ANCHOR_HINTS
    for a in soup.find_all("a", href=True):
        if any(h in a["href"] for h in hints):
            return a["href"], "anchor"
    return None


def pick_link(html: str, selector: str):
    """Returns (href, source) where source explains how it was found."""
    soup = BeautifulSoup(html, "html.parser")
    try:
        el = soup.select_one(selector)
    except Exception:
        el = None
    if el is not None and el.get("href"):
        return el["href"], "selector"
    # Fallback: first anchor that is a real link and not just the site root / logo.
    for a in soup.find_all("a", href=True):
        h = a["href"].strip()
        if not h or h.startswith(("#", "javascript:", "mailto:")):
            continue
        pu = urllib.parse.urlparse(h)
        if pu.scheme not in ("http", "https"):
            continue  # relative links like /login are site navigation, not downloads
        if pu.path in ("", "/") or pu.path.lower().startswith(("/login", "/register", "/signup")):
            continue
        return h, "fallback_anchor"
    return None, "no_anchor"


def page_title(soup: BeautifulSoup) -> str | None:
    return soup.title.get_text(strip=True)[:120] if soup.title else None


def js_field_value(scripts: str, name: str):
    """Find a value a page script assigns to a form field (JS-populated tokens)."""
    n = re.escape(name)
    pats = (
        rf"""getElementsByName\(\s*["']{n}["']\s*\)\s*\[\s*0\s*\]\s*\.value\s*=\s*["']([^"']+)["']""",
        rf"""name=["']?{n}["']?\]?["')\s\]]*\)?\s*\.(?:val\(|value\s*=)\s*["']([^"']+)["']""",
        rf"""["']{n}["']\s*[,:]\s*["']([^"']{{6,}})["']""",
        rf"""\.name\s*=\s*["']{n}["'][^;]*;[^;]*\.value\s*=\s*["']([^"']+)["']""",
    )
    for p in pats:
        m = re.search(p, scripts)
        if m:
            return m.group(1)
    return None


def build_form_request(form, base_url: str, scripts: str = ""):
    """Rebuild what form.submit() would send (submit buttons are not included)."""
    url = urllib.parse.urljoin(base_url, form.get("action") or base_url)
    method = (form.get("method") or "get").lower()
    data: dict[str, str] = {}
    for el in form.find_all(["input", "select", "textarea"]):
        name = el.get("name")
        if not name:
            continue
        if el.name == "select":
            opt = el.find("option", selected=True) or el.find("option")
            data[name] = (opt.get("value", opt.get_text()) if opt else "")
            continue
        if el.name == "textarea":
            data[name] = el.get_text()
            continue
        itype = (el.get("type") or "text").lower()
        if itype in ("submit", "button", "image", "file", "reset"):
            continue
        if itype in ("checkbox", "radio") and not el.has_attr("checked"):
            continue
        data[name] = el.get("value", "")
        if not data[name] and scripts:
            data[name] = js_field_value(scripts, name) or ""
    return method, url, data


# --------------------------------------------------------------------------- #
# HTTP path
# --------------------------------------------------------------------------- #
async def http_request(session, method, url, step, trace, fail_code, **kw):
    t = time.perf_counter()
    try:
        r = await session.request(method, url, timeout=HTTP_TIMEOUT, allow_redirects=True, **kw)
    except Exception as e:
        trace.add(step, via="http", ok=False, url=url, ms=int((time.perf_counter() - t) * 1000), error=err_text(e))
        raise ScrapeError(fail_code, f"{step}: {err_text(e)}", 502, step=step, url=url)
    text = r.text or ""
    final = str(r.url)
    challenge = is_challenge(r.status_code, text)
    trace.add(
        step, via="http", status=r.status_code, url=final, bytes=len(text),
        ms=int((time.perf_counter() - t) * 1000), challenge=True if challenge else None,
    )
    if challenge:
        raise ScrapeError(
            "BLOCKED_BY_CHALLENGE", f"{step}: Cloudflare-style challenge page", 503,
            step=step, status=r.status_code, snippet=snippet(text),
        )
    return r, text


async def http_hops(session, start_url, trace):
    r, html = await http_request(session, "GET", start_url, "http:start", trace, "START_URL_FAILED")
    soup = BeautifulSoup(html, "html.parser")

    form = soup.find("form", id="landing")
    if form is not None:
        scripts = "\n".join((sc.string or sc.get_text() or "") for sc in soup.find_all("script"))
        method, action, data = build_form_request(form, str(r.url), scripts)
        trace.add(
            "http:landing_form", method=method, action=action,
            fields={k: (v[:30] + ("..." if len(v) > 30 else "")) for k, v in data.items()},
            form_html=snippet(str(form), 500), scripts=snippet(scripts, 900),
            cookies=sorted(session.cookies.keys()) if hasattr(session, "cookies") else None,
        )
        kw = {"headers": {"Referer": str(r.url)}}
        kw["data" if method == "post" else "params"] = data
        r, html = await http_request(
            session, "POST" if method == "post" else "GET", action,
            "http:landing_submit", trace, "START_URL_FAILED", **kw,
        )
        soup = BeautifulSoup(html, "html.parser")
    else:
        trace.add("http:landing_form", present=False)

    found = find_redirect(soup)
    if not found:
        raise ScrapeError(
            "REDIRECT_NOT_FOUND", "Redirect URL not found in HTML (maybe built by JavaScript)", 504,
            via="http", page_url=str(r.url), title=page_title(soup), snippet=snippet(html),
        )
    raw, source = found
    redirect_url = clean_url(raw, str(r.url))
    trace.add("http:redirect_found", rule=source, url=redirect_url)

    r2, html2 = await http_request(
        session, "GET", redirect_url, "http:redirect_page", trace, "REDIRECT_LOAD_FAILED",
        headers={"Referer": str(r.url)},
    )
    final_url = str(r2.url)
    for i in range(MAX_EXTRA_HOPS):
        if is_file_url(final_url):
            break
        found = find_redirect(BeautifulSoup(html2, "html.parser"), strict=True)
        if not found:
            raise ScrapeError(
                "REDIRECT_NOT_FOUND", "Stuck on intermediate page (redirect probably built by JavaScript)", 504,
                via="http", page_url=final_url, snippet=snippet(html2),
            )
        nxt = clean_url(found[0], final_url)
        trace.add("http:extra_hop", n=i + 1, rule=found[1], url=nxt)
        r2, html2 = await http_request(
            session, "GET", nxt, f"http:extra_hop{i + 1}", trace, "REDIRECT_LOAD_FAILED",
            headers={"Referer": final_url},
        )
        final_url = str(r2.url)
    if not is_file_url(final_url):
        raise ScrapeError("NO_FILE_ID", "Final URL is not a file page", 502, via="http", final_url=final_url)
    return redirect_url, final_url


async def http_variant(session, key, url, selector, trace):
    t = time.perf_counter()
    try:
        r = await session.get(url, timeout=VARIANT_TIMEOUT, allow_redirects=True)
    except Exception as e:
        trace.add(f"variant:{key}", via="http", ok=False, url=url, reason="request_failed", error=err_text(e))
        return key, None
    text = r.text or ""
    if is_challenge(r.status_code, text):
        trace.add(f"variant:{key}", via="http", ok=False, url=url, status=r.status_code, reason="challenge")
        return key, None
    href, source = pick_link(text, selector)
    trace.add(
        f"variant:{key}", via="http", ok=bool(href), url=url, status=r.status_code,
        source=source, ms=int((time.perf_counter() - t) * 1000),
        reason=None if href else "no_anchor", snippet=None if href else snippet(text, 200),
    )
    return key, href


# --------------------------------------------------------------------------- #
# Browser path (lazy, filtered, wait-based)
# --------------------------------------------------------------------------- #
EXTRACT_JS = r"""(() => {
    for (const s of document.scripts) {
        const t = s.textContent || "";
        let m;
        if ((m = /c\.setAttribute\("href","([^"]+)"\)/.exec(t))) return [m[1], "script:setAttribute"];
        if ((m = /window\.location(?:\.href)?\s*=\s*"([^"]+)"/.exec(t))) return [m[1], "script:window.location"];
        if ((m = /location\.assign\(["']([^"']+)["']\)/.exec(t))) return [m[1], "script:location.assign"];
    }
    const cEl = document.getElementById("c");
    if (cEl && cEl.getAttribute("href")) return [cEl.getAttribute("href"), "element#c"];
    for (const a of document.querySelectorAll("a[href]")) {
        const h = a.getAttribute("href");
        if (!h) continue;
        if (h.includes("driveseed.org") || h.includes("/zfile/") ||
            h.includes("/wfile/") || h.includes("/file/")) return [h, "anchor"];
    }
    return null;
})();"""

EXTRACT_JS_STRICT = EXTRACT_JS.replace('h.includes("driveseed.org") || ', "")

FILE_URL_WAIT = _env_float("FILE_URL_WAIT", 12)  # browser: wait for /file/<id> URL

SUBMIT_JS = "()=>{const f=document.getElementById('landing');if(f&&f.submit)f.submit();}"


async def _route_filter(route):
    try:
        req = route.request
        if req.resource_type in BLOCKED_TYPES or any(h in req.url for h in BLOCKED_HOSTS):
            await route.abort()
        else:
            await route.continue_()
    except Exception:
        pass


async def get_context(trace: Trace | None = None):
    """Launch Chromium on first use (and again if it ever crashes)."""
    global _pw, _browser, _context
    async with _browser_lock:
        if _browser is not None and _browser.is_connected() and _context is not None:
            return _context
        t = time.perf_counter()
        if _pw is None:
            _pw = await async_playwright().start()
        if _browser is not None:
            try:
                await _browser.close()
            except Exception:
                pass
        kwargs = {"headless": True, "args": CHROME_ARGS}
        if USE_PROXY:
            kwargs["proxy"] = {"server": PROXY}
        _browser = await _pw.chromium.launch(**kwargs)
        _context = await _browser.new_context(user_agent=UA, viewport={"width": 1280, "height": 720})
        await _context.route("**/*", _route_filter)
        ms = int((time.perf_counter() - t) * 1000)
        log.info("Browser launched in %d ms (proxy: %s)", ms, PROXY if USE_PROXY else "none")
        if trace:
            trace.add("browser:launched", ms=ms)
        return _context


async def attach_screenshot(page, trace: Trace):
    if not trace.want_screenshot or trace.screenshot:
        return
    try:
        buf = await page.screenshot(type="jpeg", quality=40, timeout=3000)
        trace.screenshot = base64.b64encode(buf).decode()
    except Exception as e:
        trace.add("browser:screenshot_failed", error=err_text(e))


async def poll_redirect(page, max_wait: float):
    deadline = time.monotonic() + max_wait
    while True:
        try:
            cand = await page.evaluate(EXTRACT_JS)
        except Exception:
            cand = None  # page is mid-navigation, try again
        if cand:
            return cand
        if time.monotonic() >= deadline:
            return None
        await asyncio.sleep(0.25)


async def browser_hops(start_url, trace):
    ctx = await get_context(trace)
    async with _page_sem:
        page = await ctx.new_page()
        try:
            t = time.perf_counter()
            try:
                resp = await page.goto(start_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            except Exception as e:
                trace.add("browser:start", ok=False, error=err_text(e))
                await attach_screenshot(page, trace)
                raise ScrapeError("START_URL_FAILED", f"Failed to load start URL: {err_text(e)}", 502)
            trace.add("browser:start", status=resp.status if resp else None, url=page.url,
                      ms=int((time.perf_counter() - t) * 1000))

            try:
                await page.wait_for_selector("#landing", state="attached", timeout=LANDING_WAIT_MS)
                has_form = True
            except Exception:
                has_form = False
            if has_form:
                navigated, note = True, None
                try:
                    async with page.expect_navigation(wait_until="domcontentloaded", timeout=6000):
                        await page.evaluate(SUBMIT_JS)
                except Exception as e:
                    navigated, note = False, err_text(e)
                trace.add("browser:landing_submit", navigated=navigated, url=page.url, note=note)
            else:
                trace.add("browser:landing_form", present=False)

            found = await poll_redirect(page, REDIRECT_WAIT)
            if not found:
                try:
                    title = await page.title()
                    html = await page.content()
                except Exception:
                    title, html = None, ""
                await attach_screenshot(page, trace)
                raise ScrapeError(
                    "REDIRECT_NOT_FOUND", "Redirect URL not found", 504,
                    via="browser", page_url=page.url, title=title,
                    challenge=is_challenge(None, html) or None, snippet=snippet(html),
                )
            raw, source = found
            redirect_url = clean_url(raw, page.url)
            trace.add("browser:redirect_found", rule=source, url=redirect_url)

            t = time.perf_counter()
            try:
                await page.goto(redirect_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            except Exception as e:
                trace.add("browser:redirect_page", ok=False, error=err_text(e))
                await attach_screenshot(page, trace)
                raise ScrapeError("REDIRECT_LOAD_FAILED", f"Failed to load redirect URL: {err_text(e)}", 502)
            await asyncio.sleep(SETTLE)  # let client-side redirects land
            trace.add("browser:redirect_page", url=page.url, ms=int((time.perf_counter() - t) * 1000))

            # The page may be an intermediate (e.g. /r?key=...&id=...) that redirects
            # again via JS / timer. Wait for a real /file/<id> URL, following any
            # extra redirect we can find on the page.
            for i in range(MAX_EXTRA_HOPS + 1):
                if is_file_url(page.url):
                    break
                try:
                    await page.wait_for_url(lambda u: is_file_url(u), timeout=FILE_URL_WAIT * 1000 if i == 0 else 4000)
                    break
                except Exception:
                    pass
                if i == MAX_EXTRA_HOPS:
                    break
                try:
                    cand = await page.evaluate(EXTRACT_JS_STRICT)
                except Exception:
                    cand = None
                if not cand:
                    break
                nxt = clean_url(cand[0], page.url)
                trace.add("browser:extra_hop", n=i + 1, rule=cand[1], url=nxt)
                try:
                    await page.goto(nxt, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
                except Exception as e:
                    trace.add("browser:extra_hop", ok=False, error=err_text(e))
                    break

            final_url = page.url
            trace.add("browser:final", url=final_url, is_file_url=is_file_url(final_url))
            if not is_file_url(final_url):
                try:
                    title, html = await page.title(), await page.content()
                except Exception:
                    title, html = None, ""
                await attach_screenshot(page, trace)
                raise ScrapeError(
                    "NO_FILE_ID", "Never reached a /file/<id> page (stuck on intermediate URL)", 502,
                    final_url=final_url, title=title,
                    challenge=is_challenge(None, html) or None, snippet=snippet(html),
                )
            return redirect_url, final_url
        finally:
            await page.close()


async def browser_variant(key, url, selector, trace):
    t = time.perf_counter()
    try:
        ctx = await get_context(trace)
        async with _page_sem:
            pg = await ctx.new_page()
            try:
                await pg.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
                try:
                    await pg.wait_for_selector(selector, state="attached", timeout=1500)
                except Exception:
                    pass
                el = await pg.query_selector(selector)
                href = await el.get_attribute("href") if el else None
                source, html = "selector", ""
                if not href:
                    html = await pg.content()
                    href, source = pick_link(html, selector)
                reason = None
                if not href:
                    reason = "challenge" if is_challenge(None, html) else "no_anchor"
                    await attach_screenshot(pg, trace)
                trace.add(
                    f"variant:{key}", via="browser", ok=bool(href), url=url, source=source,
                    ms=int((time.perf_counter() - t) * 1000), reason=reason,
                    snippet=snippet(html, 200) if reason else None,
                )
                return key, href
            finally:
                await pg.close()
    except Exception as e:
        trace.add(f"variant:{key}", via="browser", ok=False, url=url, reason="exception", error=err_text(e))
        return key, None


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
async def scrape(start_url: str, mode: str, trace: Trace) -> dict:
    if mode == "http" and not HAVE_CURL:
        raise ScrapeError("HTTP_UNAVAILABLE", "curl_cffi is not installed", 500)

    host = urllib.parse.urlparse(start_url).hostname or ""
    bad = mode == "auto" and time.time() - _http_bad.get(host, 0) < HTTP_BAD_TTL
    use_http = HAVE_CURL and HTTP_FIRST and mode in ("auto", "http") and not bad
    if bad:
        trace.add("http:skipped", reason="HTTP hops failed recently for this host", host=host)
    session = None
    try:
        hops, method = None, None
        if use_http:
            kw = {"proxies": {"http": PROXY, "https": PROXY}} if USE_PROXY else {}
            session = AsyncSession(impersonate="chrome", **kw)
            try:
                hops, method = await http_hops(session, start_url, trace), "http"
            except ScrapeError as e:
                trace.add("http:failed", code=e.code, message=e.message, **e.info)
                if e.code in ("REDIRECT_NOT_FOUND", "BLOCKED_BY_CHALLENGE", "NO_FILE_ID"):
                    _http_bad[host] = time.time()
                if mode == "http":
                    raise
            except Exception as e:
                trace.add("http:crashed", error=err_text(e))
                if mode == "http":
                    raise ScrapeError("INTERNAL", f"HTTP path crashed: {err_text(e)}", 500)
            if hops is None:
                trace.add(
                    "fallback", to="browser", reason="http path failed",
                    warning="start link may be single-use and already consumed; retry with a fresh link if the browser finds no form",
                )

        if hops is None:
            hops, method = await browser_hops(start_url, trace), "browser"

        redirect_url, final_url = hops
        file_id = file_id_from(final_url)
        if not is_file_url(final_url):
            raise ScrapeError("NO_FILE_ID", "Final URL is not a file page", 502, final_url=final_url)
        trace.add("hops_done", method=method, final_url=final_url, file_id=file_id)

        variants = {
            key: (VARIANT_BASE + path.format(id=file_id), sel)
            for key, (path, sel) in VARIANTS.items()
        }

        async def run_bounded(coros):
            """Run variant fetches in parallel; stop waiting after VARIANT_DEADLINE."""
            tasks = {asyncio.ensure_future(c): k for k, c in coros.items()}
            done, pending = await asyncio.wait(tasks, timeout=VARIANT_DEADLINE)
            for t in pending:
                t.cancel()
                trace.add(f"variant:{tasks[t]}", ok=False, reason="deadline")
            out = {k: None for k in coros}
            for t in done:
                if not t.cancelled() and t.exception() is None:
                    k, v = t.result()
                    out[k] = v
            return out

        if session is None and HAVE_CURL and mode == "auto":
            kw = {"proxies": {"http": PROXY, "https": PROXY}} if USE_PROXY else {}
            session = AsyncSession(impersonate="chrome", **kw)
            method_for_variants = "browser"
        else:
            method_for_variants = method
        if session is not None and mode != "browser":
            results = await run_bounded({k: http_variant(session, k, u, s, trace) for k, (u, s) in variants.items()})
            missing = [k for k, v in results.items() if not v]
            # Browser retry only when nothing at all came back (it costs 15s+).
            if mode == "auto" and missing and (BROWSER_VARIANT_RETRY or len(missing) == len(results)):
                trace.add("variants:browser_retry", keys=missing)
                retried = await asyncio.gather(*[browser_variant(k, *variants[k], trace) for k in missing])
                recovered = False
                for k, v in retried:
                    if v:
                        results[k], recovered = v, True
                if recovered:
                    method = method_for_variants + "+browser"
        else:
            results = await run_bounded({k: browser_variant(k, u, s, trace) for k, (u, s) in variants.items()})

        return {
            "final_url": final_url,
            "file_id": file_id,
            "download_links": results,
            "method": method,
        }
    finally:
        if session is not None:
            try:
                await session.close()
            except Exception:
                pass


# --------------------------------------------------------------------------- #
# Cache + response helpers
# --------------------------------------------------------------------------- #
def cache_get(key: str):
    item = _cache.get(key)
    if item and time.time() - item[0] < CACHE_TTL:
        return item[1]
    _cache.pop(key, None)
    return None


def cache_put(key: str, payload: dict):
    if CACHE_TTL <= 0:
        return
    _cache[key] = (time.time(), payload)
    while len(_cache) > CACHE_MAX:
        _cache.pop(next(iter(_cache)))


def host_allowed(url: str) -> bool:
    p = urllib.parse.urlparse(url)
    if p.scheme not in ("http", "https") or not p.hostname:
        return False
    if not ALLOWED_HOSTS:
        return True
    host = p.hostname.lower()
    return any(host == h or host.endswith("." + h) for h in ALLOWED_HOSTS)


def error_response(e: ScrapeError, trace: Trace, url: str):
    log.warning("FAIL %s %s -> %s: %s | steps=%s", url, f"{trace.elapsed_ms}ms", e.code, e.message, trace.steps)
    body = {
        "error": e.message,
        "code": e.code,
        "detail": e.info or None,
        "elapsed_ms": trace.elapsed_ms,
        "debug": trace.export(),
    }
    return jsonify(body), e.http_status


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
@app.before_serving
async def startup():
    global _browser_lock, _page_sem
    _browser_lock = asyncio.Lock()
    _page_sem = asyncio.Semaphore(MAX_BROWSER_PAGES)
    log.info(
        "Ready | http_available=%s http_first=%s proxy=%s cache_ttl=%ss variant_base=%s",
        HAVE_CURL, HTTP_FIRST, PROXY if USE_PROXY else "none", CACHE_TTL, VARIANT_BASE,
    )
    if PRELAUNCH:
        try:
            await get_context()
        except Exception as e:
            log.warning("Browser prelaunch failed: %s", err_text(e))


@app.after_serving
async def shutdown():
    try:
        if _context:
            await _context.close()
        if _browser:
            await _browser.close()
        if _pw:
            await _pw.stop()
    except Exception:
        pass


@app.route("/health")
async def health():
    return jsonify({
        "ok": True,
        "http_available": HAVE_CURL,
        "http_first": HTTP_FIRST,
        "browser_running": bool(_browser and _browser.is_connected()),
        "cache_entries": len(_cache),
    })


@app.route("/getlink")
async def get_link():
    start_url = (request.args.get("url") or "").strip()
    if not start_url:
        return jsonify({"error": "Missing ?url parameter", "code": "MISSING_URL"}), 400
    if not host_allowed(start_url):
        return jsonify({"error": "URL must be http(s) and on an allowed host", "code": "URL_NOT_ALLOWED"}), 400

    mode = request.args.get("mode", "auto").lower()
    if mode not in ("auto", "http", "browser"):
        return jsonify({"error": "mode must be auto, http or browser", "code": "BAD_MODE"}), 400
    debug = request.args.get("debug", "0")
    nocache = request.args.get("nocache") == "1"

    cache_key = f"{mode}|{start_url}"
    if not nocache:
        hit = cache_get(cache_key)
        if hit is not None:
            return jsonify({**hit, "cached": True})

    trace = Trace(want_screenshot=(debug == "2"))
    try:
        result = await asyncio.wait_for(scrape(start_url, mode, trace), timeout=TOTAL_TIMEOUT)
    except asyncio.TimeoutError:
        return error_response(
            ScrapeError("TOTAL_TIMEOUT", f"Gave up after {TOTAL_TIMEOUT:.0f}s", 504), trace, start_url
        )
    except ScrapeError as e:
        return error_response(e, trace, start_url)
    except Exception as e:
        log.exception("Unhandled error for %s", start_url)
        return error_response(ScrapeError("INTERNAL", err_text(e), 500), trace, start_url)

    links = result["download_links"]
    missing = [k for k, v in links.items() if not v]
    result["elapsed_ms"] = trace.elapsed_ms
    if missing:
        result["warning"] = (
            "no download links found" if len(missing) == len(links)
            else f"missing links for: {', '.join(missing)}"
        )
        log.warning("PARTIAL %s %dms missing=%s steps=%s", start_url, trace.elapsed_ms, missing, trace.steps)
    else:
        log.info("OK %s %dms via %s", start_url, trace.elapsed_ms, result["method"])
        cache_put(cache_key, dict(result))

    # trace is attached on request (?debug=1|2) and automatically on any failure
    if debug in ("1", "2") or missing:
        result["debug"] = trace.export()
    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
