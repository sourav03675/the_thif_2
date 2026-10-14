import asyncio
import urllib.parse
import os
from bs4 import BeautifulSoup

from quart import Quart, request, jsonify
from quart_cors import cors
from playwright.async_api import async_playwright

app = Quart(__name__)
app = cors(app, allow_origin="*")

_playwright = None
_browser = None
_context = None

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
]

# "none" = direct connection. Or set e.g. socks5://127.0.0.1:9150 (Tor Browser)
# or socks5://127.0.0.1:9050 (standalone Tor). Can also be set via env var.
PROXY = os.environ.get("PROXY", "none")


async def extract_redirect_url(page):
    async def snap():
        return await page.evaluate(
            r"""(() => {
    for (const s of document.scripts) {
        const t = s.textContent || "";
        let m;
        if ((m = /c\.setAttribute\("href","([^"]+)"\)/.exec(t))) return m[1];
        if ((m = /window\.location(?:\.href)?\s*=\s*"([^"]+)"/.exec(t))) return m[1];
        if ((m = /location\.assign\(["']([^"']+)["']\)/.exec(t))) return m[1];
    }
    const cEl = document.getElementById("c");
    if (cEl && cEl.getAttribute("href")) return cEl.getAttribute("href");
    for (const a of document.querySelectorAll("a[href]")) {
        const h = a.getAttribute("href");
        if (!h) continue;
        if (h.includes("driveseed.org") || h.includes("/zfile/") ||
            h.includes("/wfile/") || h.includes("/file/")) return h;
    }
    return null;
})();"""
        )

    for _ in range(20):
        try:
            candidate = await snap()
        except Exception:
            candidate = None  # page may be mid-navigation; retry
        if candidate:
            return candidate
        await asyncio.sleep(1)
    return None


async def fetch_variant(context, key, url, selector):
    pg = await context.new_page()
    try:
        await pg.goto(url, wait_until="domcontentloaded", timeout=25000)
        el = await pg.query_selector(selector)
        if el:
            href = await el.get_attribute("href")
            if href:
                return key, href
        html = await pg.content()
        soup = BeautifulSoup(html, "html.parser")
        a = soup.find("a", href=True)
        return key, (a["href"] if a else None)
    except Exception as e:
        print(f"fetch_variant {key} failed: {e}")
        return key, None
    finally:
        await pg.close()


@app.before_serving
async def init_browser():
    global _playwright, _browser, _context
    _playwright = await async_playwright().start()

    launch_kwargs = {"headless": True, "args": CHROME_ARGS}
    if PROXY and PROXY.strip().lower() != "none":
        launch_kwargs["proxy"] = {"server": PROXY.strip()}

    _browser = await _playwright.chromium.launch(**launch_kwargs)
    _context = await _browser.new_context(
        user_agent=UA, viewport={"width": 1280, "height": 720}
    )
    print(f"Browser ready (proxy: {PROXY})")


@app.after_serving
async def close_browser():
    try:
        if _context:
            await _context.close()
        if _browser:
            await _browser.close()
        if _playwright:
            await _playwright.stop()
    except Exception:
        pass


@app.route("/getlink")
async def get_link():
    start_url = request.args.get("url")
    if not start_url:
        return jsonify({"error": "Missing ?url parameter"}), 400
    if _context is None:
        return jsonify({"error": "Browser not ready"}), 500

    page = await _context.new_page()
    try:
        try:
            await page.goto(start_url, wait_until="domcontentloaded", timeout=25000)
        except Exception as e:
            return jsonify({"error": "Failed to load start URL", "detail": str(e)}), 502

        await asyncio.sleep(2)
        try:
            await page.evaluate(
                "()=>{const f=document.getElementById('landing');if(f&&f.submit)f.submit();}"
            )
        except Exception:
            pass
        await asyncio.sleep(2)

        redirect_url = await extract_redirect_url(page)
        if not redirect_url:
            return jsonify({
                "error": "Redirect not found",
                "debug_url": page.url,
                "debug_title": await page.title(),
            }), 504

        try:
            await page.goto(redirect_url, wait_until="domcontentloaded", timeout=25000)
        except Exception as e:
            return jsonify({"error": "Failed to load redirect URL", "detail": str(e)}), 502
        await asyncio.sleep(1)

        final_url = page.url
        file_id = urllib.parse.urlparse(final_url).path.split("/")[-1]

        variants = {
            "zfile": f"https://driveseed.org/zfile/{file_id}",
            "wfile_type1": f"https://driveseed.org/wfile/{file_id}?type=1",
            "wfile_type2": f"https://driveseed.org/wfile/{file_id}?type=2",
        }
        selectors = {
            "zfile": "#cf_captcha > div.card-body > div > a",
            "wfile_type1": "body > div > div > div.card-body > div > div > a",
            "wfile_type2": "body > div > div > div.card-body > div > div > a",
        }

        tasks = [fetch_variant(_context, k, v, selectors[k]) for k, v in variants.items()]
        results = dict(await asyncio.gather(*tasks))

        return jsonify({
            "final_url": final_url,
            "file_id": file_id,
            "download_links": results,
        })
    finally:
        await page.close()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)