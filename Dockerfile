# Use official Playwright base (includes Chromium + dependencies).
# The version here MUST match the playwright version pinned in requirements.txt.
FROM mcr.microsoft.com/playwright/python:v1.43.0-jammy

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Expose port
EXPOSE 5000

# Start server
CMD ["python", "app.py"]