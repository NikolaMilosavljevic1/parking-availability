FROM python:3.11-slim

WORKDIR /app

# All system libraries required by Playwright's Chromium (Debian Trixie compatible)
# Installed manually so we skip --with-deps which pulls deprecated font packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Core tools
    wget curl ca-certificates gnupg \
    # Chromium runtime libraries
    libglib2.0-0 libnss3 libnspr4 libdbus-1-3 \
    libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libasound2 \
    libx11-6 libx11-xcb1 libxcb1 libxext6 libxfixes3 \
    libxi6 libxrender1 libxtst6 \
    libpango-1.0-0 libpangocairo-1.0-0 libcairo2 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium browser only (skip --with-deps to avoid deprecated font packages)
RUN playwright install chromium

COPY . .

CMD ["python", "main.py"]
