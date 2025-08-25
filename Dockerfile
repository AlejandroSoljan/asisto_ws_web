FROM node:18-bullseye

# Dependencias de Chromium para puppeteer/whatsapp-web.js
RUN apt-get update && apt-get install -y   chromium   ca-certificates   fonts-liberation   libasound2   libatk-bridge2.0-0   libatk1.0-0   libatspi2.0-0   libcups2   libdbus-1-3   libxkbcommon0   libxcomposite1   libxdamage1   libxfixes3   libxrandr2   libgbm1   libgtk-3-0   libnss3   libxss1   libxi6   && rm -rf /var/lib/apt/lists/*

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=1
ENV CHROME_PATH=/usr/bin/chromium
ENV NODE_ENV=production

WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production || npm i --production

COPY . .

CMD ["node", "app_asisto.js"]
