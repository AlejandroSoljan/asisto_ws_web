# Render FREE sin Docker (Puppeteer descarga Chromium)

## Importante
- **No uses Dockerfile** en este repo. Si existe, Render forzará Docker.
- Puppeteer descargará Chromium durante `npm install`.
- Ruta de sesión: `./sessions` (sin persistencia entre reinicios).

## Pasos
1. Subí este repo a GitHub/GitLab sin Dockerfile.
2. Render → New + → Web Service → conectá el repo.
3. Environment: Node (Native).
4. Build Command: (vacío, Render usa `npm install`).
5. Start Command: `node app_asisto.js` (o dejá `npm start`).

Si antes definiste `SESSION_PATH` en Environment, **eliminala**.
