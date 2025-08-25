# Asisto WhatsApp en Render

Este paquete incluye todo para desplegar un cliente de WhatsApp (whatsapp-web.js) en Render con QR público.

## Requisitos
- Cuenta en Render.com
- Repo en Git (subí estos archivos)
- Número de WhatsApp que quieras vincular

## Estructura
- `app_asisto.js`: servidor Express + Socket.IO + whatsapp-web.js
- `public/index.html`: página para mostrar el QR y eventos
- `Dockerfile`: imagen con Chromium del sistema
- `render.yaml`: define el servicio, disco persistente y variables
- `package.json`: dependencias
- `.gitignore`

## Despliegue rápido
1. `npm install`
2. `npm start` (local) → abre http://localhost:3000 y escaneá el QR.
3. Subí a GitHub/GitLab/Bitbucket.
4. En Render → **New +** → **Blueprint** → apuntá al repo (usa `render.yaml`).

> Importante: el disco persistente (`/data`) guarda la sesión para que no tengas que reescanear el QR en cada deploy.

## Probar envío
```
curl -X POST http://localhost:3000/send -H "Content-Type: application/json" -d '{ "to": "549351XXXXXXX", "message": "Hola desde Render!" }'
```
(Usa formato internacional. El servidor agregará `@c.us` si falta.)

## Notas
- Si el plan “duerme”, WhatsApp puede desconectarse; evalúa un plan que no hiberne.
- Respetá Términos de WhatsApp; whatsapp-web.js automatiza el cliente oficial.
- Para proteger el QR público, poné auth básica o limitá acceso a la ruta `/`.
