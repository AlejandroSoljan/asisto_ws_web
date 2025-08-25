/* eslint-disable */
const path = require('path');
const fs = require('fs');
const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const qrcode = require('qrcode');

const { Client, MessageMedia, LocalAuth } = require('whatsapp-web.js');

// --- Config para Render FREE sin Docker ---
// Usamos ./sessions para no depender de /data
const PORT = process.env.PORT || 3000;
const SESSION_PATH = path.join(__dirname, 'sessions');

// Asegurar carpeta de sesión local
try {
  fs.mkdirSync(SESSION_PATH, { recursive: true });
  console.log(`✅ Carpeta de sesión lista en: ${SESSION_PATH}`);
} catch (e) {
  console.error('❌ Error creando carpeta de sesión:', e.message);
}

const app = express();
const server = http.createServer(app);
const io = socketIO(server, { cors: { origin: '*' } });

app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true }));

app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

function log(msg) {
  console.log(msg);
  try {
    fs.appendFileSync(path.join(__dirname, 'server.log'), `[${new Date().toISOString()}] ${msg}\n`);
  } catch (e) {}
}

// ⚠️ Importante: NO fijamos executablePath. De esta forma, puppeteer (no puppeteer-core)
// usará el Chromium que descarga automáticamente durante npm install.
const client = new Client({
  restartOnAuthFail: true,
  puppeteer: {
    headless: true,
    // NO executablePath aquí
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--no-first-run',
      '--no-zygote',
      '--single-process'
    ]
  },
  authStrategy: new LocalAuth({ dataPath: SESSION_PATH })
});

io.on('connection', (socket) => {
  socket.emit('message', 'Conectando con WhatsApp...');
});

client.on('qr', (qr) => {
  qrcode.toDataURL(qr, (err, url) => {
    if (err) {
      log('Error generando QR: ' + err.message);
      return;
    }
    io.emit('qr', url);
    io.emit('message', 'Código QR listo. Escanealo desde tu teléfono (Dispositivos vinculados).');
  });
});

client.on('authenticated', () => {
  io.emit('message', 'WhatsApp autenticado!');
});

client.on('ready', () => {
  io.emit('message', 'WhatsApp listo!');
  log('WhatsApp ready');
});

client.on('disconnected', (reason) => {
  io.emit('message', `WhatsApp desconectado: ${reason}`);
  log('WhatsApp disconnected: ' + reason);
  setTimeout(() => client.initialize(), 5000);
});

client.on('message', async (msg) => {
  log(`Mensaje de ${msg.from}: ${msg.body}`);
  io.emit('message', `>> ${msg.from}: ${msg.body}`);
  if (msg.body.toLowerCase() === 'ping') {
    await msg.reply('pong');
    io.emit('message', `<< reply: pong`);
  }
});

app.post('/send', async (req, res) => {
  try {
    let { to, message } = req.body;
    if (!to || !message) return res.status(400).json({ ok: false, error: 'Faltan campos to/message' });
    if (!to.endsWith('@c.us')) to = `${to}@c.us`;
    await client.sendMessage(to, message);
    io.emit('message', `<< Enviado a ${to}: ${message}`);
    res.json({ ok: true });
  } catch (e) {
    log('Error /send: ' + e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

server.listen(PORT, '0.0.0.0', () => {
  log('App running on *:' + PORT);
});

client.initialize();
