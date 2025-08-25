/* eslint-disable */
const path = require('path');
const fs = require('fs');
const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const qrcode = require('qrcode');

const { Client, MessageMedia, LocalAuth } = require('whatsapp-web.js');

// --- Config Render / General ---
const PORT = process.env.PORT || 3000;
const SESSION_PATH = process.env.SESSION_PATH || '/data/wwebjs';
const CHROME_PATH = process.env.CHROME_PATH || '/usr/bin/chromium';

// --- Asegurar carpeta de sesión ---
try {
  fs.mkdirSync(SESSION_PATH, { recursive: true });
  console.log(`✅ Carpeta de sesión lista en: ${SESSION_PATH}`);
} catch (e) {
  console.error('❌ Error creando carpeta de sesión:', e.message);
}

// --- App & Server ---
const app = express();
const server = http.createServer(app);
const io = socketIO(server, { cors: { origin: '*' } });

app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true }));

// Static files for QR page
app.use(express.static(path.join(__dirname, 'public')));

// Health check / index
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Simple log helper
function log(msg) {
  console.log(msg);
  try {
    fs.appendFileSync(path.join(__dirname, 'server.log'), `[${new Date().toISOString()}] ${msg}\n`);
  } catch (e) {}
}

// --- WhatsApp client ---
const client = new Client({
  restartOnAuthFail: true,
  puppeteer: {
    headless: true,
    executablePath: CHROME_PATH,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--single-process',
      '--disable-gpu'
    ]
  },
  authStrategy: new LocalAuth({ dataPath: SESSION_PATH })
});

// Socket.IO bridge
io.on('connection', (socket) => {
  socket.emit('message', 'Conectando con WhatsApp...');
});

// WhatsApp events
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
  // Reintento automático
  setTimeout(() => client.initialize(), 5000);
});

client.on('message', async (msg) => {
  log(`Mensaje de ${msg.from}: ${msg.body}`);
  io.emit('message', `>> ${msg.from}: ${msg.body}`);

  // Respuesta demo
  if (msg.body.toLowerCase() === 'ping') {
    await msg.reply('pong');
    io.emit('message', `<< reply: pong`);
  }
});

// --- Simple REST endpoint to send a test message ---
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

// Start server
server.listen(PORT, '0.0.0.0', () => {
  log('App running on *:' + PORT);
});

// Initialize WhatsApp
client.initialize();
