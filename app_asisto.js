const { Client, LocalAuth } = require('whatsapp-web.js');
const express = require('express');
const socketIO = require('socket.io');
const qrcode = require('qrcode');
const http = require('http');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = socketIO(server, { cors: { origin: "*" } });

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Servir carpeta public
app.use(express.static(path.join(__dirname, 'public')));

// Ruta raíz apunta a public/index.html
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Sesión local en ./sessions
const SESSION_PATH = path.join(__dirname, 'sessions');
try { fs.mkdirSync(SESSION_PATH, { recursive: true }); } catch {}

const client = new Client({
  restartOnAuthFail: true,
  puppeteer: {
    headless: true,
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

// Eventos de socket
io.on('connection', (socket) => {
  socket.emit('message', 'Conectando con WhatsApp...');
});

// Eventos de WhatsApp
client.on('qr', (qr) => {
  qrcode.toDataURL(qr, (err, url) => {
    io.emit('qr', url);
    io.emit('message', 'Código QR Recibido');
  });
});

client.on('ready', () => {
  io.emit('message', 'WhatsApp Listo!');
});

client.on('authenticated', () => {
  io.emit('message', 'WhatsApp Autenticado!');
});

client.on('disconnected', (reason) => {
  io.emit('message', 'WhatsApp Desconectado: ' + reason);
  client.destroy();
  client.initialize();
});

// Inicializar cliente
client.initialize();

// Iniciar server
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('App running on *:' + PORT);
});
