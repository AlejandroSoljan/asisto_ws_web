/*script:app_asisto*/
/*version: 4.04.20 29/08/2026   */
try {
  console.log(`[BOOT] app_asisto version=4.04.20 file=${__filename} pid=${process.pid}`);
} catch {}

// Baileys usa ws. Mantenemos deshabilitados los aceleradores nativos opcionales
// para evitar dependencias N-API innecesarias en las sesiones.
process.env.WS_NO_BUFFER_UTIL = '1';
process.env.WS_NO_UTF_8_VALIDATE = '1';

const dns = require("dns");

try {
  dns.setServers(["8.8.8.8", "1.1.1.1"]);
  console.log("[DNS] Servidores DNS forzados para Node:", dns.getServers().join(", "));
} catch (e) {
  console.error("[DNS] No se pudieron configurar DNS:", e.message);
}


//const chatbot = require("./funciones_asisto.js")
const { EventEmitter } = require('events');

// whatsapp-web.js / Puppeteer se cargan de forma diferida.
// Si todas las sesiones usan Baileys, una dependencia rota de Puppeteer NO debe
// impedir que el proceso arranque antes de leer wweb_engine.
let WwebClient = null;
let LocalAuth = null;
let RemoteAuth = null;
let MongoStore = null;

class AsistoCompatMessageMedia {
  constructor(mimetype, data, filename) {
    this.mimetype = String(mimetype || 'application/octet-stream');
    this.data = String(data || '');
    this.filename = filename == null ? null : String(filename);
  }
}
let MessageMedia = AsistoCompatMessageMedia;

let mongoose = null;

function getMongooseModule() {
  if (mongoose) return mongoose;
  mongoose = require('mongoose');
  return mongoose;
}

const os = require('os');
const crypto = require('crypto');
const express = require('express');
const socketIO = require('socket.io');
const qrcode = require('qrcode');
const http = require('http');
// ODBC es opcional y nativo. No se carga al iniciar cada proceso;
// se carga únicamente cuando una función realmente necesita acceder al DSN.
let odbc = null;
let odbcLoadAttempted = false;

function getOdbcModule() {
  if (odbc) return odbc;
  if (odbcLoadAttempted) return null;
  odbcLoadAttempted = true;
  try {
    odbc = require("odbc");
    return odbc;
  } catch (e) {
    odbc = null;
    try { console.log('[ODBC] módulo no disponible:', e?.message || e); } catch {}
    return null;
  }
}
const fetch = require('node-fetch');
const fileUpload = require('express-fileupload');
const axios = require('axios');

const mime = require('mime-types');


//const { OdbcError } = require('odbc');

const fs = require('fs');
const path = require('path');
const { spawn, fork } = require('child_process');
// Multi-sesión se ejecuta con procesos Node hijos, no Worker Threads.
// Mantener estos valores evita tocar el resto de la lógica que distingue supervisor/hijo.
const isMainThread = true;
const threadId = 0;
let mongoConnectingPromise = null;


let wwebJsRuntimeLoadPromise = null;


function tryLoadWwebJsRuntimeSync() {
  const wweb = require('whatsapp-web.js');
  if (!wweb || typeof wweb.Client !== 'function') throw new Error('whatsapp_web_js_client_missing');

  WwebClient = wweb.Client;
  MessageMedia = wweb.MessageMedia || AsistoCompatMessageMedia;
  LocalAuth = wweb.LocalAuth;
  RemoteAuth = wweb.RemoteAuth;

  if (typeof LocalAuth !== 'function' || typeof RemoteAuth !== 'function') {
    throw new Error('whatsapp_web_js_auth_classes_missing');
  }

  return true;
}




async function ensureWwebJsRuntimeLoaded() {
  if (WwebClient && LocalAuth && RemoteAuth) return true;
  if (wwebJsRuntimeLoadPromise) return wwebJsRuntimeLoadPromise;

  wwebJsRuntimeLoadPromise = Promise.resolve().then(() => {
    tryLoadWwebJsRuntimeSync();
    return true;
  }).catch((e) => {
    wwebJsRuntimeLoadPromise = null;
    throw e;
  });

  return wwebJsRuntimeLoadPromise;
}

async function ensureWwebMongoStoreLoaded() {
  if (MongoStore) return MongoStore;
  await ensureWwebJsRuntimeLoaded();
  const mod = require('wwebjs-mongo');
  MongoStore = mod?.MongoStore || mod?.default?.MongoStore || null;
  if (typeof MongoStore !== 'function') throw new Error('wwebjs_mongo_store_missing');
  return MongoStore;
}
 

// ============================================================================
// Baileys compatibility layer
// - Se usa solo cuando wweb_engine=baileys; whatsapp-web.js sigue disponible.
// - Mantiene la interfaz que usa el resto de app_asisto_ws (MessageMedia,
//   eventos message/message_create/message_ack/ready/qr, etc.) para evitar
//   reescribir la lógica de negocio.
// ============================================================================
let baileysModulePromise = null;
let baileysInstallPromise = null;

const BAILEYS_NPM_PACKAGE = 'baileys';
const BAILEYS_NPM_VERSION = '7.0.0-rc14';


function resolveInstalledBaileysEntry(packageName) {
  try {
    return require.resolve(String(packageName || ''), { paths: [__dirname] });
  } catch {
    return '';
  }
}


function baileysAutoInstallEnabled() {
  const raw = process.env.ASISTO_BAILEYS_AUTO_INSTALL ?? process.env.BAILEYS_AUTO_INSTALL;
  if (raw === undefined || raw === null || String(raw).trim() === '') return true;
  return !['0', 'false', 'no', 'off'].includes(String(raw).trim().toLowerCase());
}


async function installBaileysDependencyIfMissing() {
  const existing = resolveInstalledBaileysEntry(BAILEYS_NPM_PACKAGE);
  if (existing) return existing;

  if (!baileysAutoInstallEnabled()) {
    const err = new Error(`Baileys no está instalado y la instalación automática está deshabilitada (${BAILEYS_NPM_PACKAGE}@${BAILEYS_NPM_VERSION}).`);
    err.code = 'BAILEYS_AUTO_INSTALL_DISABLED';
    throw err;
  }

  if (!baileysInstallPromise) {
    baileysInstallPromise = (async () => {
      const packageSpec = `${BAILEYS_NPM_PACKAGE}@${BAILEYS_NPM_VERSION}`;
      const startMsg = `[BAILEYS] módulo faltante; instalando automáticamente ${packageSpec} en ${__dirname}`;
      try { console.log(startMsg); } catch {}
      try { if (typeof EscribirLog === 'function') EscribirLog(startMsg, 'event'); } catch {}

      try {
        await runCommand(
          'npm',
          ['install', packageSpec, '--no-save', '--no-package-lock', '--omit=dev', '--no-audit', '--no-fund'],
          { cwd: __dirname, timeout: 10 * 60_000 }
        );
      } catch (e) {
        const detail = String(e?.stderr || e?.stdout || e?.message || e || '').trim().slice(0, 2000);
        const err = new Error(`No se pudo instalar automáticamente ${packageSpec}: ${detail || 'npm_install_failed'}`);
        err.code = 'BAILEYS_AUTO_INSTALL_FAILED';
        err.cause = e;
        throw err;
      }

      const entry = resolveInstalledBaileysEntry(BAILEYS_NPM_PACKAGE);
      if (!entry) {
        const err = new Error(`npm finalizó pero ${packageSpec} sigue sin aparecer en node_modules.`);
        err.code = 'BAILEYS_AUTO_INSTALL_NOT_FOUND';
        throw err;
      }

      const okMsg = `[BAILEYS] instalación automática completada package=${packageSpec} entry=${entry}`;
      try { console.log(okMsg); } catch {}
      try { if (typeof EscribirLog === 'function') EscribirLog(okMsg, 'event'); } catch {}
      return entry;
    })().finally(() => {
      baileysInstallPromise = null;
    });
  }

  return baileysInstallPromise;
}




async function importInstalledBaileysModule() {
  // Preferimos el paquete actual "baileys". El nombre histórico queda como fallback
  // para instalaciones viejas que todavía lo tengan en node_modules.
  const candidates = [BAILEYS_NPM_PACKAGE, '@whiskeysockets/baileys'];

  for (const packageName of candidates) {
    const entry = resolveInstalledBaileysEntry(packageName);
    if (!entry) continue;

    try {
      const mod = await import(packageName);
      try {
        const msg = `[BAILEYS] módulo cargado package=${packageName} entry=${entry}`;
        console.log(msg);
        if (typeof EscribirLog === 'function') EscribirLog(msg, 'event');
      } catch {}
      return mod;
    } catch (e) {
      // El paquete existe físicamente: si falla el import NO lo tratamos como
      // "no instalado", porque podría ser una dependencia interna/ESM/Node.
      const detail = String(e?.stack || e?.message || e || '').trim();
      const err = new Error(`baileys_import_failed package=${packageName} entry=${entry}: ${detail || 'import_failed'}`);
      err.code = 'BAILEYS_IMPORT_FAILED';
      err.cause = e;
      throw err;
    }
  }

  const err = new Error('baileys_module_not_installed');
  err.code = 'BAILEYS_MODULE_NOT_INSTALLED';
  throw err;
}



async function loadBaileysModule() {
  if (!baileysModulePromise) {
    baileysModulePromise = (async () => {
      try {
        return await importInstalledBaileysModule();
      } catch (e) {
        if (e?.code !== 'BAILEYS_MODULE_NOT_INSTALLED') throw e;

        // Caso típico de actualización manual: se copió app_asisto_ws.js/package.json
        // pero node_modules todavía no contiene Baileys. Lo reparamos sin modificar
        // package.json ni package-lock.json y luego reintentamos el import.
        await installBaileysDependencyIfMissing();
        return await importInstalledBaileysModule();
      }
    })().catch((e) => {
      baileysModulePromise = null;
      if (e?.code === 'BAILEYS_MODULE_NOT_INSTALLED') {
        const err = new Error(
          `Baileys no está instalado en node_modules. Debe estar declarado en package.json como ${BAILEYS_NPM_PACKAGE}@${BAILEYS_NPM_VERSION}.`
        );
        err.code = 'BAILEYS_MODULE_NOT_INSTALLED';
        throw err;
      }
      throw e;
    });
  }

  return baileysModulePromise;
}

const BAILEYS_SILENT_LOGGER = {
  level: 'silent',
  child() { return this; },
  trace() {}, debug() {}, info() {}, warn() {}, error() {}, fatal() {}
};


function baileysOnlyDigits(value) {
  return String(value || '').replace(/\D/g, '');
}

function baileysNormalizeDeviceJid(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  // 549...:12@s.whatsapp.net -> 549...@s.whatsapp.net
  return raw.replace(/:\d+@/i, '@');
}

function baileysToJid(value) {
  let raw = String(value || '').trim();
  if (!raw) return '';
  raw = raw.replace(/^whatsapp:/i, '');
  if (/@c\.us$/i.test(raw)) return raw.replace(/@c\.us$/i, '@s.whatsapp.net');
  if (/@s\.whatsapp\.net$/i.test(raw) || /@lid$/i.test(raw) || /@g\.us$/i.test(raw) || /@broadcast$/i.test(raw)) {
    return baileysNormalizeDeviceJid(raw);
  }
  const digits = baileysOnlyDigits(raw);
  return digits ? `${digits}@s.whatsapp.net` : raw;
}

function baileysToCompatJid(value) {
  const raw = baileysNormalizeDeviceJid(value);
  if (!raw) return '';
  if (/@s\.whatsapp\.net$/i.test(raw)) return raw.replace(/@s\.whatsapp\.net$/i, '@c.us');
  return raw;
}

function baileysMessageIdSerialized(key, compatRemote) {
  const id = String(key?.id || '').trim();
  const remote = String(compatRemote || baileysToCompatJid(key?.remoteJid || '')).trim();
  return `${key?.fromMe ? 'true' : 'false'}_${remote}_${id}`;
}

function baileysStatusToWwebAck(status) {
  const n = Number(status);
  if (!Number.isFinite(n)) return 0;
  // Baileys: ERROR=0,PENDING=1,SERVER_ACK=2,DELIVERY_ACK=3,READ=4,PLAYED=5
  // wwebjs: ACK_ERROR=-1,ACK_PENDING=0,ACK_SERVER=1,ACK_DEVICE=2,ACK_READ=3,ACK_PLAYED=4
  return n <= 0 ? -1 : Math.min(4, n - 1);
}

function baileysDisconnectReasonName(reasons, statusCode) {
  try {
    const code = Number(statusCode || 0);
    if (!code || !reasons || typeof reasons !== 'object') return '';
    for (const [name, value] of Object.entries(reasons)) {
      if (Number(value) === code) return String(name || '');
    }
  } catch {}
  return '';
}

function baileysDisconnectErrorDetail(err) {
  try {
    if (!err) return '';
    const parts = [];
    const message = String(err?.message || '').trim();
   const data = err?.data;
    const outputPayload = err?.output?.payload;
    const outputMessage = String(err?.output?.payload?.message || err?.output?.message || '').trim();
    const causeMessage = String(err?.cause?.message || '').trim();

    if (message) parts.push(`message=${message}`);
    if (outputMessage && outputMessage !== message) parts.push(`output=${outputMessage}`);
    if (causeMessage && causeMessage !== message) parts.push(`cause=${causeMessage}`);

    if (data !== undefined) {
      try { parts.push(`data=${JSON.stringify(data)}`); } catch { parts.push(`data=${String(data)}`); }
    }
    if (outputPayload !== undefined) {
      try { parts.push(`payload=${JSON.stringify(outputPayload)}`); } catch { parts.push(`payload=${String(outputPayload)}`); }
    }

    if (!parts.length) {
      try {
        const raw = JSON.stringify(err, Object.getOwnPropertyNames(err));
        if (raw && raw !== '{}') parts.push(`raw=${raw}`);
      } catch {}
    }

    return parts.join(' | ').slice(0, 3000);
  } catch {
    try { return String(err || '').slice(0, 3000); } catch { return ''; }
  }
}


class BaileysCompatClient extends EventEmitter {
  constructor(options = {}) {
    super();
    this.__transport = 'baileys';
    this.clientId = String(options.clientId || 'asisto');
    this.authDir = String(options.authDir || '');
    this.info = { me: { user: '' } };
    this.pupPage = null;
    this.pupBrowser = null;
    this._socket = null;
    this._baileys = null;
    this._saveCreds = null;
    this._state = 'DISCONNECTED';
    this._manualClose = false;
    this._authenticatedEmitted = false;
    this._messageById = new Map();
    this._messagesByChat = new Map();
    this._contacts = new Map();
    this._lidToPn = new Map();
    this._pnToLid = new Map();

    // Cache sólo para compatibilidad reciente (confirmaciones, ACK y fetchMessages).
    // Antes estos Maps podían crecer durante toda la vida del proceso.
    this._maxMessageCacheKeys = 2000; // ~1000 mensajes si cada uno usa 2 claves
    this._maxMessagesPerChat = 30;    // el código actual consulta como máximo 15
    this._maxChatsCached = 300;
  }

  _cacheMessageKey(key, value) {
    const cacheKey = String(key || '').trim();
    if (!cacheKey) return;

    // Map conserva orden de inserción: delete+set refresca la entrada.
    if (this._messageById.has(cacheKey)) this._messageById.delete(cacheKey);
    this._messageById.set(cacheKey, value);

    while (this._messageById.size > this._maxMessageCacheKeys) {
      const oldest = this._messageById.keys().next().value;
      if (oldest === undefined) break;
      this._messageById.delete(oldest);
    }
  }

  _cacheChatMessages(chatKey, list) {
    const key = String(chatKey || '').trim();
    if (!key) return;

    if (this._messagesByChat.has(key)) this._messagesByChat.delete(key);
    this._messagesByChat.set(key, list);

    while (this._messagesByChat.size > this._maxChatsCached) {
      const oldest = this._messagesByChat.keys().next().value;
      if (oldest === undefined) break;
      this._messagesByChat.delete(oldest);
    }
  }

  async initialize() {
    if (this._socket) return;
    this._manualClose = false;
    this._state = 'OPENING';

    const b = await loadBaileysModule();
    this._baileys = b;
    const makeWASocket = b.default || b.makeWASocket;
    if (typeof makeWASocket !== 'function') throw new Error('baileys_makeWASocket_missing');
    if (typeof b.useMultiFileAuthState !== 'function') throw new Error('baileys_useMultiFileAuthState_missing');

    if (!this.authDir) throw new Error('baileys_auth_dir_missing');
    await fs.promises.mkdir(this.authDir, { recursive: true });

    const { state, saveCreds } = await b.useMultiFileAuthState(this.authDir);
    this._saveCreds = saveCreds;

    const authKeys = (typeof b.makeCacheableSignalKeyStore === 'function')
      ? b.makeCacheableSignalKeyStore(state.keys, BAILEYS_SILENT_LOGGER)
      : state.keys;

    const socket = makeWASocket({
      auth: { creds: state.creds, keys: authKeys },
      logger: BAILEYS_SILENT_LOGGER,
      printQRInTerminal: false,
      markOnlineOnConnect: false,
      syncFullHistory: false,
      emitOwnEvents: true,
      getMessage: async (key) => {
        const cached = this._findCachedRawMessage(key?.id);
        return cached?.message || undefined;
      }
    });

    this._socket = socket;
    this._wireSocket(socket);
  }

  _wireSocket(socket) {
    socket.ev.on('creds.update', async () => {
      try { if (this._saveCreds) await this._saveCreds(); } catch (e) {
        try { console.log('[BAILEYS] creds.update error:', e?.message || e); } catch {}
      }
    });

    socket.ev.on('connection.update', async (update = {}) => {
      try {
        if (update.qr) {
          this._state = 'QR';
          this.emit('qr', update.qr);
        }

        if (update.connection === 'connecting') {
          this._state = 'OPENING';
        }

        if (update.connection === 'open') {
          this._state = 'CONNECTED';
          const ownJid = baileysNormalizeDeviceJid(socket?.user?.phoneNumber || socket?.user?.id || '');
          const ownDigits = baileysOnlyDigits(String(ownJid).split('@')[0]);
          if (ownDigits) this.info.me.user = ownDigits;
          if (!this._authenticatedEmitted) {
            this._authenticatedEmitted = true;
            this.emit('authenticated');
          }
          this.emit('ready');
        }

        if (update.connection === 'close' && !this._manualClose) {
          this._state = 'DISCONNECTED';
          const err = update?.lastDisconnect?.error;
          const statusCode = Number(
            err?.output?.statusCode ??
            err?.data?.statusCode ??
            err?.statusCode ??
            err?.status ??
            0
          );
          const reasons = this._baileys?.DisconnectReason || {};
          const loggedOut = statusCode && statusCode === Number(reasons.loggedOut);
          const restartRequired = statusCode && statusCode === Number(reasons.restartRequired);
          const reasonName = baileysDisconnectReasonName(reasons, statusCode);
          const errorDetail = baileysDisconnectErrorDetail(err);
          const closeLog = `[BAILEYS] connection.close statusCode=${statusCode || 0}` +
            ` reason=${reasonName || 'unknown'}` +
            ` loggedOut=${!!loggedOut}` +
            ` restartRequired=${!!restartRequired}` +
            (errorDetail ? ` error=${errorDetail}` : '');
          try { console.log(closeLog); } catch {}
          try { if (typeof EscribirLog === 'function') EscribirLog(closeLog, 'error'); } catch {}

          // Baileys fuerza este cierre inmediatamente después de vincular el QR.
          // Es un paso normal: recreamos solamente el socket interno, sin avisar al
          // supervisor de Asisto ni generar un falso "WhatsApp desconectado".
          if (restartRequired) {
            const restartLog = `[BAILEYS] restartRequired statusCode=${statusCode || 0}; recreando socket interno sin reiniciar la sesión Asisto`;
            try { console.log(restartLog); } catch {}
            try { if (typeof EscribirLog === 'function') EscribirLog(restartLog, 'event'); } catch {}
            this._socket = null;
            setTimeout(() => {
              this.initialize().catch((e) => {
                this.emit('disconnected', `baileys_restart_required_failed:${String(e?.message || e)}`);
              });
            }, 300);
            return;
          }

          this._socket = null;
          if (loggedOut) {
            this.emit('auth_failure', `baileys_logged_out:${statusCode}`);
          } else {
            this.emit('disconnected', `baileys:${statusCode || 'connection_closed'}`);
          }
        }
      } catch (e) {
        try { console.log('[BAILEYS] connection.update error:', e?.message || e); } catch {}
      }
    });

    socket.ev.on('messages.upsert', async (event = {}) => {
      try {
        const messages = Array.isArray(event.messages) ? event.messages : [];
        for (const raw of messages) {
          if (!raw?.key?.id || !raw?.message) continue;
          const wrapped = this._cacheAndWrap(raw);
          // append suele ser historial/sync; no debe disparar el bot.
          if (event.type !== 'notify') continue;
          this.emit('message_create', wrapped);
          if (wrapped.fromMe !== true) this.emit('message', wrapped);
        }
      } catch (e) {
        try { console.log('[BAILEYS] messages.upsert error:', e?.message || e); } catch {}
      }
    });

    socket.ev.on('messages.update', async (updates = []) => {
      try {
        for (const row of (Array.isArray(updates) ? updates : [])) {
          const id = String(row?.key?.id || '').trim();
          if (!id || row?.update?.status === undefined || row?.update?.status === null) continue;
          let wrapped = this._findCachedWrappedMessage(id);
          if (!wrapped) {
            const raw = this._findCachedRawMessage(id);
            if (raw) wrapped = this._wrapMessage(raw);
          }
          if (!wrapped) {
            wrapped = this._wrapMessage({
              key: row.key || { id },
              messageTimestamp: Math.floor(Date.now() / 1000),
              message: { conversation: '' }
            });
          }
          this.emit('message_ack', wrapped, baileysStatusToWwebAck(row.update.status));
        }
      } catch (e) {
        try { console.log('[BAILEYS] messages.update error:', e?.message || e); } catch {}
      }
    });

    socket.ev.on('messaging-history.set', async (history = {}) => {
      try {
        for (const c of (history.contacts || [])) this._cacheContact(c);
        for (const m of (history.messages || [])) {
          if (m?.key?.id && m?.message) this._cacheAndWrap(m);
        }
        for (const map of (history.lidPnMappings || [])) this._rememberLidMapping(map);
      } catch (e) {
        try { console.log('[BAILEYS] history cache error:', e?.message || e); } catch {}
      }
    });

    socket.ev.on('contacts.upsert', (contacts = []) => {
      for (const c of (Array.isArray(contacts) ? contacts : [])) this._cacheContact(c);
    });
    socket.ev.on('contacts.update', (contacts = []) => {
      for (const c of (Array.isArray(contacts) ? contacts : [])) this._cacheContact(c);
    });
    socket.ev.on('lid-mapping.update', (mapping) => {
      try {
        if (Array.isArray(mapping)) mapping.forEach((m) => this._rememberLidMapping(m));
        else this._rememberLidMapping(mapping);
      } catch {}
    });
  }

  _rememberLidMapping(mapping) {
   if (!mapping) return;
    let lid = mapping.lid || mapping.lidJid || mapping.id || mapping.key || '';
    let pn = mapping.pn || mapping.phoneNumber || mapping.phone || mapping.value || '';
    if (!lid && mapping.mapping && typeof mapping.mapping === 'object') {
      for (const [k, v] of Object.entries(mapping.mapping)) this._rememberLidMapping({ lid: k, pn: v });
      return;
    }
    lid = baileysToJid(lid);
    pn = baileysToJid(pn);
    if (lid && /@lid$/i.test(lid) && pn && /@s\.whatsapp\.net$/i.test(pn)) {
      this._lidToPn.set(lid, pn);
      this._pnToLid.set(pn, lid);
    }
  }

  _cacheContact(contact) {
    if (!contact || typeof contact !== 'object') return;
    const ids = [contact.id, contact.lid, contact.phoneNumber]
      .map(baileysToJid)
      .filter(Boolean);
    for (const id of ids) this._contacts.set(id, { ...(this._contacts.get(id) || {}), ...contact });
    if (contact.lid && contact.phoneNumber) this._rememberLidMapping({ lid: contact.lid, pn: contact.phoneNumber });
  }

  _messageRemoteJid(raw) {
    const key = raw?.key || {};
    let remote = baileysNormalizeDeviceJid(key.remoteJid || '');
    const alt = baileysNormalizeDeviceJid(key.remoteJidAlt || '');
    if (/@lid$/i.test(remote) && /@s\.whatsapp\.net$/i.test(alt)) {
      this._rememberLidMapping({ lid: remote, pn: alt });
      remote = alt;
    } else if (/@lid$/i.test(remote) && this._lidToPn.get(remote)) {
      remote = this._lidToPn.get(remote);
    }
    return remote;
  }

  _messageParticipantJid(raw) {
    const key = raw?.key || {};
    let participant = baileysNormalizeDeviceJid(key.participant || '');
    const alt = baileysNormalizeDeviceJid(key.participantAlt || '');
    if (/@lid$/i.test(participant) && /@s\.whatsapp\.net$/i.test(alt)) {
      this._rememberLidMapping({ lid: participant, pn: alt });
      participant = alt;
    } else if (/@lid$/i.test(participant) && this._lidToPn.get(participant)) {
      participant = this._lidToPn.get(participant);
    }
    return participant;
  }

  _cacheAndWrap(raw) {
    const wrapped = this._wrapMessage(raw);
    const id = String(raw?.key?.id || '').trim();
    if (id) {
      const cacheValue = { raw, wrapped };
      this._cacheMessageKey(id, cacheValue);
      if (wrapped?.id?._serialized) this._cacheMessageKey(wrapped.id._serialized, cacheValue);
    }

    const chatKey = baileysToJid(this._messageRemoteJid(raw));
    if (chatKey) {
      const list = this._messagesByChat.get(chatKey) || [];
      const filtered = list.filter((x) => String(x?.key?.id || '') !== id);
      filtered.push(raw);
      filtered.sort((a, b) => Number(a?.messageTimestamp || 0) - Number(b?.messageTimestamp || 0));
      while (filtered.length > this._maxMessagesPerChat) filtered.shift();
      this._cacheChatMessages(chatKey, filtered);
    }
    return wrapped;
  }

  _findCachedRawMessage(id) {
    return this._messageById.get(String(id || ''))?.raw || null;
  }

  _findCachedWrappedMessage(id) {
    return this._messageById.get(String(id || ''))?.wrapped || null;
  }

  _messageContent(raw) {
    let content = raw?.message || {};
    try {
      if (typeof this._baileys?.normalizeMessageContent === 'function') {
        content = this._baileys.normalizeMessageContent(content) || content;
      }
    } catch {}
    return content || {};
  }

  _messageTypeAndData(raw) {
    const content = this._messageContent(raw);
    let key = '';
    try {
      if (typeof this._baileys?.getContentType === 'function') key = this._baileys.getContentType(content) || '';
    } catch {}
    if (!key) key = Object.keys(content || {}).find((k) => content[k] != null) || '';
    const data = key ? (content[key] || {}) : {};

    let type = 'chat';
    if (key === 'imageMessage') type = 'image';
    else if (key === 'videoMessage') type = 'video';
    else if (key === 'documentMessage' || key === 'documentWithCaptionMessage') type = 'document';
    else if (key === 'audioMessage') type = data?.ptt ? 'ptt' : 'audio';
    else if (key === 'stickerMessage') type = 'sticker';
    else if (key === 'contactMessage' || key === 'contactsArrayMessage') type = 'vcard';
    else if (key === 'locationMessage' || key === 'liveLocationMessage') type = 'location';

    let body = '';
    if (key === 'conversation') body = String(content.conversation || '');
    else if (key === 'extendedTextMessage') body = String(data?.text || '');
    else if (key === 'buttonsResponseMessage') body = String(data?.selectedDisplayText || data?.selectedButtonId || '');
    else if (key === 'listResponseMessage') body = String(data?.title || data?.singleSelectReply?.selectedRowId || '');
    else if (key === 'templateButtonReplyMessage') body = String(data?.selectedDisplayText || data?.selectedId || '');
    else if (key === 'interactiveResponseMessage') body = String(data?.body?.text || data?.nativeFlowResponseMessage?.paramsJson || '');
    else if (data && typeof data === 'object') body = String(data.text || data.caption || '');

    const caption = String(data?.caption || '');
    const mimetype = String(data?.mimetype || '');
    const filename = String(data?.fileName || data?.filename || '');
    const hasMedia = ['imageMessage', 'videoMessage', 'documentMessage', 'documentWithCaptionMessage', 'audioMessage', 'stickerMessage'].includes(key);
    return { key, data, type, body, caption, mimetype, filename, hasMedia };
  }

  _wrapMessage(raw) {
    const key = raw?.key || {};
    const remoteBaileys = this._messageRemoteJid(raw);
    const remote = baileysToCompatJid(remoteBaileys);
    const participant = baileysToCompatJid(this._messageParticipantJid(raw));
    const own = this.info?.me?.user ? `${this.info.me.user}@c.us` : '';
    const md = this._messageTypeAndData(raw);
    const fromMe = key.fromMe === true;
    const isGroup = /@g\.us$/i.test(remoteBaileys);
    const from = fromMe ? own : remote;
    const to = fromMe ? remote : own;
    const serialized = baileysMessageIdSerialized(key, remote);
    const timestamp = Number(raw?.messageTimestamp || raw?.messageTimestamp?.low || Math.floor(Date.now() / 1000));

    const wrapper = {
      id: {
        id: String(key.id || ''),
        remote,
        fromMe,
        _serialized: serialized
      },
      from,
      to,
      author: isGroup ? participant : undefined,
      fromMe,
      body: md.body,
      caption: md.caption,
      type: md.type,
      timestamp,
      hasMedia: md.hasMedia,
      _data: {
        id: { id: String(key.id || ''), remote, fromMe, _serialized: serialized },
        from,
        to,
        author: isGroup ? participant : undefined,
        body: md.body,
        caption: md.caption,
        type: md.type,
        mimetype: md.mimetype,
        filename: md.filename,
        fileName: md.filename,
        t: timestamp,
        mediaKey: md.data?.mediaKey || undefined,
        directPath: md.data?.directPath || undefined,
        isViewOnce: !!md.data?.viewOnce
      },
      __baileysRaw: raw,
      getContact: async () => this.getContactById(isGroup && participant ? participant : (fromMe ? to : from)),
      downloadMedia: async () => this._downloadMedia(raw, md)
    };
    return wrapper;
 }

  async _downloadMedia(raw, md = this._messageTypeAndData(raw)) {
    if (!md.hasMedia) return undefined;
    const b = this._baileys || await loadBaileysModule();
    if (typeof b.downloadMediaMessage !== 'function') throw new Error('baileys_downloadMediaMessage_missing');

    const ctx = {
      logger: BAILEYS_SILENT_LOGGER,
      reuploadRequest: async (msg) => {
        if (!this._socket?.updateMediaMessage) throw new Error('baileys_updateMediaMessage_missing');
        return this._socket.updateMediaMessage(msg);
      }
    };

    let buffer;
    try {
      buffer = await b.downloadMediaMessage(raw, 'buffer', {}, ctx);
    } catch (firstError) {
      if (this._socket?.updateMediaMessage) {
        try { await this._socket.updateMediaMessage(raw); } catch {}
      }
      buffer = await b.downloadMediaMessage(raw, 'buffer', {}, ctx);
    }
    if (!buffer) return undefined;
    const buf = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer);
    return {
      data: buf.toString('base64'),
      mimetype: md.mimetype || 'application/octet-stream',
      filename: md.filename || ''
    };
  }

  async getState() {
    return this._state;
  }

  async sendMessage(to, content, options = {}) {
    if (!this._socket || this._state !== 'CONNECTED') throw new Error(`baileys_not_connected:${this._state}`);
    const jid = baileysToJid(to);
    if (!jid) throw new Error('baileys_invalid_recipient');

    let payload;
    if (content instanceof MessageMedia || (content && typeof content === 'object' && content.data && content.mimetype)) {
      const mimetype = String(content.mimetype || 'application/octet-stream');
      const filename = String(content.filename || options.filename || 'archivo');
      const caption = String(options.caption || '');
      const data = Buffer.from(String(content.data || ''), 'base64');

      if (/^image\//i.test(mimetype) && !/^image\/webp/i.test(mimetype)) {
        payload = { image: data, caption, mimetype };
      } else if (/^video\//i.test(mimetype)) {
        payload = { video: data, caption, mimetype };
      } else if (/^audio\//i.test(mimetype)) {
        payload = { audio: data, mimetype, ptt: options.sendAudioAsVoice === true };
      } else if (/^image\/webp/i.test(mimetype) && options.sendMediaAsSticker === true) {
        payload = { sticker: data };
      } else {
        payload = { document: data, mimetype, fileName: filename, caption };
      }
    } else {
      payload = { text: String(content ?? '') };
    }

    const sent = await this._socket.sendMessage(jid, payload);
    return this._cacheAndWrap(sent);
  }

  async isRegisteredUser(id) {
    const jid = baileysToJid(id);
    if (!jid || /@g\.us$/i.test(jid) || /@broadcast$/i.test(jid)) return false;
    if (/@lid$/i.test(jid)) return true;
    if (!this._socket?.onWhatsApp) return true;
    try {
      const result = await this._socket.onWhatsApp(jid);
      if (!Array.isArray(result) || !result.length) return false;
      return result.some((row) => row?.exists !== false);
    } catch {
      return false;
    }
  }

  async _resolvePnForLid(jid) {
    const raw = baileysToJid(jid);
    if (!/@lid$/i.test(raw)) return raw;
    if (this._lidToPn.has(raw)) return this._lidToPn.get(raw);
    try {
      const pn = await this._socket?.signalRepository?.lidMapping?.getPNForLID?.(raw);
      const normalized = baileysToJid(pn);
      if (normalized && /@s\.whatsapp\.net$/i.test(normalized)) {
        this._rememberLidMapping({ lid: raw, pn: normalized });
        return normalized;
      }
    } catch {}
    return raw;
  }

  async getContactById(id) {
    const requested = baileysToJid(id);
    const resolved = await this._resolvePnForLid(requested);
    let contact = this._contacts.get(requested) || this._contacts.get(resolved) || {};

    for (const c of this._contacts.values()) {
      if (!contact || !Object.keys(contact).length) {
        const ids = [c?.id, c?.lid, c?.phoneNumber].map(baileysToJid);
        if (ids.includes(requested) || ids.includes(resolved)) contact = c;
      }
    }

    let businessProfile = null;
    try {
      if (this._socket?.getBusinessProfile && /@s\.whatsapp\.net$/i.test(resolved)) {
        businessProfile = await this._socket.getBusinessProfile(resolved);
      }
    } catch {}

    const compat = baileysToCompatJid(resolved);
    const number = /@s\.whatsapp\.net$/i.test(resolved)
      ? baileysOnlyDigits(resolved.split('@')[0])
      : '';
    const user = number || baileysOnlyDigits(String(requested).split('@')[0]);
    const name = contact?.name || contact?.verifiedName || contact?.notify || '';
    const pushname = contact?.notify || contact?.name || contact?.verifiedName || '';

    return {
      number,
      id: { user, _serialized: compat || baileysToCompatJid(requested) },
      isBusiness: !!businessProfile,
      businessProfile: businessProfile ? {
        email: businessProfile.email || null,
        address: businessProfile.address || null,
        description: businessProfile.description || null,
        website: businessProfile.website || null
      } : null,
      name: name || null,
      pushname: pushname || null,
      shortName: name || pushname || null,
      _data: {
        id: { user, _serialized: compat || baileysToCompatJid(requested) },
        number,
        wid: { user, _serialized: compat || baileysToCompatJid(requested) },
        userid: user,
        phone: number
      }
    };
  }

  async getChatById(id) {
    const jid = baileysToJid(id);
    const self = this;
    return {
      _data: { id: jid },
      async fetchMessages(options = {}) {
        const limit = Math.max(1, Number(options.limit || 50) || 50);
        let list = self._messagesByChat.get(jid) || [];
        if (!list.length && /@s\.whatsapp\.net$/i.test(jid)) {
          const lid = self._pnToLid.get(jid);
          if (lid) list = self._messagesByChat.get(lid) || [];
        }
        return list.slice(-limit).reverse().map((raw) => self._findCachedWrappedMessage(raw?.key?.id) || self._wrapMessage(raw));
      }
    };
  }

  async getMessageById(id) {
    return this._findCachedWrappedMessage(id) || null;
  }

  async logout() {
    this._manualClose = true;
    this._state = 'DISCONNECTED';
    try { await this._socket?.logout?.(); } finally { this._socket = null; }
  }

  async destroy() {
    this._manualClose = true;
    this._state = 'DISCONNECTED';
    const socket = this._socket;
    this._socket = null;
    try {
      if (socket?.end) socket.end(new Error('asisto_baileys_destroy'));
      else if (socket?.ws?.close) socket.ws.close();
    } catch {}
  }
}




// Momento en el que ESTA instancia tomó el lock (para ignorar acciones viejas en wa_wweb_actions)
let lockAcquiredAt = null;
// --- LocalAuth backup/restore removido ---
let authFailureHandling = false;
const AR_TZ = 'America/Argentina/Cordoba';

// ============================================================================
// Multi-sesión real supervisada por un único proceso padre usando procesos Node hijos.
// - El proceso principal actúa como supervisor cuando configuracion.json contiene
//   multi_sessions / multiSessions.
// - Cada sesión ejecuta ESTE MISMO archivo en un proceso aislado.
// - El aislamiento por proceso es necesario porque ODBC es un addon N-API nativo;
//   cargarlo en varios Worker Threads del mismo PID puede provocar crashes fatales de V8.
// - Baileys y whatsapp-web.js mantienen exactamente la misma lógica por sesión.
// ============================================================================
const ASISTO_MULTI_WORKER = String(process.env.ASISTO_MULTI_WORKER || '').trim() === '1';
const ASISTO_MULTI_SESSION_KEY = String(process.env.ASISTO_MULTI_SESSION_KEY || '').trim();
const ASISTO_MULTI_PRIMARY_WORKER = String(process.env.ASISTO_MULTI_PRIMARY_WORKER || '').trim() === '1';
let multiSessionSupervisorState = null;


function sendMultiSupervisorMessage(message) {
  if (!ASISTO_MULTI_WORKER || typeof process.send !== 'function') return false;
  try {
    process.send(message);
    return true;
  } catch {
    return false;
  }
}

function onMultiSupervisorMessage(handler) {
  if (!ASISTO_MULTI_WORKER || typeof process.on !== 'function' || typeof process.send !== 'function') return false;
  process.on('message', handler);
  return true;
}

function sanitizeMultiSessionFilePart(value) {
  return String(value || 'session')
    .trim()
    .replace(/[^a-zA-Z0-9_.-]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'session';
}

// Evita dos instancias de la MISMA sesión. En modo multi cada worker tiene
// su propio lock para poder convivir en la misma carpeta y dentro del mismo PID.
const SINGLE_INSTANCE_LOCK_PATH = path.join(
  __dirname,
  'logs',
  ASISTO_MULTI_WORKER && ASISTO_MULTI_SESSION_KEY
    ? `app_asisto_ws.${sanitizeMultiSessionFilePart(ASISTO_MULTI_SESSION_KEY)}.pid`
    : 'app_asisto_ws.pid'
);

let singleInstanceLockOwned = false;


// ================================================================
// Cliente de Control API integrado en este mismo archivo.
// Antes estaba en ./wweb_control_client.js. Se integra aquí para que
// el auto-update de las PC no dependa de distribuir un segundo archivo.
// ================================================================
const TYPE_KEY = '__asistoType';

function encodeSpecial(value) {
  if (value instanceof Date) {
    return { [TYPE_KEY]: 'date', value: value.toISOString() };
  }
  if (value instanceof RegExp) {
    return { [TYPE_KEY]: 'regexp', source: value.source, flags: value.flags };
  }
  if (Buffer.isBuffer(value)) {
    return { [TYPE_KEY]: 'buffer', value: value.toString('base64') };
  }
  if (Array.isArray(value)) return value.map(encodeSpecial);
  if (value && typeof value === 'object') {
    if (value._bsontype === 'ObjectId' && typeof value.toHexString === 'function') {
      return { [TYPE_KEY]: 'objectId', value: value.toHexString() };
    }
    const out = {};
    for (const [key, item] of Object.entries(value)) out[key] = encodeSpecial(item);
    return out;
  }
  return value;
}

function decodeSpecial(value) {
  if (Array.isArray(value)) return value.map(decodeSpecial);
  if (value && typeof value === 'object') {
    if (value[TYPE_KEY] === 'date') return new Date(value.value);
    if (value[TYPE_KEY] === 'regexp') return new RegExp(value.source || '', value.flags || '');
    if (value[TYPE_KEY] === 'buffer') return Buffer.from(String(value.value || ''), 'base64');
    // En la PC no hace falta construir ObjectId. El servidor volverá a convertir
    // los strings hexadecimales cuando se usen como filtro _id.
    if (value[TYPE_KEY] === 'objectId') return String(value.value || '');
    const out = {};
    for (const [key, item] of Object.entries(value)) out[key] = decodeSpecial(item);
    return out;
  }
  return value;
}

function normalizeBaseUrl(value) {
  return String(value || '').trim().replace(/\/+$/, '');
}

function createControlApiClient(options = {}) {
  let baseUrl = normalizeBaseUrl(options.baseUrl);
  let token = String(options.token || '').trim();
  let tenantId = String(options.tenantId || '').trim().toUpperCase();
  let numero = String(options.numero || '').replace(/\D/g, '');
  let readyUntil = 0;
  let readyPromise = null;

  const timeoutMs = Math.max(3000, Number(options.timeoutMs || process.env.WWEB_CONTROL_API_TIMEOUT_MS || 15000));
  const http = axios.create({
    timeout: timeoutMs,
    maxContentLength: 20 * 1024 * 1024,
    maxBodyLength: 20 * 1024 * 1024,
    validateStatus: () => true,
  });

  function configure(next = {}) {
    if (next.baseUrl !== undefined) baseUrl = normalizeBaseUrl(next.baseUrl);
    if (next.token !== undefined) token = String(next.token || '').trim();
    if (next.tenantId !== undefined) tenantId = String(next.tenantId || '').trim().toUpperCase();
    if (next.numero !== undefined) numero = String(next.numero || '').replace(/\D/g, '');
    readyUntil = 0;
  }

  function isConfigured() {
    return !!(baseUrl && token && tenantId);
  }

  function headers() {
    return {
      'content-type': 'application/json',
      'x-api-key': token,
      'x-asisto-tenant': tenantId,
      'x-asisto-numero': numero,
    };
  }

  async function request(path, payload = {}) {
    if (!isConfigured()) throw new Error('control_api_not_configured');
    const response = await http.post(`${baseUrl}${path}`, encodeSpecial({
      tenantId,
      numero,
      ...payload,
    }), { headers: headers() });

    const body = decodeSpecial(response.data);
    if (response.status < 200 || response.status >= 300 || !body || body.ok !== true) {
      const detail = body?.detail || body?.error || `http_${response.status}`;
      const error = new Error(`control_api_${detail}`);
      error.status = response.status;
      error.responseBody = body;
      throw error;
    }
    return body;
  }

  async function ensureReady(force = false) {
    if (!isConfigured()) return false;
    if (!force && Date.now() < readyUntil) return true;
    if (readyPromise) return readyPromise;

    readyPromise = request('/ping', { host: os.hostname(), pid: process.pid })
      .then(() => {
        readyUntil = Date.now() + 30000;
        return true;
      })
      .catch(() => false)
      .finally(() => { readyPromise = null; });

    return readyPromise;
  }

  async function dbCall(collection, operation, args = {}) {
    const body = await request('/db', { collection, operation, args });
    return body.result;
  }

  function collection(name) {
    const collectionName = String(name || '').trim();
    return {
      findOne(query = {}, options = {}) {
        return dbCall(collectionName, 'findOne', { query, options });
      },
      insertOne(document = {}, options = {}) {
        return dbCall(collectionName, 'insertOne', { document, options });
      },
      updateOne(query = {}, update = {}, options = {}) {
        return dbCall(collectionName, 'updateOne', { query, update, options });
      },
      updateMany(query = {}, update = {}, options = {}) {
        return dbCall(collectionName, 'updateMany', { query, update, options });
      },
      deleteMany(query = {}, options = {}) {
        return dbCall(collectionName, 'deleteMany', { query, options });
      },
      findOneAndUpdate(query = {}, update = {}, options = {}) {
        return dbCall(collectionName, 'findOneAndUpdate', { query, update, options });
      },
      find(query = {}, options = {}) {
        let sort = null;
        let limit = 0;
        const cursor = {
          sort(value) { sort = value || null; return cursor; },
          limit(value) { limit = Math.max(0, Number(value) || 0); return cursor; },
          async toArray() {
            return dbCall(collectionName, 'find', { query, options, sort, limit });
          }
        };
        return cursor;
      }
    };
  }

  function leanable(promise) {
    return {
      lean: () => promise,
      then: (resolve, reject) => promise.then(resolve, reject),
      catch: (reject) => promise.catch(reject),
      finally: (handler) => promise.finally(handler),
    };
  }

  function model(collectionName) {
    const coll = collection(collectionName);
    return {
      async create(document) {
        const result = await coll.insertOne(document || {});
        return result?.document || document;
      },
      findById(id) {
        return leanable(coll.findOne({ _id: id }));
      },
     findOne(query, options) {
        return leanable(coll.findOne(query || {}, options || {}));
      },
      updateOne(query, update, options) {
        return coll.updateOne(query || {}, update || {}, options || {});
      },
      findOneAndUpdate(query, update, options) {
        return leanable(coll.findOneAndUpdate(query || {}, update || {}, options || {}));
      }
    };
  }

  return {
    configure,
    isConfigured,
    ensureReady,
    request,
    dbCall,
    collection,
    model,
    getConfig: () => ({ baseUrl, token, tenantId, numero, timeoutMs }),
  };
}

// ================= FIN CONTROL API INTEGRADO =====================



function isPidAlive(pid) {
  const n = Number(pid);
  if (!Number.isInteger(n) || n <= 0) return false;
  try {
    process.kill(n, 0);
    return true;
  } catch {
    return false;
  }
}

function acquireSingleInstanceLock() {
  try { fs.mkdirSync(path.dirname(SINGLE_INSTANCE_LOCK_PATH), { recursive: true }); } catch {}

  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const fd = fs.openSync(SINGLE_INSTANCE_LOCK_PATH, 'wx');
      fs.writeFileSync(fd, String(process.pid), 'utf8');
      fs.closeSync(fd);
      singleInstanceLockOwned = true;
      return true;
    } catch (e) {
      if (e?.code !== 'EEXIST') throw e;

      let previousPid = 0;
      try { previousPid = Number(fs.readFileSync(SINGLE_INSTANCE_LOCK_PATH, 'utf8').trim()); } catch {}
      if (previousPid && previousPid !== process.pid && isPidAlive(previousPid)) {
        console.error(`[INSTANCE] Ya existe app_asisto_ws.js activo pid=${previousPid}. Este proceso pid=${process.pid} finaliza.`);
        return false;
      }

      try { fs.unlinkSync(SINGLE_INSTANCE_LOCK_PATH); } catch {}
    }
  }
  return false;
}

function releaseSingleInstanceLock() {
  if (!singleInstanceLockOwned) return;
  try {
    const current = Number(fs.readFileSync(SINGLE_INSTANCE_LOCK_PATH, 'utf8').trim());
    if (!current || current === process.pid) fs.unlinkSync(SINGLE_INSTANCE_LOCK_PATH);
  } catch {}
  singleInstanceLockOwned = false;
}

if (!acquireSingleInstanceLock()) {
  process.exit(72);
}
process.once('exit', releaseSingleInstanceLock);

 

// Modo de reinicio solicitado desde el panel de sesiones.
// task_runner: sale con código 77 y lo levanta el .cmd runner.
// pm2: sale con código 0 y lo levanta PM2.
// whatsapp: mantiene el comportamiento viejo, reinicia solo WhatsApp/Chromium.
var panel_restart_mode = String(process.env.ASISTO_PANEL_RESTART_MODE || process.env.PANEL_RESTART_MODE || 'task_runner').trim().toLowerCase();

function normalizePanelRestartMode(value, fallback = 'task_runner') {
  const v = String(value || '').trim().toLowerCase();
  if (['pm2', 'pm2_restart', 'pm2_exit'].includes(v)) return 'pm2';
  if (['task', 'task_runner', 'runner', 'windows_task', 'tarea_programada', 'scheduled_task'].includes(v)) return 'task_runner';
  if (['whatsapp', 'wweb', 'restart_whatsapp'].includes(v)) return 'whatsapp';
  return fallback;
}

panel_restart_mode = normalizePanelRestartMode(panel_restart_mode, 'task_runner');

function getPanelRestartMode() {
  return normalizePanelRestartMode(
    process.env.ASISTO_PANEL_RESTART_MODE || process.env.PANEL_RESTART_MODE || panel_restart_mode || 'task_runner',
    'task_runner'
  );
}



// =========================
// Reinicio automático por error fatal
// =========================
// IMPORTANTE:
// - El script NO intenta relanzarse solo.
// - Si corre con Tarea Programada, la tarea debe ejecutar asisto_ws_runner.cmd.
// - Ante un error fatal, salimos con código distinto de 0 para que el runner
//   vuelva a iniciar el script.
const FATAL_PROCESS_EXIT_CODE = Number(
  process.env.ASISTO_FATAL_EXIT_CODE ||
  process.env.ASISTO_CRASH_EXIT_CODE ||
  88
);

let fatalProcessExitInProgress = false;

function fatalReasonToString(reason) {
  try {
    if (reason instanceof Error) return (reason.stack || reason.message || String(reason));
    if (typeof reason === 'string') return reason;
    return JSON.stringify(reason);
  } catch {
    return String(reason);
  }
}

function writeFatalProcessLog(label, reason) {
  const text = fatalReasonToString(reason);
  const line = '[FATAL] ' + String(label || 'fatal') + ' -> ' + text;

  try { console.error(line); } catch {}
  try {
    if (typeof EscribirLog === 'function') EscribirLog(line, 'error');
  } catch {}

  try {
    const logsDir = path.join(__dirname, 'logs');
    try { fs.mkdirSync(logsDir, { recursive: true }); } catch {}
    fs.appendFileSync(
      path.join(logsDir, 'asisto-fatal.log'),
      '[' + new Date().toISOString() + '] pid=' + process.pid + ' ' + line + '\n',
      'utf8'
    );
  } catch {}
}

function exitForFatalProcessError(label, reason) {
  if (fatalProcessExitInProgress) return;
  fatalProcessExitInProgress = true;

  const exitCode = Number.isFinite(FATAL_PROCESS_EXIT_CODE) && FATAL_PROCESS_EXIT_CODE !== 0
    ? FATAL_PROCESS_EXIT_CODE
    : 88;

  writeFatalProcessLog(label, reason);

  try { localWsPanelState = 'crashed'; } catch {}

  // Intento rápido de liberar estado/lock, sin bloquear el cierre.
  try {
    Promise.resolve()
      .then(async () => {
        try {
          if (typeof updateLockStateSafe === 'function') await updateLockStateSafe('crashed');
        } catch {}
        try {
          if (typeof forceReleaseLock === 'function') await forceReleaseLock('crashed');
        } catch {}
      })
      .catch(() => {});
  } catch {}

  try { process.exitCode = exitCode; } catch {}
  try {
    const t = setTimeout(() => {
      try { process.exit(exitCode); } catch {}
    }, 1500);
    if (t && typeof t.unref === 'function') t.unref();
  } catch {
    try { process.exit(exitCode); } catch {}
  }
}

// Los handlers fatales se registran una sola vez junto al cierre controlado,
// más abajo, cuando ya están definidas las funciones de liberación de recursos.

// =========================
// Multi-PC failover (Opción B)
// - Sesión WhatsApp guardada en Mongo (RemoteAuth)
// - Lock/lease en Mongo para evitar 2 PCs conectadas al mismo tiempo
// =========================
let tenantId = process.env.TENANT_ID || "";
let numero = process.env.NUMERO || "";              // solo dígitos, ej: 54911...
let mongo_uri = process.env.MONGO_URI || "";
let status_token = process.env.STATUS_TOKEN || "";  // opcional para proteger /status

// DB name: si el URI no incluye "/<db>", Mongo usa "test" por defecto.
// Para que quede en tu DB (ej: "CARICO"), seteá mongo_db en configuracion.json
// o usaremos tenantId como dbName por defecto.
let mongo_db = process.env.MONGO_DB || "";
// Control HTTPS centralizado. Cuando está configurado, esta PC NO abre
// conexiones directas a MongoDB; todas las operaciones pasan por Render.
let control_api_url = String(process.env.WWEB_CONTROL_API_URL || process.env.CONTROL_API_URL || 'https://www.asistobot.com.ar/api/ext/wweb/agent').trim();
let control_api_token = String(process.env.WWEB_CONTROL_API_TOKEN || process.env.CONTROL_API_TOKEN || '').trim();
let control_api_enabled = String(process.env.WWEB_CONTROL_API_ENABLED || process.env.CONTROL_API_ENABLED || '').trim().toLowerCase() === 'true';
let controlApiReadyLogged = false;
let controlApiLastErrorLogAt = 0;
const controlApi = createControlApiClient({
  baseUrl: control_api_url,
  token: control_api_token,
  tenantId,
  numero,
});

function isControlApiConfigured() {
  return control_api_enabled === true && controlApi.isConfigured();
}

function configureControlApiFromValues(values = {}) {
  const url = values.control_api_url ?? values.controlApiUrl ?? values.wweb_control_api_url ?? values.wwebControlApiUrl;
  const token = values.control_api_token ?? values.controlApiToken ?? values.wweb_control_api_token ?? values.wwebControlApiToken ?? values.status_token ?? values.statusToken;
  const enabled = values.control_api_enabled ?? values.controlApiEnabled ?? values.wweb_control_api_enabled ?? values.wwebControlApiEnabled;

  if (url !== undefined && url !== null && String(url).trim()) control_api_url = String(url).trim().replace(/\/+$/, '');
  if (token !== undefined && token !== null && String(token).trim()) control_api_token = String(token).trim();
  if (enabled !== undefined && enabled !== null && enabled !== '') {
    const normalized = String(enabled).trim().toLowerCase();
    control_api_enabled = ['1', 'true', 'yes', 'si', 'sí', 'on'].includes(normalized);
  } else if (control_api_url && control_api_token) {
    control_api_enabled = true;
  }

  controlApi.configure({
    baseUrl: control_api_url,
    token: control_api_token,
    tenantId,
    numero,
  });
}


function readControlApiTokenFromTenantDoc(doc) {
  try {
    const nested = doc && doc.configuracion && typeof doc.configuracion === 'object'
      ? doc.configuracion
      : {};
    return String(
      nested.control_api_token ||
      nested.controlApiToken ||
      nested.status_token ||
      nested.statusToken ||
      doc?.control_api_token ||
      doc?.controlApiToken ||
      doc?.status_token ||
      doc?.statusToken ||
      ''
    ).trim();
  } catch {
    return '';
  }
}

// Migración sin intervención manual:
// - la PC usa una última conexión Mongo que ya tenía configurada;
// - si el tenant no tiene token, crea uno aleatorio en tenant_config;
// - luego lo guarda automáticamente en configuracion.json y pasa a HTTPS.
//
// No se habilita una API pública sin autenticación. El token se genera dentro
// de MongoDB usando las credenciales antiguas que la PC ya poseía.
async function ensureControlApiBootstrapInTenantConfig(collection, doc) {
  if (!doc || !collection || isControlApiConfigured()) return doc;

 const existingToken = readControlApiTokenFromTenantDoc(doc);
  if (existingToken) return doc;

  const generatedToken = crypto.randomBytes(32).toString('hex');
  const nested = !!(doc.configuracion && typeof doc.configuracion === 'object');
  const selector = doc._id !== undefined && doc._id !== null
    ? { _id: doc._id }
    : { tenantId: tenantId };

  const tokenPath = nested ? 'configuracion.control_api_token' : 'control_api_token';
  const enabledPath = nested ? 'configuracion.control_api_enabled' : 'control_api_enabled';
  const urlPath = nested ? 'configuracion.control_api_url' : 'control_api_url';

  const tokenMissing = {
    $or: [
      { [tokenPath]: { $exists: false } },
      { [tokenPath]: null },
      { [tokenPath]: '' }
    ]
  };

  try {
    // findOneAndUpdate evita que dos PC del mismo tenant creen tokens distintos
    // al mismo tiempo. Solo la primera que encuentre el campo vacío lo escribe.
    const updatedResult = await collection.findOneAndUpdate(
      { $and: [selector, tokenMissing] },
      {
        $set: {
          [tokenPath]: generatedToken,
         [enabledPath]: true,
          [urlPath]: control_api_url
        }
      },
      { returnDocument: 'after' }
    );

    // MongoDB Driver 5 puede devolver { value }, Driver 6 devuelve el documento.
    let updatedDoc = updatedResult?.value || updatedResult || null;

    // Si otra PC ganó la carrera, releemos el documento para tomar el mismo token.
    if (!updatedDoc || !readControlApiTokenFromTenantDoc(updatedDoc)) {
      updatedDoc = await collection.findOne(selector);
    }

    const finalToken = readControlApiTokenFromTenantDoc(updatedDoc);
    if (!finalToken) {
      throw new Error('control_api_token_generation_failed');
    }

    try {
      console.log(`[CONTROL_API] token generado automáticamente para tenant=${tenantId}; no requiere modificar configuracion.json`);
    } catch {}
    try {
      EscribirLog(`[CONTROL_API] token generado automáticamente para tenant=${tenantId}`, 'event');
    } catch {}

    return updatedDoc;
  } catch (e) {
    try {
      console.log('[CONTROL_API] no se pudo generar token automático:', e?.message || e);
    } catch {}
    try {
      EscribirLog('[CONTROL_API] no se pudo generar token automático: ' + String(e?.message || e), 'error');
    } catch {}
    return doc;
  }
}


// =========================
// Config por tenant (MongoDB)
// configuracion.json: SOLO { tenantId, mongo_uri, mongo_db }
// El resto (puerto, numero, seg_desde, etc.) viene de la colección tenant_config.
// =========================
let tenantConfig = null; // config cargada desde Mongo

function decodeMultiWorkerBootstrapOverride() {
  if (!ASISTO_MULTI_WORKER) return {};
  try {
    const raw = String(process.env.ASISTO_WORKER_BOOTSTRAP_B64 || '').trim();
    if (!raw) return {};
    const json = Buffer.from(raw, 'base64').toString('utf8');
    const obj = JSON.parse(json);
    return obj && typeof obj === 'object' ? obj : {};
  } catch (e) {
    try { console.log('[MULTI] bootstrap worker inválido:', e?.message || e); } catch {}
    return {};
  }
}


function readBootstrapFromFile() {
  try {
    const candidates = [
      path.join(__dirname, "configuracion.json"),
      path.join(process.cwd(), "configuracion.json"),
    ];
    let p = null;
    for (const candidate of candidates) {
      if (fs.existsSync(candidate)) {
        p = candidate;
        break;
      }
    }
    if (!p) return decodeMultiWorkerBootstrapOverride();
    const raw = JSON.parse(fs.readFileSync(p, "utf8"));
    const nested = (raw && raw.configuracion && typeof raw.configuracion === "object") ? raw.configuracion : null;
    const obj = nested ? { ...nested } : ((raw && typeof raw === 'object') ? { ...raw } : {});

    // Si multi_sessions está en la raíz y configuracion está anidado, no perderlo.
    if (raw && typeof raw === 'object') {
      if (raw.multi_sessions !== undefined) obj.multi_sessions = raw.multi_sessions;
      if (raw.multiSessions !== undefined) obj.multiSessions = raw.multiSessions;
      if (raw.multi_base_port !== undefined) obj.multi_base_port = raw.multi_base_port;
      if (raw.multiBasePort !== undefined) obj.multiBasePort = raw.multiBasePort;
      if (raw.multi_refresh_ms !== undefined) obj.multi_refresh_ms = raw.multi_refresh_ms;
      if (raw.multiRefreshMs !== undefined) obj.multiRefreshMs = raw.multiRefreshMs;
    }

    if (!ASISTO_MULTI_WORKER) return obj;

    const workerOverride = decodeMultiWorkerBootstrapOverride();
    const merged = { ...obj, ...workerOverride };

    // Un configuracion.json que antes era mono-tenant puede conservar un token raíz.
    // No heredarlo accidentalmente cuando este worker pertenece a otro dominio.
    const baseTenant = String(obj.tenantId || obj.tenantid || '').trim().toUpperCase();
    const workerTenant = String(workerOverride.tenantId || workerOverride.tenantid || process.env.TENANT_ID || '').trim().toUpperCase();
    if (baseTenant && workerTenant && baseTenant !== workerTenant) {
      const tenantScopedKeys = [
        'control_api_token', 'controlApiToken', 'wweb_control_api_token', 'wwebControlApiToken',
        'status_token', 'statusToken'
      ];
      for (const key of tenantScopedKeys) {
        if (!Object.prototype.hasOwnProperty.call(workerOverride, key)) delete merged[key];
      }
    }

    if (process.env.TENANT_ID) merged.tenantId = String(process.env.TENANT_ID).trim();
    if (process.env.NUMERO) merged.numero = String(process.env.NUMERO).replace(/\D/g, '');
    if (process.env.ASISTO_WORKER_PORT) merged.puerto = Number(process.env.ASISTO_WORKER_PORT);
    return merged;
  } catch {
    return decodeMultiWorkerBootstrapOverride();
  }
}


async function acquireBootstrapFileLock(lockPath, timeoutMs = 10000) {
  const started = Date.now();
  while ((Date.now() - started) < timeoutMs) {
    try {
      const fd = fs.openSync(lockPath, 'wx');
      return fd;
    } catch (e) {
      if (e?.code !== 'EEXIST') throw e;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  throw new Error('bootstrap_file_lock_timeout');
}

async function persistControlApiBootstrap() {
  if (!control_api_url || !control_api_token) return false;

  const candidates = [
    path.join(__dirname, "configuracion.json"),
    path.join(process.cwd(), "configuracion.json"),
  ];
  const configPath = candidates.find((candidate) => fs.existsSync(candidate)) || candidates[0];
  const lockPath = configPath + '.multi.lock';
  let lockFd = null;

  try {
    try { fs.mkdirSync(path.dirname(configPath), { recursive: true }); } catch {}
    lockFd = await acquireBootstrapFileLock(lockPath, 10000);

    let raw = {};
    try { raw = JSON.parse(fs.readFileSync(configPath, 'utf8')); } catch {}

    if (ASISTO_MULTI_WORKER) {
      // En multi-sesión cada tenant conserva SU token. Nunca pisar el token raíz
      // con el de otro dominio.
      const root = raw && typeof raw === 'object' ? raw : {};
      let holder = root;
      let listKey = null;
      let list = null;

      if (Array.isArray(root.multi_sessions)) { listKey = 'multi_sessions'; list = root.multi_sessions; }
      else if (Array.isArray(root.multiSessions)) { listKey = 'multiSessions'; list = root.multiSessions; }
      else if (root.configuracion && typeof root.configuracion === 'object' && Array.isArray(root.configuracion.multi_sessions)) {
        holder = root.configuracion; listKey = 'multi_sessions'; list = holder.multi_sessions;
      } else if (root.configuracion && typeof root.configuracion === 'object' && Array.isArray(root.configuracion.multiSessions)) {
        holder = root.configuracion; listKey = 'multiSessions'; list = holder.multiSessions;
      }

      if (list && listKey) {
        const wantedTenant = String(tenantId || '').trim().toUpperCase();
        const wantedNumero = String(numero || '').replace(/\D/g, '');
        let found = false;
        holder[listKey] = list.map((item) => {
          if (!item || typeof item !== 'object') return item;
          const nested = item.configuracion && typeof item.configuracion === 'object' ? item.configuracion : null;
          const v = nested ? { ...item, ...nested } : item;
          const t = String(v.tenantId || v.tenantid || '').trim().toUpperCase();
          const n = String(v.numero || v.number || v.phone || '').replace(/\D/g, '');
          if (!found && t === wantedTenant && (!n || !wantedNumero || n === wantedNumero)) {
            found = true;
            if (nested) {
              return {
                ...item,
                configuracion: {
                  ...nested,
                  control_api_url,
                  control_api_token,
                  control_api_enabled: true,
                }
              };
            }
            return {
              ...item,
              control_api_url,
              control_api_token,
              control_api_enabled: true,
            };
          }
          return item;
        });

        if (found) {
         const tempPath = configPath + `.tmp.${process.pid}.${threadId || 0}`;
          fs.writeFileSync(tempPath, JSON.stringify(root, null, 2), 'utf8');
          fs.renameSync(tempPath, configPath);
          try { EscribirLog('[CONTROL_API] token multi-sesión guardado localmente tenant=' + tenantId + ' numero=' + numero, 'event'); } catch {}
          return true;
        }
      }

      // Si no encontramos la sesión, no escribir un token de tenant en la raíz.
      try { EscribirLog('[CONTROL_API] no se encontró entrada multi_sessions para persistir token tenant=' + tenantId + ' numero=' + numero, 'error'); } catch {}
      return false;
    }

    // Modo histórico de una sola sesión.
    const nested = raw && raw.configuracion && typeof raw.configuracion === 'object';
    const target = nested ? raw.configuracion : raw;
    target.tenantId = target.tenantId || tenantId;
    target.mongo_uri = target.mongo_uri || mongo_uri;
    target.mongo_db = target.mongo_db || mongo_db;
    target.control_api_url = control_api_url;
    target.control_api_token = control_api_token;
    target.control_api_enabled = true;
    if (nested) raw.configuracion = target;
    const tempPath = configPath + '.tmp';
    fs.writeFileSync(tempPath, JSON.stringify(raw, null, 2), 'utf8');
    fs.renameSync(tempPath, configPath);
    try { EscribirLog('[CONTROL_API] configuración guardada localmente url=' + control_api_url, 'event'); } catch {}
    return true;
  } catch (e) {
    try { EscribirLog('[CONTROL_API] no se pudo guardar configuracion.json: ' + String(e?.message || e), 'error'); } catch {}
    return false;
  } finally {
    try { if (lockFd !== null) fs.closeSync(lockFd); } catch {}
    try { fs.unlinkSync(lockPath); } catch {}
  }
}

function getDataCollection(name) {
  if (isControlApiConfigured()) return controlApi.collection(name);
  return mongoose?.connection?.db?.collection(name) || null;
}

function dataBackendReady() {
  if (isControlApiConfigured()) return true;
  return !!mongoose?.connection?.db;
}


function extractTenantConfigFromDoc(doc) {
  if (!doc || typeof doc !== "object") return {};
  const nested = (doc.configuracion && typeof doc.configuracion === "object") ? doc.configuracion : null;
  if (!nested) return doc;

  // Compatibilidad: algunos campos operativos pueden estar en la raíz del documento
  // tenant_config y otros dentro de configuracion. La configuración anidada gana
  // cuando el mismo campo existe en ambos lugares, pero no descartamos la raíz.
  return {
    ...doc,
    ...nested,
    _id: doc._id,
    tenantId: nested.tenantId ?? doc.tenantId ?? doc.tenantid,
    tenantid: nested.tenantid ?? doc.tenantid ?? doc.tenantId
  };
}


function applyTenantConfig(conf) {
  if (!conf || typeof conf !== "object") return;

  const hasValue = (v) => v !== undefined && v !== null && !(typeof v === "string" && v.trim() === "");
  const asNumber = (v, current) => {
    if (!hasValue(v)) return current;
    const n = Number(v);
    return Number.isFinite(n) ? n : current;
  };
  const asString = (v, current = "") => {
    if (!hasValue(v)) return current;
    return String(v).trim();
  };

  // Core. En worker multi-sesión el puerto lo asigna el supervisor para evitar
  // colisiones aunque distintos tenant_config tengan el mismo puerto histórico.
  const forcedWorkerPort = ASISTO_MULTI_WORKER ? Number(process.env.ASISTO_WORKER_PORT || 0) : 0;
  if (Number.isFinite(forcedWorkerPort) && forcedWorkerPort > 0) port = forcedWorkerPort;
  else if (hasValue(conf.puerto)) port = asNumber(conf.puerto, port);
  if (conf.headless !== undefined) {
    headless = parseBoolLike(conf.headless, !!headless);
  }

  // Motor usado para la conexión de WhatsApp Web. No confundir con whatsapp_transport,
  // que en otras partes del script significa API vs WWEB para determinados envíos.
  const forcedWorkerEngine = ASISTO_MULTI_WORKER ? String(process.env.ASISTO_WORKER_WWEB_ENGINE || '').trim() : '';
  const engineRaw = forcedWorkerEngine || (
    conf.wweb_engine ?? conf.wwebEngine ??
    conf.whatsapp_web_engine ?? conf.whatsappWebEngine ??
    conf.whatsapp_client_engine ?? conf.whatsappClientEngine
  );
  if (engineRaw !== undefined && engineRaw !== null && String(engineRaw).trim() !== '') {
    wweb_engine = normalizeWwebEngine(engineRaw);
  }

  const baileysAuthPathRaw = conf.baileys_auth_base_path ?? conf.baileysAuthBasePath ?? conf.baileys_auth_path ?? conf.baileysAuthPath;
  if (baileysAuthPathRaw !== undefined && baileysAuthPathRaw !== null && String(baileysAuthPathRaw).trim() !== '') {
    baileys_auth_base_path = String(baileysAuthPathRaw).trim();
  }

  if (!numero && (conf.numero || conf.NUMERO)) numero = asString(conf.numero || conf.NUMERO, numero);
  if (conf.status_token !== undefined) status_token = asString(conf.status_token, status_token);
  configureControlApiFromValues(conf);

  // Lock/lease
  lease_ms = asNumber(conf.lease_ms, lease_ms);
  heartbeat_ms = asNumber(conf.heartbeat_ms, heartbeat_ms);
  backup_every_ms = asNumber(conf.backup_every_ms, backup_every_ms);
  if (conf.auth_base_path !== undefined || conf.auth_path !== undefined) {
    auth_base_path = asString(conf.auth_base_path || conf.auth_path, auth_base_path);
  }
  // En Windows el backup (zip) puede bloquear el event loop varios segundos.
  // Si lease_ms es muy bajo, otra PC toma el lock aunque esta siga viva.
  if (!Number.isFinite(lease_ms) || lease_ms < MIN_LEASE_MS) lease_ms = MIN_LEASE_MS;

  if (conf.auth_mode !== undefined && conf.auth_mode !== null && String(conf.auth_mode).trim() !== '') {
    auth_mode = String(conf.auth_mode).trim().toLowerCase();
  }

  if (
    conf.panel_restart_mode !== undefined ||
    conf.panelRestartMode !== undefined ||
    conf.restart_mode !== undefined ||
    conf.restartMode !== undefined
  ) {
    panel_restart_mode = normalizePanelRestartMode(
      conf.panel_restart_mode ?? conf.panelRestartMode ?? conf.restart_mode ?? conf.restartMode,
      panel_restart_mode
    );
  }

  // Mensajes / límites
  seg_desde = asNumber(conf.seg_desde, seg_desde);
  seg_hasta = asNumber(conf.seg_hasta, seg_hasta);
   seg_desde2 = asNumber(
    conf.seg_desde2 ??
    conf.segDesde2 ??
    conf.seg_desde_diferente ??
    conf.segDesdeDiferente,
    seg_desde2
  );
  seg_hasta2 = asNumber(
    conf.seg_hasta2 ??
    conf.segHasta2 ??
    conf.seg_hasta_diferente ??
    conf.segHastaDiferente,
    seg_hasta2
  );
  if (conf.dsn !== undefined) dsn = String(conf.dsn);
  seg_msg = asNumber(conf.seg_msg, seg_msg);
  seg_tele = asNumber(conf.seg_tele, seg_tele);
  if (conf.api !== undefined) api = String(conf.api);
  if (
    conf.wweb_bot_logic_mode !== undefined ||
    conf.wwebBotLogicMode !== undefined ||
    conf.bot_logic_mode !== undefined ||
    conf.botLogicMode !== undefined
  ) {
    wweb_bot_logic_mode = normalizeWwebBotLogicMode(
      conf.wweb_bot_logic_mode ??
      conf.wwebBotLogicMode ??
      conf.bot_logic_mode ??
      conf.botLogicMode ??
      wweb_bot_logic_mode
    );
  }

  // Bot/API principal de mensajes entrantes. Es independiente de la consulta
  // de mensajes salientes. Por defecto queda habilitado para no cambiar el
  // comportamiento actual.
  if (
    conf.habilitar_bot !== undefined ||
    conf.habilitarBot !== undefined ||
    conf.bot_habilitado !== undefined ||
    conf.botHabilitado !== undefined ||
    conf.enable_bot !== undefined ||
    conf.enableBot !== undefined
  ) {
    habilitar_bot = parseBoolLike(
      conf.habilitar_bot ??
      conf.habilitarBot ??
      conf.bot_habilitado ??
      conf.botHabilitado ??
      conf.enable_bot ??
      conf.enableBot,
      habilitar_bot
    );
  }


  if (
    conf.runtime_config_refresh_ms !== undefined ||
    conf.runtimeConfigRefreshMs !== undefined ||
    conf.intervalo_refresco_config_ms !== undefined ||
   conf.intervaloRefrescoConfigMs !== undefined
  ) {
    runtime_config_refresh_ms = asNumber(
      conf.runtime_config_refresh_ms ??
      conf.runtimeConfigRefreshMs ??
      conf.intervalo_refresco_config_ms ??
      conf.intervaloRefrescoConfigMs,
      runtime_config_refresh_ms
    );
    if (!Number.isFinite(runtime_config_refresh_ms) || runtime_config_refresh_ms < 5000) runtime_config_refresh_ms = 5000;
  }


  // Consulta API de mensajes salientes (opcional, por tenant)
  if (conf.api2 !== undefined || conf.api_consulta_mensajes !== undefined || conf.apiConsultaMensajes !== undefined) {
    api2 = asString(conf.api2 ?? conf.api_consulta_mensajes ?? conf.apiConsultaMensajes, api2);
  }
 if (conf.api3 !== undefined || conf.api_actualiza_mensajes !== undefined || conf.apiActualizaMensajes !== undefined) {
    api3 = asString(conf.api3 ?? conf.api_actualiza_mensajes ?? conf.apiActualizaMensajes, api3);
  }
  if (conf.key !== undefined || conf.api_key !== undefined || conf.apiKey !== undefined || conf.api_mensajes_key !== undefined || conf.apiMensajesKey !== undefined) {
    key = asString(conf.key ?? conf.api_key ?? conf.apiKey ?? conf.api_mensajes_key ?? conf.apiMensajesKey, key);
  }

  if (
    conf.api_mensajes_alta !== undefined ||
    conf.apiMensajesAlta !== undefined ||
    conf.api_alta_mensajes !== undefined ||
    conf.apiAltaMensajes !== undefined
  ) {
    api_mensajes_alta = asString(
     conf.api_mensajes_alta ??
      conf.apiMensajesAlta ??
      conf.api_alta_mensajes ??
      conf.apiAltaMensajes,
      api_mensajes_alta
    );
  }
  if (
    conf.api_mensajes_alta_key !== undefined ||
    conf.apiMensajesAltaKey !== undefined ||
    conf.key_mensajes_alta !== undefined ||
    conf.keyMensajesAlta !== undefined ||
    conf.api_alta_mensajes_key !== undefined ||
    conf.apiAltaMensajesKey !== undefined
  ) {
    api_mensajes_alta_key = asString(
      conf.api_mensajes_alta_key ??
      conf.apiMensajesAltaKey ??
     conf.key_mensajes_alta ??
      conf.keyMensajesAlta ??
      conf.api_alta_mensajes_key ??
      conf.apiAltaMensajesKey,
      api_mensajes_alta_key
    );
  }

  if (
    conf.api_mensajes_alta_nro_tel_from !== undefined ||
    conf.apiMensajesAltaNroTelFrom !== undefined ||
    conf.nro_tel_from_mensajes_alta !== undefined ||
    conf.nroTelFromMensajesAlta !== undefined ||
    conf.api_alta_mensajes_nro_tel_from !== undefined ||
    conf.apiAltaMensajesNroTelFrom !== undefined ||
    conf.nro_tel_from !== undefined ||
    conf.nroTelFrom !== undefined
  ) {
    api_mensajes_alta_nro_tel_from = asString(
      conf.api_mensajes_alta_nro_tel_from ??
      conf.apiMensajesAltaNroTelFrom ??
      conf.nro_tel_from_mensajes_alta ??
      conf.nroTelFromMensajesAlta ??
      conf.api_alta_mensajes_nro_tel_from ??
      conf.apiAltaMensajesNroTelFrom ??
      conf.nro_tel_from ??
      conf.nroTelFrom,
      api_mensajes_alta_nro_tel_from
    );
  }

  if (
    conf.compra_mensajes_usar_api_alta !== undefined ||
    conf.compraMensajesUsarApiAlta !== undefined ||
    conf.usar_api_alta_compra !== undefined ||
    conf.usarApiAltaCompra !== undefined
 ) {
    compra_mensajes_usar_api_alta = parseBoolLike(
      conf.compra_mensajes_usar_api_alta ??
      conf.compraMensajesUsarApiAlta ??
      conf.usar_api_alta_compra ??
      conf.usarApiAltaCompra,
      compra_mensajes_usar_api_alta
    );
  
  }

  if (
    conf.entrega_mensajes_usar_api_alta !== undefined ||
    conf.entregaMensajesUsarApiAlta !== undefined ||
    conf.usar_api_alta_entrega !== undefined ||
    conf.usarApiAltaEntrega !== undefined
  ) {
    entrega_mensajes_usar_api_alta = parseBoolLike(
      conf.entrega_mensajes_usar_api_alta ??
      conf.entregaMensajesUsarApiAlta ??
      conf.usar_api_alta_entrega ??
      conf.usarApiAltaEntrega,
      entrega_mensajes_usar_api_alta
    );
  
  }


  if (
    conf.habilitar_consulta_mensajes !== undefined ||
    conf.habilitarConsultaMensajes !== undefined ||
    conf.consulta_api_mensajes_habilitado !== undefined ||
    conf.consultaApiMensajesHabilitado !== undefined ||
    conf.consulta_api_mensajes_enabled !== undefined ||
    conf.consultaApiMensajesEnabled !== undefined ||
    conf.envio_mensajes_habilitado !== undefined ||
    conf.envioMensajesHabilitado !== undefined
  ) {
    consulta_api_mensajes_habilitado = parseBoolLike(
      conf.habilitar_consulta_mensajes ??
      conf.habilitarConsultaMensajes ??
      conf.consulta_api_mensajes_habilitado ??
      conf.consultaApiMensajesHabilitado ??
      conf.consulta_api_mensajes_enabled ??
      conf.consultaApiMensajesEnabled ??
      conf.envio_mensajes_habilitado ??
      conf.envioMensajesHabilitado,
      consulta_api_mensajes_habilitado
    );
  }

  if (
    conf.habilitar_mensajes_info !== undefined ||
    conf.habilitarMensajesInfo !== undefined ||
    conf.mensajes_info_habilitado !== undefined ||
    conf.mensajesInfoHabilitado !== undefined ||
    conf.enviar_mensajes_info_habilitado !== undefined ||
    conf.enviarMensajesInfoHabilitado !== undefined ||
    conf.enable_mensajes_info !== undefined ||
    conf.enableMensajesInfo !== undefined
  ) {
    habilitar_mensajes_info = parseBoolLike(
      conf.habilitar_mensajes_info ??
      conf.habilitarMensajesInfo ??
      conf.mensajes_info_habilitado ??
      conf.mensajesInfoHabilitado ??
      conf.enviar_mensajes_info_habilitado ??
      conf.enviarMensajesInfoHabilitado ??
      conf.enable_mensajes_info ??
     conf.enableMensajesInfo,
      habilitar_mensajes_info
    );
  }


  if (
    conf.habilitar_odbc_manager !== undefined ||
    conf.habilitarOdbcManager !== undefined ||
    conf.odbc_manager_habilitado !== undefined ||
    conf.odbcManagerHabilitado !== undefined ||
    conf.habilitar_manager_local !== undefined ||
   conf.habilitarManagerLocal !== undefined
  ) {
    habilitar_odbc_manager = parseBoolLike(
      conf.habilitar_odbc_manager ??
      conf.habilitarOdbcManager ??
      conf.odbc_manager_habilitado ??
      conf.odbcManagerHabilitado ??
      conf.habilitar_manager_local ??
      conf.habilitarManagerLocal,
      habilitar_odbc_manager
    );
  }


  if (
    conf.consulta_mensajes_respetar_horarios !== undefined ||
    conf.consultaMensajesRespetarHorarios !== undefined ||
    conf.consulta_api_mensajes_respetar_horarios !== undefined ||
    conf.consultaApiMensajesRespetarHorarios !== undefined ||
    conf.respetar_horarios_consulta_mensajes !== undefined ||
   conf.respetarHorariosConsultaMensajes !== undefined
  ) {
    consulta_mensajes_respetar_horarios = parseBoolLike(
      conf.consulta_mensajes_respetar_horarios ??
      conf.consultaMensajesRespetarHorarios ??
      conf.consulta_api_mensajes_respetar_horarios ??
      conf.consultaApiMensajesRespetarHorarios ??
      conf.respetar_horarios_consulta_mensajes ??
      conf.respetarHorariosConsultaMensajes,
      consulta_mensajes_respetar_horarios
    );
  }

  if (
    conf.consulta_mensajes_fuera_horario_sleep_ms !== undefined ||
    conf.consultaMensajesFueraHorarioSleepMs !== undefined ||
    conf.consulta_api_mensajes_fuera_horario_sleep_ms !== undefined ||
    conf.consultaApiMensajesFueraHorarioSleepMs !== undefined
  ) {
    consulta_mensajes_fuera_horario_sleep_ms = asNumber(
      conf.consulta_mensajes_fuera_horario_sleep_ms ??
      conf.consultaMensajesFueraHorarioSleepMs ??
      conf.consulta_api_mensajes_fuera_horario_sleep_ms ??
      conf.consultaApiMensajesFueraHorarioSleepMs,
      consulta_mensajes_fuera_horario_sleep_ms
    );
    if (!Number.isFinite(consulta_mensajes_fuera_horario_sleep_ms) || consulta_mensajes_fuera_horario_sleep_ms < 5000) {
      consulta_mensajes_fuera_horario_sleep_ms = 60000;
    }
  }

  if (
    conf.api_mensajes_confirmacion_habilitada !== undefined ||
   conf.apiMensajesConfirmacionHabilitada !== undefined ||
    conf.confirmar_api_mensajes !== undefined ||
    conf.confirmarApiMensajes !== undefined
  ) {
    api_mensajes_confirmacion_habilitada = parseBoolLike(
      conf.api_mensajes_confirmacion_habilitada ??
      conf.apiMensajesConfirmacionHabilitada ??
      conf.confirmar_api_mensajes ??
      conf.confirmarApiMensajes,
      api_mensajes_confirmacion_habilitada
    );
  }
  if (conf.api_mensajes_confirmacion_mensaje !== undefined || conf.apiMensajesConfirmacionMensaje !== undefined) {
    api_mensajes_confirmacion_mensaje = String(conf.api_mensajes_confirmacion_mensaje ?? conf.apiMensajesConfirmacionMensaje ?? api_mensajes_confirmacion_mensaje);
  }
  if (conf.api_mensajes_confirmacion_respuestas_ok !== undefined || conf.apiMensajesConfirmacionRespuestasOk !== undefined) {
    api_mensajes_confirmacion_respuestas_ok = conf.api_mensajes_confirmacion_respuestas_ok ?? conf.apiMensajesConfirmacionRespuestasOk;
  }
  if (conf.api_mensajes_confirmacion_reenviar_ms !== undefined || conf.apiMensajesConfirmacionReenviarMs !== undefined) {
   api_mensajes_confirmacion_reenviar_ms = asNumber(
      conf.api_mensajes_confirmacion_reenviar_ms ?? conf.apiMensajesConfirmacionReenviarMs,
      api_mensajes_confirmacion_reenviar_ms
    );
    if (!Number.isFinite(api_mensajes_confirmacion_reenviar_ms) || api_mensajes_confirmacion_reenviar_ms < 0) api_mensajes_confirmacion_reenviar_ms = 86400000;
  }
  if (conf.api_mensajes_confirmacion_validez_ms !== undefined || conf.apiMensajesConfirmacionValidezMs !== undefined) {
    api_mensajes_confirmacion_validez_ms = asNumber(
      conf.api_mensajes_confirmacion_validez_ms ?? conf.apiMensajesConfirmacionValidezMs,
      api_mensajes_confirmacion_validez_ms
    );
    if (!Number.isFinite(api_mensajes_confirmacion_validez_ms) || api_mensajes_confirmacion_validez_ms < 0) api_mensajes_confirmacion_validez_ms = 0;
  }

 

  if (conf.msg_inicio !== undefined) msg_inicio = String(conf.msg_inicio ?? "");
  if (conf.msg_fin !== undefined) msg_fin = String(conf.msg_fin ?? "");
  cant_lim = asNumber(conf.cant_lim, cant_lim);
  if (conf.msg_lim !== undefined) msg_lim = String(conf.msg_lim ?? "");
  const timeCadRaw =
    conf.time_cad ??
    conf.timeCad ??
    conf.caducidad_mensaje_ms ??
    conf.caducidadMensajeMs ??
    conf.continuar_timeout_ms ??
    conf.continuarTimeoutMs;
  if (timeCadRaw !== undefined) {
    const n = Number(timeCadRaw);
    // time_cad se usa en milisegundos para caducar la espera de Continuar S/N.
    // No se convierte a segundos: si en Mongo dice 60000, son 60 segundos.
    if (Number.isFinite(n) && n >= 0) time_cad = n;
  }
  if (conf.msg_cad !== undefined) msg_cad = String(conf.msg_cad ?? "");
  if (conf.msg_can !== undefined) msg_can = String(conf.msg_can ?? "");
  if (conf.nom_emp !== undefined) nom_chatbot = String(conf.nom_emp);
  if (conf.nom_chatbot !== undefined) nom_chatbot = String(conf.nom_chatbot);

    const panelRestartModeRaw =
    conf.panel_restart_mode ??
    conf.panelRestartMode ??
    conf.wweb_panel_restart_mode ??
    conf.wwebPanelRestartMode ??
    conf.restart_mode ??
    conf.restartMode ??
    conf.modo_reinicio_panel ??
    conf.modoReinicioPanel;
  if (panelRestartModeRaw !== undefined && panelRestartModeRaw !== null && String(panelRestartModeRaw).trim() !== '') {
    panel_restart_mode = normalizePanelRestartMode(panelRestartModeRaw, panel_restart_mode);
  } else {
    panel_restart_mode = normalizePanelRestartMode(panel_restart_mode, 'task_runner');
  }

  applyAutoUpdateConfig(conf);
}

async function loadTenantConfigFromDb() {
  const boot = readBootstrapFromFile();
  if (!tenantId && boot.tenantId) tenantId = String(boot.tenantId).trim();

  // Normalizar tenantId para evitar locks duplicados por mayúsculas/espacios
  tenantId = String(tenantId || '').trim();
  if (tenantId) tenantId = tenantId.toUpperCase();
  if (!mongo_uri && (boot.mongo_uri || boot.mongoUri)) mongo_uri = String(boot.mongo_uri || boot.mongoUri).trim();
  if (!mongo_db && (boot.mongo_db || boot.mongoDb || boot.dbName)) mongo_db = String(boot.mongo_db || boot.mongoDb || boot.dbName).trim();
  if (!mongo_db) mongo_db = "Cluster0";
  configureControlApiFromValues(boot);
  controlApi.configure({ tenantId, numero });

  if (!tenantId || (!mongo_uri && !isControlApiConfigured())) {
    throw new Error("Falta tenantId y backend de datos en configuracion.json");
  }
 
  const startedWithControlApi = isControlApiConfigured();

  const ok = await ensureMongo();
  if (!ok || !dataBackendReady()) throw new Error("No se pudo conectar al backend de configuración");

  const collName = String(process.env.ASISTO_CONFIG_COLLECTION || "tenant_config").trim() || "tenant_config";
  const coll = getDataCollection(collName);

  let doc = await coll.findOne({ _id: tenantId });
  if (!doc) doc = await coll.findOne({ tenantId: tenantId });
  if (!doc) throw new Error(`No existeconfiguración en BD para tenantId=${tenantId} (${collName})`);
 
  // No requiere token ni edición manual previa. Si falta, esta primera conexión
  // Mongo lo crea en tenant_config y continúa la migración a HTTPS.
  if (!startedWithControlApi) {
    doc = await ensureControlApiBootstrapInTenantConfig(coll, doc);
  }
  const conf = extractTenantConfigFromDoc(doc);
  tenantConfig = conf;
  applyTenantConfig(conf);
  controlApi.configure({ tenantId, numero });

  // Migración automática: la versión nueva lee una última vez tenant_config
  // desde Mongo, guarda URL/token y libera inmediatamente el MongoClient local.
  if (!startedWithControlApi && isControlApiConfigured()) {
    await persistControlApiBootstrap();
    await disconnectMongoSafe('migrated_to_control_api');
    try { console.log('[CONTROL_API] migración completada; MongoDB directo deshabilitado'); } catch {}
    try { EscribirLog('[CONTROL_API] migración completada; MongoDB directo deshabilitado', 'event'); } catch {}
  }
  try {
   // console.log(`[CONFIG] tenantId=${tenantId} numero=${numero || ""} puerto=${port} headless=${headless} seg_desde=${seg_desde}`);
  } catch {}
  return true;
}

function sessionLog(msg) {
  try { console.log(msg); } catch {}
  try { EscribirLog(msg, "event"); } catch {}
}

function arDatePartsForStats(date) {
  try {
    const parts = new Intl.DateTimeFormat('sv-SE', {
      timeZone: AR_TZ,
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false
    }).formatToParts(date || new Date());

    const map = {};
    for (const p of (parts || [])) {
      if (p && p.type) map[p.type] = p.value;
    }
    const y = map.year || '0000';
    const m = map.month || '00';
    const d = map.day || '00';
    const hh = map.hour || '00';
    const mm = map.minute || '00';
    const ss = map.second || '00';
    return {
      dayKey: `${y}-${m}-${d}`,
      atLocal: `${y}-${m}-${d}T${hh}:${mm}:${ss}`
    };
  } catch {
    const dt = date || new Date();
    const iso = dt.toISOString();
    return { dayKey: iso.slice(0, 10), atLocal: iso.slice(0, 19) };
  }
}

// WhatsApp Web puede entregar remotos como @lid, sobre todo en Linux/nuevas sesiones.
// Para el API y las estadísticas necesitamos el teléfono real cuando whatsapp-web.js
// lo puede resolver desde el contacto.
const waContactPhoneCache = new Map();

function stripWhatsappSuffix(value) {
  return String(value || '')
    .replace(/^whatsapp:/i, '')
    .replace(/@c\.us$/i, '')
    .replace(/@s\.whatsapp\.net$/i, '')
    .trim();
}

function onlyDigits(value) {
  return String(value || '').replace(/\D/g, '');
}


// =========================
// Filtro global de clientes habilitados
// - Configuración en settings._id = client_phone_access:<TENANT>
// - Desactivado o inexistente: responden todos.
// - Activado: solo responde a los números/IDs cargados.
// =========================
const clientPhoneAccessCache = new Map();
const CLIENT_PHONE_ACCESS_CACHE_TTL_MS = Math.max(
  1000,
  Number(process.env.CLIENT_PHONE_ACCESS_CACHE_TTL_MS || 5000) || 5000
);

function normalizeClientAccessTenant(value) {
  return String(value || 'default').trim().toUpperCase() || 'DEFAULT';
}

function normalizeClientAccessIdentifier(value) {
  let d = onlyDigits(value);
  if (!d) return '';
  if (d.startsWith('00')) d = d.slice(2);

  if (d.startsWith('549') && d.length >= 13) return d;
  if (d.startsWith('54') && !d.startsWith('549') && d.length >= 12) {
    return '549' + d.slice(2);
  }
  if (d.length === 10 && d.startsWith('3')) {
    return '549' + d;
  }
  return d;
}

function normalizeClientAccessNumbers(raw) {
  const arr = Array.isArray(raw) ? raw : [];
  const set = new Set();
  for (const item of arr) {
    const source = item && typeof item === 'object'
      ? (item.number ?? item.numero ?? item.phone ?? item.telefono ?? item.id ?? '')
      : item;
    const normalized = normalizeClientAccessIdentifier(source);
    if (normalized) set.add(normalized);
  }
  return set;
}

async function loadClientPhoneAccessConfigLocal(force = false) {
  const tenant = normalizeClientAccessTenant(tenantId);
  const now = Date.now();
  const cached = clientPhoneAccessCache.get(tenant);
  if (!force && cached && (now - cached.at) < CLIENT_PHONE_ACCESS_CACHE_TTL_MS) {
    return cached.value;
 }

  try {
    if (!await ensureMongo() || !dataBackendReady()) {
      if (cached?.value) return cached.value;
      return { enabled: false, numbers: new Set() };
    }

    const doc = await getDataCollection('settings').findOne({
      _id: 'client_phone_access:' + tenant
    });

    const enabled = parseBoolLike(
      doc?.enabled ?? doc?.filterEnabled ?? doc?.habilitado,
      false
    );
    const value = {
      enabled,
      numbers: normalizeClientAccessNumbers(
        doc?.numbers ?? doc?.numeros ?? doc?.allowedNumbers ?? []
      )
    };
    clientPhoneAccessCache.set(tenant, { at: now, value });
    return value;
  } catch (e) {
    if (cached?.value) return cached.value;
    try {
      EscribirLog('[CLIENT_ACCESS] error leyendo configuración: ' + String(e?.message || e), 'error');
    } catch {}
    // Compatibilidad: si no puede leerse la configuración y nunca hubo cache,
    // se mantiene el comportamiento predeterminado de responder a todos.
    return { enabled: false, numbers: new Set() };
 }
}

async function isIncomingClientAllowedLocal(identifier) {
  // La PC de WhatsApp Web es otro proceso: se fuerza lectura para que un cambio
  // guardado en el panel se aplique al mensaje siguiente sin esperar el cache.
  const config = await loadClientPhoneAccessConfigLocal(true);
  const normalized = normalizeClientAccessIdentifier(identifier);

  if (!config.enabled) {
    return { allowed: true, enabled: false, normalized, reason: 'filter_disabled' };
  }

  if (!normalized) {
    return { allowed: false, enabled: true, normalized: '', reason: 'identifier_missing' };
  }

  const allowed = config.numbers.has(normalized);
  return {
    allowed,
    enabled: true,
    normalized,
    reason: allowed ? 'listed' : 'not_listed'
  };

}

function normalizarNroTelFromApiMensajes(value) {
  const n = onlyDigits(value || '');
  if (!n) return '';

  // En Argentina, el usuario del API está registrado como 549 + número local.
  // WhatsApp/QR puede dejar el teléfono como 346..., 54346... o 549346...
  if (n.startsWith('549')) return n;
  if (n.startsWith('54') && !n.startsWith('549') && n.length >= 12) return '549' + n.slice(2);
  if (n.length === 10 && n.startsWith('3')) return '549' + n;
  return n;
}

function getApiMensajesNroTelFrom() {
  const candidatos = [
    api_mensajes_alta_nro_tel_from,
    tenantConfig?.api_mensajes_nro_tel_from,
    tenantConfig?.apiMensajesNroTelFrom,
    tenantConfig?.api_mensajes_alta_nro_tel_from,
    tenantConfig?.apiMensajesAltaNroTelFrom,
    tenantConfig?.nro_tel_from,
    tenantConfig?.nroTelFrom,
    tenantConfig?.telefono_qr,
    tenantConfig?.telefonoQr,
    numero,
    telefono_qr,
    telefono_local
  ];

  for (const c of candidatos) {
    const n = normalizarNroTelFromApiMensajes(c);
    if (n) return n;
  }
  return '';
}

function looksLikeLid(value) {
  return /@lid$/i.test(String(value || '').trim());
}


function lidDigitsFromRaw(value) {
  if (!looksLikeLid(value)) return '';
  return onlyDigits(stripWhatsappSuffix(value));
}

function expectedApiPhonePrefix() {
  try {
    const configured = onlyDigits(
      tenantConfig?.api_phone_country_prefix ||
      tenantConfig?.apiPhoneCountryPrefix ||
      tenantConfig?.phone_country_prefix ||
      process.env.API_PHONE_COUNTRY_PREFIX ||
      ''
    );
    if (configured) return configured;

    // Si el WhatsApp del tenant es argentino, no aceptamos candidatos que no
    // empiecen con 54. Esto evita tomar IDs internos de WhatsApp como teléfono.
    const own = onlyDigits(numero || telefono_qr || '');
    if (own.startsWith('54')) return '54';
  } catch {}
  return '';
}

function validPhoneCandidateForRaw(rawId, candidate) {
  const p = onlyDigits(stripWhatsappSuffix(candidate));
  // Teléfonos E.164: máximo 15 dígitos. Menos de 10 suele ser dato incompleto.
  if (!p || p.length < 10 || p.length > 15) return '';

  // CLAVE: cuando WhatsApp entrega @lid, whatsapp-web.js puede devolver
 
  // c.number = "150607..." que NO es teléfono, es el mismo LID sin sufijo.
  
  const lidDigits = lidDigitsFromRaw(rawId);
  if (lidDigits && p === lidDigits) return '';
  // En tus tenants argentinos, Tel_Origen debe entrar con prefijo país 54.
  // Si alguna vez tenés otro país, podés configurar api_phone_country_prefix.
  const prefix = expectedApiPhonePrefix();
  if (prefix && !p.startsWith(prefix)) return '';

  return p;
}

function readPhoneFromConfiguredLidMap(rawId) {
  try {
    if (!looksLikeLid(rawId)) return '';
    const raw = String(rawId || '').trim();
    const lid = stripWhatsappSuffix(raw);
    const candidates = [
      tenantConfig?.lid_phone_map,
      tenantConfig?.lidPhoneMap,
      tenantConfig?.wa_lid_phone_map,
      tenantConfig?.waLidPhoneMap
    ].filter(Boolean);

    for (const map of candidates) {
      if (Array.isArray(map)) {
        for (const row of map) {
          if (!row || typeof row !== 'object') continue;
          const rowLid = String(row.lid || row.waLid || row.remote || row.id || '').trim();
          if (!rowLid) continue;
          if (rowLid === raw || stripWhatsappSuffix(rowLid) === lid) {
            const phone = row.phone || row.telefono || row.numero || row.phoneNumber || row.number || '';
            const ok = validPhoneCandidateForRaw(raw, phone);
            if (ok) return ok;
          }
        }
      } else if (map && typeof map === 'object') {
        const phone = map[raw] || map[lid] || map[raw.toLowerCase()] || map[lid.toLowerCase()] || '';
        const ok = validPhoneCandidateForRaw(raw, phone);
        if (ok) return ok;
      }
    }
  } catch {}
  return '';
}

async function readPhoneFromMongoLidMap(rawId) {
  try {
    if (!looksLikeLid(rawId)) return '';
   if (!await ensureMongo()) return '';
    if (!dataBackendReady()) return '';

    const raw = String(rawId || '').trim();
    const lid = stripWhatsappSuffix(raw);
    const coll = getDataCollection('wa_lid_phone_map');
    const tenant = String(tenantId || '').trim();

    const baseOr = [
      { lid: raw },
      { lid },
      { waLid: raw },
      { waLid: lid },
      { remote: raw },
      { remote: lid }
    ];

    let doc = null;
    if (tenant) {
     doc = await coll.findOne({
        $and: [
          { $or: [{ tenantId: tenant }, { tenantid: tenant }] },
          { $or: baseOr }
        ]
      });
    }
    if (!doc) doc = await coll.findOne({ $or: baseOr });

    const phone = doc?.phone || doc?.telefono || doc?.numero || doc?.phoneNumber || doc?.number || '';
    return validPhoneCandidateForRaw(raw, phone);
  } catch (e) {
    try { EscribirLog('readPhoneFromMongoLidMap error: ' + String(e?.message || e), 'error'); } catch {}
    return '';
  }
}


function rememberContactPhone(rawId, phone) {
  try {
    const p = validPhoneCandidateForRaw(rawId, phone);
    if (!p) return '';

    const raw = String(rawId || '').trim();
    const cleanRaw = stripWhatsappSuffix(raw);

    if (raw) waContactPhoneCache.set(raw, p);
    if (cleanRaw) waContactPhoneCache.set(cleanRaw, p);
    waContactPhoneCache.set(p, p);
    waContactPhoneCache.set(p + '@c.us', p);

    return p;
  } catch {
    return '';
  }
}

async function resolvePhoneFromContactId(contactId) {
  const raw = String(contactId || '').trim();
  if (!raw) return '';

  const cleanRaw = stripWhatsappSuffix(raw);
  const cached = waContactPhoneCache.get(raw) || waContactPhoneCache.get(cleanRaw);
 if (cached) return cached;

  // Si ya vino como teléfono real, no hace falta consultar.
  if (!looksLikeLid(raw)) {
    const digits = validPhoneCandidateForRaw(raw, cleanRaw);
    if (digits) return rememberContactPhone(raw, digits);
  }
  // Mapeo manual opcional: permite resolver LID -> teléfono desde tenant_config.
  const configured = readPhoneFromConfiguredLidMap(raw);
  if (configured) return rememberContactPhone(raw, configured);

  // Si vino como @lid, intentamos resolverlo desde whatsapp-web.js.
  try {
    if (client && typeof client.getContactById === 'function') {
      const c = await client.getContactById(raw);

      const number = validPhoneCandidateForRaw(raw, c?.number || '');
      if (number) return rememberContactPhone(raw, number);

      const idUser = String(c?.id?.user || '').trim();
      const serialized = String(c?.id?._serialized || '').trim();
      if (idUser && !looksLikeLid(serialized)) {
        const idDigits = validPhoneCandidateForRaw(raw, idUser);
        if (idDigits) return rememberContactPhone(raw, idDigits);
      }
    }
  } catch (e) {
    try { EscribirLog('resolvePhoneFromContactId no pudo resolver ' + raw + ': ' + String(e?.message || e), 'event'); } catch {}
  }
  // Mapeo manual opcional desde Mongo: colección wa_lid_phone_map.
  const fromMongo = await readPhoneFromMongoLidMap(raw);
  if (fromMongo) return rememberContactPhone(raw, fromMongo);

  return '';
}

async function resolvePhoneFromIncomingMessage(message) {
  try {
    if (!message) return '';
    const from = String(message.from || '').trim();
    if (from === 'status@broadcast') return from;

    try {
      if (typeof message.getContact === 'function') {
        const c = await message.getContact();
        const number = validPhoneCandidateForRaw(from, c?.number || '');
        if (number) return rememberContactPhone(from, number);
      }
    } catch {}
    // No revisar recursivamente message._data/message.id/chat._data: ahí aparecen
    // IDs internos de WhatsApp que pueden parecer números pero NO son teléfonos.

    const byId = await resolvePhoneFromContactId(from);
    if (byId) return byId;

    // Último fallback: si era @lid, no inventamos teléfono.
    // Sin resolver el número real, no conviene mandarlo al API como si fuera teléfono.
    if (looksLikeLid(from)) return '';
    return stripWhatsappSuffix(from);
  } catch {
    const from = String(message?.from || '');
    if (looksLikeLid(from)) return '';
    return stripWhatsappSuffix(from);
  }
}

async function normalizeContactForStats(contact) {
  const raw = String(contact || '').trim();
  if (!raw) return '';

  const resolved = await resolvePhoneFromContactId(raw);
  if (resolved) return resolved;

  // Si quedó @lid sin resolver, no lo guardamos como contacto porque duplica
  // estadísticas y no representa el teléfono real del cliente.
  if (looksLikeLid(raw)) return '';
  return stripWhatsappSuffix(raw);
}



async function logMessageStat(direction, contact, payload) {
  try {
    if (!tenantId || !numero) return;
    if (!await ensureMongo()) return;
    if (!MessageLogModel) return;

    const dir = String(direction || '').trim().toLowerCase();
    if (dir !== 'in' && dir !== 'out') return;

    const now = new Date();
    const parts = arDatePartsForStats(now);

    let messageType = 'text';
    let hasMedia = false;
    let body = '';

    if (typeof payload === 'string') {
      body = payload;
      messageType = 'text';
    } else if (payload && typeof payload === 'object') {
      if (typeof payload.body === 'string') body = payload.body;
      if (typeof payload.caption === 'string' && !body) body = payload.caption;
      if (payload.type) messageType = String(payload.type);
      if (payload.mimetype || payload.filename || payload.data) hasMedia = true;
      if (payload.hasMedia === true) hasMedia = true;
      if (!messageType || messageType === 'undefined') messageType = hasMedia ? 'media' : 'text';
    }

    body = String(body || '');
    const cleanContact = await normalizeContactForStats(contact);
    if (!cleanContact) return;

    await MessageLogModel.create({
      tenantId: String(tenantId || ''),
      numero: String(numero || ''),
      contact: cleanContact,
      direction: dir,
      messageType: messageType || (hasMedia ? 'media' : 'text'),
      body,
      bodyLength: body.length,
      hasMedia: !!hasMedia,
      at: now,
      atLocal: parts.atLocal,
      dayKey: parts.dayKey
    });
  } catch (e) {
    try { EscribirLog('logMessageStat error: ' + String(e?.message || e), 'error'); } catch {}
  }
}

function apiMensajesWindowConfig() {
  const rawMinutes =
    tenantConfig?.api_mensajes_window_minutes ??
    tenantConfig?.apiMensajesWindowMinutes ??
    tenantConfig?.api_mensajes_billing_window_minutes ??
    tenantConfig?.apiMensajesBillingWindowMinutes ??
    20;
  const rawValue =
    tenantConfig?.api_mensajes_window_value ??
    tenantConfig?.apiMensajesWindowValue ??
    tenantConfig?.api_mensajes_billing_window_value ??
    tenantConfig?.apiMensajesBillingWindowValue ??
    0;
  const rawCurrency =
    tenantConfig?.api_mensajes_window_currency ??
    tenantConfig?.apiMensajesWindowCurrency ??
    tenantConfig?.api_mensajes_billing_currency ??
    tenantConfig?.apiMensajesBillingCurrency ??
    'ARS';

  let minutes = Number(rawMinutes);
  if (!Number.isFinite(minutes) || minutes <= 0) minutes = 20;
  minutes = Math.max(1, Math.min(1440, Math.round(minutes)));

  let value = Number(typeof rawValue === 'string' ? rawValue.replace(',', '.') : rawValue);
  if (!Number.isFinite(value) || value < 0) value = 0;

  let currency = String(rawCurrency || 'ARS').trim().toUpperCase();
  if (!/^[A-Z]{3}$/.test(currency)) currency = 'ARS';

  return { minutes, value, currency };
}

function apiMensajesWindowMessageEntry({ at, messageType, text, idDest, idRenglon, messageId }) {
  return {
    at: at || new Date(),
    type: String(messageType || 'text').slice(0, 30),
    text: String(text || '').slice(0, 1000),
    id_msj_dest: idDest == null ? null : String(idDest).slice(0, 120),
    id_msj_renglon: idRenglon == null ? null : String(idRenglon).slice(0, 120),
    waMessageId: String(messageId || '').slice(0, 250)
  };
}

async function recordApiMensajesBillingWindow(contact, details = {}) {
  try {
    if (!tenantId || !numero) return null;
    if (!await ensureMongo()) return null;
    if (!ApiMessageWindowModel) return null;

    const cleanContact = await normalizeContactForStats(contact);
    if (!cleanContact) return null;

    const now = new Date();
    const cfg = apiMensajesWindowConfig();
    const numeroFrom = normalizarNroTelFromApiMensajes(getApiMensajesNroTelFrom()) || onlyDigits(numero) || String(numero || '');
    const messageId = getOutgoingStatMessageId(details.sentMessage);
    const entry = apiMensajesWindowMessageEntry({
      at: now,
     messageType: details.messageType,
      text: details.text,
      idDest: details.idDest,
      idRenglon: details.idRenglon,
      messageId
    });

    // Ventana fija: comienza con el primer mensaje. Los mensajes enviados antes
    // de windowEndsAt pertenecen a la misma ventana; el siguiente abre una nueva.
    const active = await ApiMessageWindowModel.findOne(
      {
        tenantId: String(tenantId),
        numeroFrom: String(numeroFrom),
        contact: cleanContact,
        channelType: 'api_messages',
        windowEndsAt: { $gt: now }
      },
      { sort: { windowStartedAt: -1 } }
    ).lean();

    if (active && active._id) {
      await ApiMessageWindowModel.updateOne(
       { _id: active._id },
        {
          $set: {
            lastMessageAt: now,
            updatedAt: now
          },
          $inc: { messageCount: 1 },
          $push: { messages: { $each: [entry], $slice: -100 } }
        }
      );
      return { _id: active._id, isNew: false, amount: Number(active.amount || 0), currency: active.currency || cfg.currency };
    }

    const windowEndsAt = new Date(now.getTime() + cfg.minutes * 60 * 1000);
    const doc = {
      tenantId: String(tenantId),
      numeroFrom: String(numeroFrom),
      contact: cleanContact,
     contactName: String(details.contactName || '').slice(0, 200),
      channelType: 'api_messages',
      source: 'consulta_api_mensajes',
      windowMinutes: cfg.minutes,
      windowStartedAt: now,
      windowEndsAt,
      lastMessageAt: now,
      messageCount: 1,
      unitValue: cfg.value,
      amount: cfg.value,
      currency: cfg.currency,
      messages: [entry],
      createdAt: now,
     updatedAt: now
    };
    const created = await ApiMessageWindowModel.create(doc);
    try {
      const log = '[API_MENSAJES_WINDOW] nueva ventana tenant=' + String(tenantId) +
        ' nro=' + cleanContact + ' minutos=' + cfg.minutes +
        ' valor=' + cfg.value + ' ' + cfg.currency;
      console.log(log);
      EscribirLog(log, 'event');
    } catch {}
    return { _id: created?._id || null, isNew: true, amount: cfg.value, currency: cfg.currency };
  } catch (e) {
    try { EscribirLog('[API_MENSAJES_WINDOW] error: ' + String(e?.message || e), 'error'); } catch {}
    return null;
  }
}


function getOutgoingStatMessageId(messageLike) {
  try {
    if (!messageLike) return '';
    if (typeof messageLike === 'string') return String(messageLike || '').trim();
    const serialized = messageLike?.id?._serialized || messageLike?._data?.id?.id || messageLike?.id?.id || messageLike?.ackId;
    return String(serialized || '').trim();
  } catch {
    return '';
  }
}

const recentOutgoingStatIds = new Map();

function rememberOutgoingStatLogged(messageLike) {
  try {
    const id = getOutgoingStatMessageId(messageLike);
    if (!id) return;
    const now = Date.now();
    recentOutgoingStatIds.set(id, now);
    for (const [k, ts] of recentOutgoingStatIds.entries()) {
      if (!ts || (now - ts) > 10 * 60 * 1000) recentOutgoingStatIds.delete(k);
    }
  } catch {}
}

function wasOutgoingStatLogged(messageLike) {
  try {
    const id = getOutgoingStatMessageId(messageLike);
    if (!id) return false;
   const ts = recentOutgoingStatIds.get(id);
    if (!ts) return false;
    if ((Date.now() - ts) > 10 * 60 * 1000) {
      recentOutgoingStatIds.delete(id);
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

async function logOutgoingFromMessageFallback(messageLike) {
  try {
    if (!messageLike) return false;
    if (messageLike.fromMe !== true) return false;
    if (wasOutgoingStatLogged(messageLike)) return false;

    const toRaw = String(messageLike.to || messageLike.from || '').trim();
    if (!toRaw) return false;
    const to = await normalizeContactForStats(toRaw);
    if (!to) return false;

    const payload = {
      body: typeof messageLike.body === 'string' ? messageLike.body : '',
      caption: typeof messageLike.caption === 'string' ? messageLike.caption : (typeof messageLike._data?.caption === 'string' ? messageLike._data.caption : ''),
      type: messageLike.type || messageLike._data?.type || 'text',
      hasMedia: !!(messageLike.hasMedia || messageLike._data?.mediaKey || messageLike._data?.isViewOnce)
    };

    await logMessageStat('out', to, payload);
    rememberOutgoingStatLogged(messageLike);
    return true;
  } catch (e) {
    try { EscribirLog('logOutgoingFromMessageFallback error: ' + String(e?.message || e), 'error'); } catch {}
    return false;
  }
}

// Dedupe de entrada: en algunas sesiones Linux/MD, whatsapp-web.js puede entregar
// entrantes por message_create en vez de message, o por ambos. Procesamos el primero
// y saltamos duplicados para no llamar dos veces al API.
const recentIncomingProcessIds = new Map();
const incomingCreateFallbackTimers = new Map();

function getMessageStableId(message) {
  try {
    const id = message?.id?._serialized || message?._data?.id?._serialized || message?._data?.id?.id || message?.id?.id || '';
    if (id) return String(id);
    const from = String(message?.from || message?._data?.from || '');
    const to = String(message?.to || message?._data?.to || '');
    const body = String(message?.body || message?._data?.body || '');
    const ts = String(message?.timestamp || message?._data?.t || '');
    return [from, to, ts, body].join('|');
  } catch {
    return '';
  }
}

function shouldProcessIncomingMessage(message, source) {
  try {
    if (!message) return false;
    if (message.fromMe === true) return false;

    const id = getMessageStableId(message);
    if (!id) return true;

    const now = Date.now();
    for (const [k, ts] of recentIncomingProcessIds.entries()) {
      if (!ts || (now - ts) > 2 * 60 * 1000) recentIncomingProcessIds.delete(k);
    }

    if (recentIncomingProcessIds.has(id)) {
      try { console.log('[INCOMING] duplicado skip source=' + String(source || '') + ' id=' + id); } catch {}
      return false;
    }

    recentIncomingProcessIds.set(id, now);
    const pendingTimer = incomingCreateFallbackTimers.get(id);
    if (pendingTimer) {
      try { clearTimeout(pendingTimer); } catch {}
      incomingCreateFallbackTimers.delete(id);
    }
    return true;
  } catch {
    return true;
  }
}

function scheduleIncomingFromMessageCreate(message, handler) {
  try {
    if (!message || message.fromMe === true) return;

    const id = getMessageStableId(message);
    const type = String(message?.type || message?._data?.type || '').trim().toLowerCase();
    const mimeType = String(message?._data?.mimetype || '').trim().toLowerCase();
    const isMedia = ['audio', 'ptt', 'voice', 'image', 'document', 'video', 'sticker'].includes(type) ||
      /^(audio|image|video)\//.test(mimeType) || mimeType.includes('pdf');

    const configuredDelay = Number(
      tenantConfig?.message_create_fallback_ms ||
      tenantConfig?.messageCreateFallbackMs ||
      process.env.MESSAGE_CREATE_FALLBACK_MS ||
      0
    );

    // Los PTT suelen aparecer primero por message_create sin directPath/mediaKey.
    // Damos prioridad al evento message y usamos message_create solo si no llegó.
    const delay = isMedia
      ? Math.max(6000, Number.isFinite(configuredDelay) && configuredDelay > 0 ? configuredDelay : 6000)
      : Math.max(500, Number.isFinite(configuredDelay) && configuredDelay > 0 ? configuredDelay : 1500);

    const runFallback = async () => {
      if (id) incomingCreateFallbackTimers.delete(id);
      if (id && recentIncomingProcessIds.has(id)) {
        try { console.log('[INCOMING] message_create fallback skip id=' + id); } catch {}
        return;
      }
      let candidate = message;

      // Para adjuntos intentamos rehidratar el mensaje antes de procesarlo.
      // Esto evita usar el wrapper incompleto que suele llegar primero para PTT.
      if (isMedia) {
        try {
          if (typeof candidate?.reload === 'function') {
            await promiseWithTimeout(candidate.reload(), 4000, 'message_create_reload');
          }
        } catch {}

        try {
          if (id && client && typeof client.getMessageById === 'function') {
            const hydrated = await promiseWithTimeout(
              client.getMessageById(id),
              5000,
              'message_create_getMessageById'
            );
            if (hydrated) candidate = hydrated;
          }
        } catch {}
      }

      // Mientras hidratábamos pudo haber llegado el evento message completo.
      if (id && recentIncomingProcessIds.has(id)) {
        try { console.log('[INCOMING] message_create fallback hidratado skip id=' + id); } catch {}
        return;
      }

      const fallbackLog = '[MESSAGE_CREATE_FALLBACK] procesando id=' + String(id || '') +
        ' type=' + type + ' media=' + String(isMedia);
      try { console.log(fallbackLog); } catch {}
      try { EscribirLog(fallbackLog, 'event'); } catch {}

      try {
        await handler(candidate, 'message_create_fallback');
      } catch (e) {
        const errLog = '[MESSAGE_CREATE_FALLBACK] error id=' + String(id || '') + ': ' + String(e?.message || e);
        try { console.log(errLog); } catch {}
        try { EscribirLog(errLog, 'error'); } catch {}
      }
    };

    if (!id) {
      setTimeout(runFallback, delay);
      return;
    }

    if (recentIncomingProcessIds.has(id) || incomingCreateFallbackTimers.has(id)) return;
    const timer = setTimeout(runFallback, delay);

    incomingCreateFallbackTimers.set(id, timer);
  } catch (e) {
    try { console.log('[message_create] schedule error:', e?.message || e); } catch {}
    try { EscribirLog('[message_create] schedule error: ' + String(e?.message || e), 'error'); } catch {}
  }
}

// No usar heurísticas sobre message._data/chat._data para resolver @lid.
// Esos objetos traen IDs internos de WhatsApp que parecen números, pero no son
// teléfonos reales. El API solo debe recibir teléfono obtenido de contacto real
// o de un mapeo explícito LID -> teléfono.
async function resolvePhoneFromMessageDeep(message) {
  return '';
}



// Lease/heartbeat configurables (ms)
const MIN_LEASE_MS = Number(process.env.MIN_LEASE_MS || 180000);
let lease_ms = Number(process.env.LEASE_MS || MIN_LEASE_MS);
let heartbeat_ms = Number(process.env.HEARTBEAT_MS || 5000);
let backup_every_ms = Number(process.env.BACKUP_EVERY_MS || 300000);
let auth_base_path = process.env.ASISTO_AUTH_PATH || "";            // whatsapp-web.js LocalAuth dataPath override
let baileys_auth_base_path = process.env.ASISTO_BAILEYS_AUTH_PATH || ""; // Baileys auth-state base path override
let auth_mode = String(process.env.ASISTO_AUTH_MODE || '').trim().toLowerCase(); // wwebjs: 'remote' | 'local'
let wweb_engine = normalizeWwebEngine(
  process.env.ASISTO_WWEB_ENGINE || process.env.WWEB_ENGINE || process.env.WHATSAPP_WEB_ENGINE || 'wwebjs'
); // 'wwebjs' | 'baileys' (default: wwebjs)

// =========================
// Auto-update desde repositorio (opcional, NO rompe comportamiento actual)
// Requiere que la carpeta local sea un checkout git y que exista 'git' en la PC.
// Por seguridad, viene DESACTIVADO por defecto y solo se habilita por config/env.
// =========================
let auto_update_enabled = String(process.env.AUTO_UPDATE_ENABLED || '').trim().toLowerCase() === 'true';
let auto_update_repo_path = String(process.env.AUTO_UPDATE_REPO_PATH || __dirname).trim() || __dirname;
let auto_update_remote = String(process.env.AUTO_UPDATE_REMOTE || 'origin').trim() || 'origin';
let auto_update_branch = String(process.env.AUTO_UPDATE_BRANCH || '').trim();
let auto_update_source = String(process.env.AUTO_UPDATE_SOURCE || 'tag_or_branch').trim().toLowerCase() || 'tag_or_branch'; // tag | branch | tag_or_branch
let auto_update_target_tag = String(process.env.AUTO_UPDATE_TARGET_TAG || '').trim();
let auto_update_check_every_ms = Number(process.env.AUTO_UPDATE_CHECK_EVERY_MS || 10 * 60_000);
let auto_update_startup_delay_ms = Number(process.env.AUTO_UPDATE_STARTUP_DELAY_MS || 120_000);
let auto_update_restart_on_apply = String(process.env.AUTO_UPDATE_RESTART_ON_APPLY || 'true').trim().toLowerCase() !== 'false';
let auto_update_require_clean = String(process.env.AUTO_UPDATE_REQUIRE_CLEAN || 'true').trim().toLowerCase() !== 'false';
let auto_update_run_npm_install = String(process.env.AUTO_UPDATE_RUN_NPM_INSTALL || 'true').trim().toLowerCase() !== 'false';
let auto_update_post_update_cmd = String(process.env.AUTO_UPDATE_POST_UPDATE_CMD || '').trim();
let autoUpdateTimer = null;
let autoUpdateRunning = false;
let autoUpdateRestarting = false;
let fastSupervisorExitInFlight = false;

function getSupervisorRestartExitCode() {
  const raw = process.env.ASISTO_RESTART_EXIT_CODE || process.env.RESTART_EXIT_CODE || '77';
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 && n <= 255 ? Math.trunc(n) : 77;
}

function clearRuntimeTimersForExit(reason = '') {
  try { if (autoUpdateTimer) { clearInterval(autoUpdateTimer); autoUpdateTimer = null; } } catch {}
  try { if (runtimeConfigPollTimer) { clearInterval(runtimeConfigPollTimer); runtimeConfigPollTimer = null; } } catch {}
  try { if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; } } catch {}
  try { if (actionTimer) { clearInterval(actionTimer); actionTimer = null; } } catch {}
  try { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } } catch {}
  try { compraEntregaQueryStopRequested = true; } catch {}
  try { if (compraEntregaConnection && typeof compraEntregaConnection.close === 'function')  compraEntregaConnection.close(); } catch {}
  try { compraEntregaConnection = null; } catch {}
  try { clearAuthReadyWatchdog(String(reason || 'supervisor_exit')); } catch {}
}

function timeoutPromise(ms, label = 'timeout') {
  return new Promise((resolve) => setTimeout(() => resolve(label), Math.max(0, Number(ms) || 0)));
}

async function fastExitForSupervisorRestart(reason = 'SUPERVISOR_RESTART', exitCode = getSupervisorRestartExitCode()) {
  if (fastSupervisorExitInFlight) return;
  fastSupervisorExitInFlight = true;

  const code = Number.isFinite(Number(exitCode)) ? Math.trunc(Number(exitCode)) : getSupervisorRestartExitCode();
  const msg = `[PROCESS_EXIT] ${String(reason || 'SUPERVISOR_RESTART')} -> salida rapida para reinicio por supervisor exitCode=${code}`;
  try { console.log(msg); } catch {}
  try { EscribirLog(msg, 'event'); } catch {}

  clearRuntimeTimersForExit(reason);

  // Cierre acotado del transporte antes de salir. Con whatsapp-web.js intentamos
  // cerrar Puppeteer/Chromium y, si no responde dentro del timeout, terminamos
  // SOLO el árbol del Chromium asociado a este Client (no todos los chrome.exe).
  try { await closeWhatsappClientForProcessExit(client, 'fast_exit:' + String(reason || ''), 1800); } catch {}
  try { client = null; } catch {}
  try { resetClientRuntimeFlags('fast_exit:' + String(reason || '')); } catch {}
  try { localWsPanelState = 'offline'; } catch {}
  try { await Promise.race([updateLockStateSafe('offline'), timeoutPromise(1200, 'update_lock_timeout')]); } catch {}
  try { await Promise.race([forceReleaseLock('offline'), timeoutPromise(1800, 'release_lock_timeout')]); } catch {}
  try { isOwner = false; } catch {}

  try { await Promise.race([disconnectMongoSafe(String(reason || 'supervisor_restart')), timeoutPromise(3000, 'mongo_disconnect_timeout')]); } catch {}
  try { releaseSingleInstanceLock(); } catch {}
 
  try { process.exitCode = code; } catch {}
  setTimeout(() => { try { process.exit(code); } catch {} }, 100);
  setTimeout(() => { try { process.exit(code); } catch {} }, 1500);
}
// opcional para proteger /status
function normalizeWwebEngine(value) {
  const v = String(value || 'wwebjs').trim().toLowerCase();
  if (['baileys', 'bailey', 'socket', 'websocket', 'ws'].includes(v)) return 'baileys';
  return 'wwebjs';
}

function getWwebEngine() {
  return normalizeWwebEngine(wweb_engine);
}

function isBaileysEngine() {
  return getWwebEngine() === 'baileys';
}

function isWwebJsEngine() {
  return getWwebEngine() === 'wwebjs';
}

function isRemoteAuthMode() {
  // RemoteAuth pertenece a whatsapp-web.js. Baileys utiliza su propio auth-state local.
  if (!isWwebJsEngine()) return false;
  const mode = String(auth_mode || 'local').trim().toLowerCase();
  return mode && mode !== 'local';
}

function isLocalAuthMode() {
  return !isRemoteAuthMode();
}

function parseBoolLike(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'boolean') return value;
  const v = String(value).trim().toLowerCase();
  if (!v) return fallback;
  if (["1", "true", "yes", "si", "sí", "on"].includes(v)) return true;
  if (["0", "false", "no", "off"].includes(v)) return false;
  return fallback;
}

function normalizeWwebBotLogicMode(value) {
  const v = String(value || 'api').trim().toLowerCase();
  if (['chatgpt', 'gpt', 'pedido', 'pedidos', 'asisto', 'ia', 'openai'].includes(v)) return 'chatgpt';
  return 'api';
}

function normalizeWhatsappTransportLocal(value) {
  const v = String(value || 'api').trim().toLowerCase();
  if (['wweb', 'whatsapp_web', 'whatsappweb', 'web'].includes(v)) return 'wweb';
  return 'api';
}

function phonesLookSame(a, b) {
  const da = onlyDigits(a);
  const db = onlyDigits(b);
  if (!da || !db) return false;
  if (da === db) return true;
  if (da.startsWith('549') && db.startsWith('54') && !db.startsWith('549')) return da === ('549' + db.slice(2));
  if (db.startsWith('549') && da.startsWith('54') && !da.startsWith('549')) return db === ('549' + da.slice(2));
  return false;
}

const ASISTO_WWEB_CHATGPT_PROCESS_URL = 'https://asistobot.com.ar/api/ext/wweb/chatgpt/process';
 

function getAsistoWwebChatgptProcessUrl() {
  return ASISTO_WWEB_CHATGPT_PROCESS_URL;
}

let wwebBotLogicModeCache = { at: 0, numero: '', value: '' };

async function getWwebBotLogicModeForPhone(phoneNumber) {
 const fallback = normalizeWwebBotLogicMode(wweb_bot_logic_mode);
  const phone = onlyDigits(phoneNumber || numero || '');
  const now = Date.now();

  if (wwebBotLogicModeCache.value && wwebBotLogicModeCache.numero === phone && (now - wwebBotLogicModeCache.at) < 30000) {
    return wwebBotLogicModeCache.value;
 }

  try {
    if (!tenantId || !phone) return fallback;
    if (!await ensureMongo()) return fallback;
    if (!dataBackendReady()) return fallback;
    const rows = await getDataCollection('tenant_channels')
      .find({ tenantId: String(tenantId || '').trim(), channelType: 'whatsapp' })
      .sort({ isDefault: -1, updatedAt: -1, createdAt: -1 })
      .limit(200)
      .toArray();
    const row = (rows || []).find((it) => {
      if (normalizeWhatsappTransportLocal(it?.whatsappTransport ?? it?.whatsapp_transport ?? it?.transport ?? 'api') !== 'wweb') return false;
      return phonesLookSame(it?.displayPhoneNumber, phone) || phonesLookSame(it?.phoneNumberId, phone);
    });

    const mode = row
      ? normalizeWwebBotLogicMode(row.wwebBotLogicMode ?? row.wweb_bot_logic_mode ?? row.botLogicMode ?? row.bot_logic_mode ?? fallback)
      : fallback;

    wwebBotLogicModeCache = { at: now, numero: phone, value: mode };
    return mode;
  } catch (e) {
    try { EscribirLog('getWwebBotLogicModeForPhone error: ' + String(e?.message || e), 'error'); } catch {}
    return fallback;
  }
}

function getIncomingApiUrlForLogicMode(mode) {
  const m = normalizeWwebBotLogicMode(mode);
  if (m === 'chatgpt') return getAsistoWwebChatgptProcessUrl();
  return api;
}


function normalizeAutoUpdateConfig(conf) {
  if (!conf || typeof conf !== 'object') return {};
  const nested = (conf.auto_update && typeof conf.auto_update === 'object') ? conf.auto_update : (conf.autoUpdate && typeof conf.autoUpdate === 'object' ? conf.autoUpdate : null);
  return { ...conf, ...(nested || {}) };
}

function applyAutoUpdateConfig(conf) {
  const au = normalizeAutoUpdateConfig(conf);
  if (!au || typeof au !== 'object') return;

  if (au.auto_update_enabled !== undefined || au.enabled !== undefined) {
    auto_update_enabled = parseBoolLike(au.enabled !== undefined ? au.enabled : au.auto_update_enabled, auto_update_enabled);
  }
  if (au.auto_update_repo_path !== undefined || au.repo_path !== undefined || au.path !== undefined) {
    const v = String(au.repo_path || au.path || au.auto_update_repo_path || '').trim();
    if (v) auto_update_repo_path = path.isAbsolute(v) ? v : path.resolve(__dirname, v);
  }
  if (au.auto_update_remote !== undefined || au.remote !== undefined) {
    const v = String(au.remote || au.auto_update_remote || '').trim();
    if (v) auto_update_remote = v;
  }
  if (au.auto_update_branch !== undefined || au.branch !== undefined) {
    const v = String(au.branch || au.auto_update_branch || '').trim();
    if (v) auto_update_branch = v;
  }
  if (au.auto_update_target_tag !== undefined || au.target_tag !== undefined || au.desired_tag !== undefined || au.release_tag !== undefined || au.version_tag !== undefined) {
    const v = String(
      au.target_tag ??
      au.desired_tag ??
      au.release_tag ??
      au.version_tag ??
      au.auto_update_target_tag ??
      ''
    ).trim();
    auto_update_target_tag = v;
  }
  if (au.auto_update_source !== undefined || au.source !== undefined || au.mode !== undefined) {
    const v = String(au.source || au.mode || au.auto_update_source || '').trim().toLowerCase();
    if (v) auto_update_source = v;
  }
  if (au.auto_update_check_every_ms !== undefined || au.check_every_ms !== undefined) {
    const n = Number(au.check_every_ms !== undefined ? au.check_every_ms : au.auto_update_check_every_ms);
    if (!Number.isNaN(n) && n > 0) auto_update_check_every_ms = n;
  }
  if (au.auto_update_startup_delay_ms !== undefined || au.startup_delay_ms !== undefined) {
    const n = Number(au.startup_delay_ms !== undefined ? au.startup_delay_ms : au.auto_update_startup_delay_ms);
    if (!Number.isNaN(n) && n >= 0) auto_update_startup_delay_ms = n;
  }
  if (au.auto_update_restart_on_apply !== undefined || au.restart_on_apply !== undefined) {
    auto_update_restart_on_apply = parseBoolLike(au.restart_on_apply !== undefined ? au.restart_on_apply : au.auto_update_restart_on_apply, auto_update_restart_on_apply);
  }
  if (au.auto_update_require_clean !== undefined || au.require_clean !== undefined) {
    auto_update_require_clean = parseBoolLike(au.require_clean !== undefined ? au.require_clean : au.auto_update_require_clean, auto_update_require_clean);
  }
  if (au.auto_update_run_npm_install !== undefined || au.run_npm_install !== undefined) {
    auto_update_run_npm_install = parseBoolLike(au.run_npm_install !== undefined ? au.run_npm_install : au.auto_update_run_npm_install, auto_update_run_npm_install);
  }
  if (au.auto_update_post_update_cmd !== undefined || au.post_update_cmd !== undefined) {
    auto_update_post_update_cmd = String(au.post_update_cmd || au.auto_update_post_update_cmd || '').trim();
  }

  if (!Number.isFinite(auto_update_check_every_ms) || auto_update_check_every_ms < 60_000) auto_update_check_every_ms = 60_000;
  if (!Number.isFinite(auto_update_startup_delay_ms) || auto_update_startup_delay_ms < 0) auto_update_startup_delay_ms = 0;
  auto_update_repo_path = auto_update_repo_path || __dirname;
  auto_update_remote = auto_update_remote || 'origin';
  if (!['tag', 'branch', 'tag_or_branch'].includes(auto_update_source)) auto_update_source = 'tag_or_branch';

  // Un solo worker puede tocar git/package.json. Los secundarios comparten el mismo
  // código y node_modules, por lo que ejecutar auto-update en paralelo sería riesgoso.
  if (ASISTO_MULTI_WORKER && !ASISTO_MULTI_PRIMARY_WORKER) {
    auto_update_enabled = false;
  }
}

function getConfiguredTargetTag(conf) {
  try {
    if (!conf || typeof conf !== 'object') return '';
    const au = normalizeAutoUpdateConfig(conf);
    const v = au.target_tag ?? au.desired_tag ?? au.release_tag ?? au.version_tag ?? au.auto_update_target_tag ?? '';
    return String(v || '').trim();
  } catch {
    return '';
  }
}

function getRuntimeScriptVersion() {
  try {
    const head = fs.readFileSync(__filename, 'utf8').slice(0, 512);
    const m = head.match(/\/\*version:([^\n*]+)/i);
    return m ? String(m[1] || '').trim() : '';
  } catch {
    return '';
  }
}

function getCurrentRuntimeInfo() {
  const currentVersion = getRuntimeScriptVersion();
  const desiredTag = String(auto_update_target_tag || '').trim();
  return {
    currentVersion,
    desiredTag,
    autoUpdateSource: String(auto_update_source || ''),
    autoUpdateEnabled: !!auto_update_enabled
  };
}

function autoUpdateLog(msg, type = 'event') {
  try { console.log(msg); } catch {}
  try { EscribirLog(msg, type); } catch {}
}

function resolveCmdBin(name) {
  if (process.platform === 'win32') {
    if (name === 'npm') return 'npm.cmd';
    if (name === 'npx') return 'npx.cmd';
  }
  return name;
}

// En Windows, npm/npx son wrappers .cmd. Con Node recientes, intentar ejecutar
// npm.cmd directamente mediante spawn(..., { shell:false }) puede fallar con
// EINVAL. Preferimos ejecutar el CLI de npm con el mismo node.exe; si no está
// en la instalación estándar, hacemos fallback a cmd.exe /c npm.
function resolveCommandInvocation(bin, args = [], opts = {}) {
  const rawBin = String(bin || '').trim();
  const argv = Array.isArray(args) ? args.map((v) => String(v)) : [];
  const useShell = !!opts.shell;

  if (process.platform === 'win32' && !useShell) {
    const lower = rawBin.toLowerCase();
    const isNpm = lower === 'npm' || lower === 'npm.cmd';
    const isNpx = lower === 'npx' || lower === 'npx.cmd';

    if (isNpm || isNpx) {
      const cliName = isNpx ? 'npx-cli.js' : 'npm-cli.js';
      const cliPath = path.join(path.dirname(process.execPath), 'node_modules', 'npm', 'bin', cliName);

      if (fs.existsSync(cliPath)) {
        return {
          bin: process.execPath,
          args: [cliPath, ...argv],
          shell: false,
          description: `${process.execPath} ${cliPath}`
        };
      }

      const comspec = process.env.ComSpec || process.env.COMSPEC || 'cmd.exe';
      const tool = isNpx ? 'npx' : 'npm';
      return {
        bin: comspec,
        args: ['/d', '/s', '/c', tool, ...argv],
        shell: false,
       description: `${comspec} /d /s /c ${tool}`
      };
    }
  }

  return {
    bin: resolveCmdBin(rawBin),
    args: argv,
    shell: useShell,
    description: rawBin
  };
}


function runCommand(bin, args = [], opts = {}) {
  return new Promise((resolve, reject) => {
    const invocation = resolveCommandInvocation(bin, args, opts);
    let child;

    try {
      child = spawn(invocation.bin, invocation.args, {
        cwd: opts.cwd || process.cwd(),
        shell: invocation.shell,
        env: { ...process.env, ...(opts.env || {}) },
        windowsHide: true,
        stdio: ['ignore', 'pipe', 'pipe']
      });
    } catch (spawnError) {
      try {
        spawnError.command = invocation.description;
      } catch {}
      reject(spawnError);
      return;
    }

    let stdout = '';
    let stderr = '';
    let finished = false;
    let timeoutId = null;

    if (opts.timeout && Number(opts.timeout) > 0) {
      timeoutId = setTimeout(() => {
        if (finished) return;
        finished = true;
        try { child.kill('SIGTERM'); } catch {}
        reject(new Error(`${bin}_timeout`));
      }, Number(opts.timeout));
    }

    child.stdout && child.stdout.on('data', (d) => { stdout += d.toString(); });
    child.stderr && child.stderr.on('data', (d) => { stderr += d.toString(); });
    child.on('error', (err) => {
      if (finished) return;
      finished = true;
      if (timeoutId) clearTimeout(timeoutId);
      try { err.command = invocation.description; } catch {}
      reject(err);
    });
    child.on('close', (code) => {
      if (finished) return;
      finished = true;
      if (timeoutId) clearTimeout(timeoutId);
      if (code === 0) return resolve({ code, stdout, stderr });
      const err = new Error(`${bin} exited with code ${code}`);
      err.code = code;
      err.stdout = stdout;
      err.stderr = stderr;
      reject(err);
    });
  });
}

async function autoUpdateGetBranch(repoPath) {
  if (auto_update_branch) return auto_update_branch;
  const out = await runCommand('git', ['rev-parse', '--abbrev-ref', 'HEAD'], { cwd: repoPath, timeout: 20_000 });
  return String(out.stdout || '').trim() || 'main';
}

function normalizeTagSortValue(tag) {
  const clean = String(tag || '').trim().replace(/^refs\/tags\//i, '').replace(/^v/i, '');
  return clean.split('.').map((p) => {
    const n = Number(String(p).replace(/[^0-9].*$/, ''));
    return Number.isFinite(n) ? n : -1;
  });
}

function compareSemverLikeTagsDesc(a, b) {
  const pa = normalizeTagSortValue(a);
  const pb = normalizeTagSortValue(b);
  const len = Math.max(pa.length, pb.length, 3);
  for (let i = 0; i < len; i++) {
    const av = i < pa.length ? pa[i] : 0;
    const bv = i < pb.length ? pb[i] : 0;
    if (av !== bv) return bv - av;
  }
  return String(b || '').localeCompare(String(a || ''), 'en', { sensitivity: 'base' });
}

async function autoUpdateGetLatestTag(repoPath, remote) {
  await runCommand('git', ['fetch', remote, '--tags', '--force', '--prune'], { cwd: repoPath, timeout: 120_000 });

  let tags = [];
  try {
    const tagOut = await runCommand('git', ['tag', '--list'], { cwd: repoPath, timeout: 20_000 });
    tags = String(tagOut.stdout || '').split(/\r?\n/).map(s => s.trim()).filter(Boolean);
  } catch {}

  const semverLike = tags.filter((t) => /^v?\d+(?:\.\d+){1,}$/.test(String(t || '').trim()));
  if (semverLike.length) {
    semverLike.sort(compareSemverLikeTagsDesc);
    return semverLike[0];
  }

  try {
    const out = await runCommand('git', ['for-each-ref', '--sort=-creatordate', '--format=%(refname:short)', 'refs/tags'], { cwd: repoPath, timeout: 20_000 });
    const byDate = String(out.stdout || '').split(/\r?\n/).map(s => s.trim()).filter(Boolean);
    if (byDate.length) return byDate[0];
  } catch {}

  return '';
}

async function autoUpdateResolveTarget(repoPath) {
  const remote = auto_update_remote || 'origin';
  const source = String(auto_update_source || 'tag_or_branch').trim().toLowerCase();
  const desiredTag = String(auto_update_target_tag || '').trim();

  if (source !== 'branch') {
    let selectedTag = desiredTag;
    if (!selectedTag) {
      selectedTag = await autoUpdateGetLatestTag(repoPath, remote);
    } else {
      await runCommand('git', ['fetch', remote, '--tags', '--force', '--prune'], { cwd: repoPath, timeout: 120_000 });
    }

    if (selectedTag) {
      const tagRef = `refs/tags/${selectedTag}`;
      const headOut = await runCommand('git', ['rev-list', '-n', '1', tagRef], { cwd: repoPath, timeout: 15_000 });
      const tagHead = String(headOut.stdout || '').trim();
      if (tagHead) {
        return {
          source: desiredTag ? 'target_tag' : 'tag',
          ref: selectedTag,
          head: tagHead,
          desiredTag: selectedTag
        };
      }
    }
    if (desiredTag) {
      throw new Error(`git_target_tag_not_found:${desiredTag}`);
    }
    if (source === 'tag') {
      throw new Error('git_latest_tag_not_found');
    }
  }

  const branch = await autoUpdateGetBranch(repoPath);
  await runCommand('git', ['fetch', remote, branch, '--prune'], { cwd: repoPath, timeout: 120_000 });
  const remoteRef = `${remote}/${branch}`;
  const remoteHeadOut = await runCommand('git', ['rev-parse', remoteRef], { cwd: repoPath, timeout: 15_000 });
  const remoteHead = String(remoteHeadOut.stdout || '').trim();
  if (!remoteHead) throw new Error('git_remote_head_empty');

  return {
    source: 'branch',
    ref: remoteRef,
    head: remoteHead,
    branch
  };
}

async function autoUpdateForceTargetTagOnBoot(reason = 'boot_target_tag_force') {
  // Todos los workers comparten el MISMO C:\Asisto, repo y node_modules.
  // Sólo el primario puede modificar Git/package.json, incluso cuando esta función
  // es invocada indirectamente desde before_whatsapp_start.
  if (ASISTO_MULTI_WORKER && !ASISTO_MULTI_PRIMARY_WORKER) {
    autoUpdateLog(`[AUTO_UPDATE] skip (${reason}): worker secundario multi-sesión no puede modificar repo compartido`, 'event');
    return false;
  }

  const desiredTag = String(auto_update_target_tag || '').trim();
  if (!desiredTag) {
    autoUpdateLog(`[AUTO_UPDATE] skip (${reason}): tenant sin targetTag configurado`, 'event');
    return false;
  }

  const repoPath = path.resolve(auto_update_repo_path || process.cwd());
  if (!fs.existsSync(repoPath)) {
    autoUpdateLog(`[AUTO_UPDATE] skip (${reason}): repo_path inexistente -> ${repoPath}`, 'error');
    return false;
  }
  if (!fs.existsSync(path.join(repoPath, '.git'))) {
    autoUpdateLog(`[AUTO_UPDATE] skip (${reason}): ${repoPath} no es un repositorio git`, 'event');
    return false;
  }

  await runCommand('git', ['rev-parse', '--is-inside-work-tree'], { cwd: repoPath, timeout: 15_000 });

  const localHeadOut = await runCommand('git', ['rev-parse', 'HEAD'], { cwd: repoPath, timeout: 15_000 });
  const localHead = String(localHeadOut.stdout || '').trim();
  if (!localHead) throw new Error('git_local_head_empty');

  // Refresca tags aunque la misma tag haya sido movida o recreada en remoto.
  await runCommand('git', ['fetch', auto_update_remote || 'origin', '--tags', '--force', '--prune'], { cwd: repoPath, timeout: 120_000 });

  const tagRef = `refs/tags/${desiredTag}`;
  const targetHeadOut = await runCommand('git', ['rev-list', '-n', '1', tagRef], { cwd: repoPath, timeout: 15_000 });
  const targetHead = String(targetHeadOut.stdout || '').trim();
  if (!targetHead) throw new Error(`git_target_tag_not_found:${desiredTag}`);

  if (targetHead === localHead) {
    autoUpdateLog(`[AUTO_UPDATE] ok (${reason}): sin cambios (${localHead.slice(0, 7)}) target=target_tag:${desiredTag}`, 'event');
    return false;
  }

  autoUpdateLog(`[AUTO_UPDATE] FORCE update (${reason}): ${localHead.slice(0, 7)} -> ${targetHead.slice(0, 7)} target=target_tag:${desiredTag}`, 'event');

  const changedOut = await runCommand('git', ['diff', '--name-only', `${localHead}..${targetHead}`], { cwd: repoPath, timeout: 30_000 });
  const changedFiles = String(changedOut.stdout || '').split(/\r?\n/).map(s => s.trim()).filter(Boolean);

  // En arranque forzado ignoramos working tree local: reemplazamos sí o sí.
  await runCommand('git', ['reset', '--hard', targetHead], { cwd: repoPath, timeout: 120_000 });
  await runCommand('git', ['clean', '-fd'], { cwd: repoPath, timeout: 120_000 });

  if (auto_update_run_npm_install) {
    const needsNpm = changedFiles.some((name) => /(^|\/)(package\.json|package-lock\.json)$/i.test(name));
    if (needsNpm) {
      autoUpdateLog('[AUTO_UPDATE] package*.json cambió, ejecutando npm install --omit=dev', 'event');
      +      await runCommand('npm', ['install', '--omit=dev'], { cwd: repoPath, timeout: 10 * 60_000 });

    }
  }

  if (auto_update_post_update_cmd) {
    autoUpdateLog(`[AUTO_UPDATE] ejecutando post_update_cmd: ${auto_update_post_update_cmd}`, 'event');
    if (process.platform === 'win32') {
      await runCommand('cmd', ['/c', auto_update_post_update_cmd], { cwd: repoPath, timeout: 10 * 60_000, shell: false });
    } else {
      await runCommand('sh', ['-lc', auto_update_post_update_cmd], { cwd: repoPath, timeout: 10 * 60_000, shell: false });
    }
  }

  autoUpdateLog(`[AUTO_UPDATE] cambios forzados aplicados en ${repoPath}`, 'event');

  if (auto_update_restart_on_apply) {
    autoUpdateRestarting = true;
    autoUpdateLog('[AUTO_UPDATE] reiniciando proceso para aplicar actualización forzada...', 'event');
    setTimeout(() => { fastExitForSupervisorRestart('AUTO_UPDATE_FORCE_BOOT'); }, 1200);
  }
  return true;
}
 

async function autoUpdateCheckAndApply(reason = 'interval') {
  if (!auto_update_enabled || autoUpdateRunning || autoUpdateRestarting) return;
  autoUpdateRunning = true;

  try {
    const repoPath = path.resolve(auto_update_repo_path || process.cwd());
    if (!fs.existsSync(repoPath)) {
      autoUpdateLog(`[AUTO_UPDATE] skip (${reason}): repo_path inexistente -> ${repoPath}`, 'error');
      return;
    }
    if (!fs.existsSync(path.join(repoPath, '.git'))) {
      autoUpdateLog(`[AUTO_UPDATE] skip (${reason}): ${repoPath} no es un repositorio git`, 'event');
      return;
    }

    await runCommand('git', ['rev-parse', '--is-inside-work-tree'], { cwd: repoPath, timeout: 15_000 });


    if (auto_update_require_clean) {
      const statusOut = await runCommand('git', ['status', '--porcelain'], { cwd: repoPath, timeout: 20_000 });
      if (String(statusOut.stdout || '').trim()) {
        autoUpdateLog(`[AUTO_UPDATE] skip (${reason}): working tree con cambios locales`, 'event');
        return;
      }
    }

    const headOut = await runCommand('git', ['rev-parse', 'HEAD'], { cwd: repoPath, timeout: 15_000 });
    const localHead = String(headOut.stdout || '').trim();
    if (!localHead) throw new Error('git_local_head_empty');

    const target = await autoUpdateResolveTarget(repoPath);
    const targetHead = String(target?.head || '').trim();
    if (!targetHead) throw new Error('git_target_head_empty');

    if (targetHead === localHead) {
      autoUpdateLog(`[AUTO_UPDATE] ok (${reason}): sin cambios (${localHead.slice(0, 7)}) target=${target.source}:${target.ref}`, 'event');
      return;
    }

    autoUpdateLog(`[AUTO_UPDATE] update (${reason}): ${localHead.slice(0, 7)} -> ${targetHead.slice(0, 7)} target=${target.source}:${target.ref}`, 'event');

    const changedOut = await runCommand('git', ['diff', '--name-only', `${localHead}..${targetHead}`], { cwd: repoPath, timeout: 30_000 });

    const changedFiles = String(changedOut.stdout || '').split(/\r?\n/).map(s => s.trim()).filter(Boolean);

    await runCommand('git', ['reset', '--hard', targetHead], { cwd: repoPath, timeout: 120_000 });

    if (auto_update_run_npm_install) {
      const needsNpm = changedFiles.some((name) => /(^|\/)(package\.json|package-lock\.json)$/i.test(name));
      if (needsNpm) {
        autoUpdateLog('[AUTO_UPDATE] package*.json cambió, ejecutando npm install --omit=dev', 'event');
        await runCommand('npm', ['install', '--omit=dev'], { cwd: repoPath, timeout: 10 * 60_000 });
      }
    }

    if (auto_update_post_update_cmd) {
      autoUpdateLog(`[AUTO_UPDATE] ejecutando post_update_cmd: ${auto_update_post_update_cmd}`, 'event');
      if (process.platform === 'win32') {
        await runCommand('cmd', ['/c', auto_update_post_update_cmd], { cwd: repoPath, timeout: 10 * 60_000, shell: false });
      } else {
        await runCommand('sh', ['-lc', auto_update_post_update_cmd], { cwd: repoPath, timeout: 10 * 60_000, shell: false });
      }
    }

    autoUpdateLog(`[AUTO_UPDATE] cambios aplicados en ${repoPath}`, 'event');

    if (auto_update_restart_on_apply) {
      autoUpdateRestarting = true;
      autoUpdateLog('[AUTO_UPDATE] reiniciando proceso para aplicar actualización...', 'event');
      setTimeout(() => { fastExitForSupervisorRestart('AUTO_UPDATE'); }, 1200);
    }
  } catch (e) {
    autoUpdateLog(`[AUTO_UPDATE] error (${reason}): ${e?.message || e}`, 'error');
  } finally {
    autoUpdateRunning = false;
  }
}

function startAutoUpdateScheduler() {
  // En multi-sesión no modificamos Git/node_modules con workers activos.
  // El worker primario ya hace la verificación forzada antes de habilitar al resto.
  if (ASISTO_MULTI_WORKER) {
    autoUpdateLog('[AUTO_UPDATE] scheduler periódico omitido en multi-sesión; update sólo al arranque del primario', 'event');
    return;
  }

  if (!auto_update_enabled) {
    autoUpdateLog('[AUTO_UPDATE] desactivado', 'event');
    return;
  }
  if (autoUpdateTimer) return;

  const repoPath = path.resolve(auto_update_repo_path || process.cwd());
  autoUpdateLog(`[AUTO_UPDATE] activado repo=${repoPath} remote=${auto_update_remote} source=${auto_update_source} targetTag=${auto_update_target_tag || '(auto)'} branch=${auto_update_branch || '(auto)'} every=${auto_update_check_every_ms}ms startupDelay=${auto_update_startup_delay_ms}ms`, 'event');

  setTimeout(async () => {
    try { await loadTenantConfigFromDbMinimal(); } catch (e) {
      try { autoUpdateLog(`[AUTO_UPDATE] refresh config startup error: ${e?.message || e}`, 'error'); } catch {}
    }
    autoUpdateCheckAndApply('startup').catch(() => {});
  }, Math.max(0, Number(auto_update_startup_delay_ms) || 0));

  autoUpdateTimer = setInterval(async () => {
    try { await loadTenantConfigFromDbMinimal(); } catch (e) {
      try { autoUpdateLog(`[AUTO_UPDATE] refresh config interval error: ${e?.message || e}`, 'error'); } catch {}
    }
    autoUpdateCheckAndApply('interval').catch(() => {});
  }, Math.max(60_000, Number(auto_update_check_every_ms) || 600_000));
}


const instanceId = process.env.INSTANCE_ID || `${os.hostname()}-${process.pid}-${crypto.randomBytes(4).toString("hex")}`;
let lockId = "";                 // `${tenantId}:${numero}`
let isOwner = false;             // true si esta PC tiene el lock
let clientStarted = false;       // true si initialize() fue llamado
let startingNow = false;       // evita inicializaciones concurrentes (doble Chrome/userDataDir)
let lastQrRaw = null;
let lastQrDataUrl = null;
let lastQrAt = null;
let localWsPanelState = 'idle';
// Cache liviano: si la política marca disabled=true, no inicializamos WhatsApp.
let lastPolicyDisabled = null;
let lastPolicyBlocked = false;
let mongoReady = false;
let LockModel = null;
let ActionModel = null;
let PolicyModel = null;      // wa_wweb_policies
let HistoryModel = null;     // wa_wweb_history
let MessageLogModel = null;  // wa_wweb_message_log
let ApiMessageWindowModel = null; // wa_api_message_windows
let heartbeatTimer = null;
let actionTimer = null;
let pollTimer = null;
let actionBusy = false;
let heartbeatBusy = false;
let restartInFlight = false;
let fullProcessRestartInFlight = false;
let authReadyWatchdogTimer = null;
let authReadyWatchdogSeq = 0;
const AUTH_READY_WATCHDOG_MS = Math.max(30000, Number(process.env.AUTH_READY_WATCHDOG_MS || 90000));
var a = 0;
var port = Number(process.env.PORT || 8002);
var headless = true;
var seg_desde = 80000;
var seg_hasta = 10000;
// ConsultaApiMensajes usa milisegundos:
// - seg_desde/seg_hasta: pausa entre mensajes al MISMO número.
// - seg_desde2/seg_hasta2: pausa entre mensajes a DISTINTO número.
var seg_desde2 = Number(process.env.SEG_DESDE2 || process.env.SEG_DESDE_DIFERENTE || seg_desde);
var seg_hasta2 = Number(process.env.SEG_HASTA2 || process.env.SEG_HASTA_DIFERENTE || seg_hasta);
if (!Number.isFinite(seg_desde2) || seg_desde2 < 0) seg_desde2 = seg_desde;
if (!Number.isFinite(seg_hasta2) || seg_hasta2 < 0) seg_hasta2 = seg_hasta;
var seg_msg = 5000;
var seg_tele = 3000;
var version = "1.0";
var script = "__";
var telefono_qr = "0";
var telefono_local = "0";
var tel_array = [];
var ver_whatsapp = "0";
var dsn = "msm_manager";
var api = "http://managermsm.ddns.net:2002/v200/api/Api_Chat_Cab/ProcesarMensajePost";
// Modo fallback para mensajes entrantes por WhatsApp Web.
// La prioridad real se resuelve por número en tenant_channels.
// Si el canal no trae configuración, queda como antes: usa el API configurado.
var wweb_bot_logic_mode = "api";
// API de consulta/envío de mensajes salientes. Por defecto queda deshabilitada
// hasta activarla en tenant_config o por variables de entorno.
var api2 = String(process.env.API_MENSAJES_CONSULTA || process.env.API2 || "http://managermsm.ddns.net:2002/v200/api/Api_Mensajes/Consulta_no_enviados");
var api3 = String(process.env.API_MENSAJES_ACTUALIZA || process.env.API3 || "http://managermsm.ddns.net:2002/v200/api/Api_Mensajes/Actualiza_mensaje_destinatario");
var key = String(process.env.API_MENSAJES_KEY || process.env.API_KEY || process.env.KEY || 'FMM0325*');

// API Alta de mensajes salientes.
// Se configura por tenant_config o variables de entorno. Los flags quedan en false
// por defecto para que, si Mongo no trae esos campos, no cambie el flujo actual.
var api_mensajes_alta = String(
  process.env.API_MENSAJES_ALTA ||
  process.env.API_ALTA_MENSAJES ||
  "https://managersistemas.ddns.net:4800/v200/api/Api_Mensajes/Alta"
);
var api_mensajes_alta_key = String(process.env.API_MENSAJES_ALTA_KEY || process.env.API_ALTA_MENSAJES_KEY || "1234");
var api_mensajes_alta_nro_tel_from = String(process.env.API_MENSAJES_ALTA_NRO_TEL_FROM || process.env.API_ALTA_MENSAJES_NRO_TEL_FROM || "");

var compra_mensajes_usar_api_alta = parseBoolLike(
  process.env.COMPRA_MENSAJES_USAR_API_ALTA ?? process.env.USAR_API_ALTA_COMPRA,
  true
);
var entrega_mensajes_usar_api_alta = parseBoolLike(
  process.env.ENTREGA_MENSAJES_USAR_API_ALTA ?? process.env.USAR_API_ALTA_ENTREGA,
  true
);

function normalizarNroTelFromApiMensajes(value) {
  const d = onlyDigits(value || '');
  if (!d) return '';
  if (d.startsWith('549')) return d;
  // Argentina: el usuario API está registrado con 549 + característica + número.
  // Si WhatsApp/client.info entrega 54 sin el 9, o solo el número local, lo corregimos.
  if (d.startsWith('54') && !d.startsWith('549') && d.length >= 12) return '549' + d.slice(2);
  if (d.length === 10 && d.startsWith('3')) return '549' + d;
  return d;
}

function getApiMensajesNroTelFrom() {
  const candidatos = [
    api_mensajes_alta_nro_tel_from,
    tenantConfig?.api_mensajes_nro_tel_from,
   tenantConfig?.apiMensajesNroTelFrom,
    tenantConfig?.api_mensajes_alta_nro_tel_from,
    tenantConfig?.apiMensajesAltaNroTelFrom,
    tenantConfig?.nro_tel_from,
    tenantConfig?.nroTelFrom,
    process.env.API_MENSAJES_NRO_TEL_FROM,
    process.env.API_MENSAJES_ALTA_NRO_TEL_FROM,
    telefono_qr,
    numero,
    telefono_local
  ];
  for (const v of candidatos) {
    const n = normalizarNroTelFromApiMensajes(v);
    if (n) return n;
  }
  return '';
}

var habilitar_bot = parseBoolLike(
  process.env.HABILITAR_BOT || process.env.BOT_HABILITADO || process.env.ENABLE_BOT,
  true
);
var consulta_api_mensajes_habilitado = parseBoolLike(
  process.env.HABILITAR_CONSULTA_MENSAJES || process.env.CONSULTA_API_MENSAJES_ENABLED || process.env.ENABLE_CONSULTA_API_MENSAJES,
  false
);

// Habilita el envío desde es_mensajes por dominio/tenant.
// Se puede configurar en tenant_config con habilitar_mensajes_info = true.
var habilitar_mensajes_info = parseBoolLike(
  process.env.HABILITAR_MENSAJES_INFO ?? process.env.MENSAJES_INFO_HABILITADO ?? process.env.ENVIAR_MENSAJES_INFO_HABILITADO,
  false
);

// Habilita el loop local por ODBC/Manager (compras, entregas y es_mensajes).
// Para tenants que solo usan Api_Mensajes/Consulta_no_enviados, poner false en tenant_config.
var habilitar_odbc_manager = parseBoolLike(
  process.env.HABILITAR_ODBC_MANAGER ?? process.env.ODBC_MANAGER_HABILITADO ?? process.env.HABILITAR_MANAGER_LOCAL,
  true
);


var consulta_mensajes_respetar_horarios = parseBoolLike(
  process.env.CONSULTA_MENSAJES_RESPETAR_HORARIOS || process.env.CONSULTA_API_MENSAJES_RESPETAR_HORARIOS,
  true
);
var consulta_mensajes_fuera_horario_sleep_ms = Number(process.env.CONSULTA_MENSAJES_FUERA_HORARIO_SLEEP_MS || 60000);
if (!Number.isFinite(consulta_mensajes_fuera_horario_sleep_ms) || consulta_mensajes_fuera_horario_sleep_ms < 5000) consulta_mensajes_fuera_horario_sleep_ms = 60000;

var api_mensajes_confirmacion_habilitada = parseBoolLike(
  process.env.API_MENSAJES_CONFIRMACION_HABILITADA || process.env.CONFIRMAR_API_MENSAJES,
  false
);
var api_mensajes_confirmacion_mensaje = String(
  process.env.API_MENSAJES_CONFIRMACION_MENSAJE ||
  'Hola, vas a recibir un mensaje de nuestra parte. Respondé OK para autorizar la recepción.'
);
var api_mensajes_confirmacion_respuestas_ok = process.env.API_MENSAJES_CONFIRMACION_RESPUESTAS_OK || 'OK,SI,SÍ,S';
var api_mensajes_confirmacion_reenviar_ms = Number(process.env.API_MENSAJES_CONFIRMACION_REENVIAR_MS || 86400000);
if (!Number.isFinite(api_mensajes_confirmacion_reenviar_ms) || api_mensajes_confirmacion_reenviar_ms < 0) api_mensajes_confirmacion_reenviar_ms = 86400000;
var api_mensajes_confirmacion_validez_ms = Number(process.env.API_MENSAJES_CONFIRMACION_VALIDEZ_MS || 0);
if (!Number.isFinite(api_mensajes_confirmacion_validez_ms) || api_mensajes_confirmacion_validez_ms < 0) api_mensajes_confirmacion_validez_ms = 0;


var consultaApiMensajesRunning = false;

let consultaMensajesHoursCache = { expiresAt: 0, hours: null, updatedAt: null };
let lastConsultaMensajesHorarioLogKey = '';

var runtime_config_refresh_ms = Number(process.env.RUNTIME_CONFIG_REFRESH_MS || process.env.CONFIG_REFRESH_MS || 30000);
if (!Number.isFinite(runtime_config_refresh_ms) || runtime_config_refresh_ms < 5000) runtime_config_refresh_ms = 5000;
let runtimeConfigPollTimer = null;
let runtimeConfigPollBusy = false;
let lastRuntimeConfigSnapshot = null;
var msg_inicio = "";
var msg_fin = "";
var cant_lim = 0;
var msg_lim = 'Continuar? S / N';
var time_cad = 0;
var mensajeCaducidadWatcherStarted = false;

var msg_cad = "";
var msg_can = "";
var bandera_msg = 1;
var jsonGlobal = [];   //1-json, 2 -i , 3-tel, 4-hora
var json;
var i_global = 0;
var msg_body;

var msg_errores;
var nom_chatbot;

var Id_msj_dest ;
var Id_msj_renglon;

// id del registro es_mensajes que esperamos actualizar cuando llegue el ACK
// (igual que app_chatbot_super)
var id_msg = 0;
// Mapa robusto: wsMsgId -> id (DB). Evita carreras cuando salen varios envíos seguidos.
const pendingAck = new Map();

var signatures = {
  JVBERi0: "application/pdf",
  R0lGODdh: "image/gif",
  R0lGODlh: "image/gif",
  iVBORw0KGgo: "image/png",
  "/9j/": "image/jpg"
};



const logFilePath_event = path.join(__dirname, 'app_asisto_event.log');
const logFilePath_error = path.join(__dirname, 'app_asisto_error.log');

EscribirLog(
  "inicio Script pid=" + process.pid +
  " restarted_from_panel=" + (process.env.ASISTO_RESTARTED_FROM_PANEL || "0") +
  " file=" + __filename,
  "event"
);


const app = express();
const server = http.createServer(app);
const io = socketIO(server);

app.use(express.json());
app.use(express.urlencoded({
  extended: true
}));


app.use(fileUpload({
  debug: false
}));

app.get('/', (req, res) => {
  res.sendFile('index.html', {
    root: __dirname
  });
});



// =========================
// STATUS endpoints (debug / monitoreo)
// =========================

function requireStatusToken(req, res, next) {
  if (!status_token) return next();
  const t = String(req.query?.token || req.headers["x-status-token"] || "");
  if (t && t === String(status_token)) return next();
  return res.status(401).json({ ok: false, error: "unauthorized" });
}

let mongoConnectionEventsBound = false;
let mongoFullIdleTimer = null;
let mongoLastUseAt = 0;
let mongoIdleDisconnectPromise = null;

function readMongoInt(envNames, fallback, min, max) {
  const names = Array.isArray(envNames) ? envNames : [envNames];
  let raw = NaN;
  for (const name of names) {
    if (process.env[name] !== undefined && process.env[name] !== '') {
      raw = Number(process.env[name]);
      break;
    }
  }
  const value = Number.isFinite(raw) ? Math.trunc(raw) : fallback;
  return Math.max(min, Math.min(max, value));
}

function readMongoFullIdleDisconnectMs() {
  const raw = Number(
    process.env.MONGO_FULL_IDLE_DISCONNECT_MS ??
    process.env.MONGODB_FULL_IDLE_DISCONNECT_MS
  );
  if (Number.isFinite(raw) && raw <= 0) return 0;
  const value = Number.isFinite(raw) ? Math.trunc(raw) : 300000;
  return Math.max(60000, Math.min(86400000, value));
}

function clearMongoFullIdleTimer() {
  if (!mongoFullIdleTimer) return;
  try { clearTimeout(mongoFullIdleTimer); } catch {}
  mongoFullIdleTimer = null;
}

function touchMongoActivity() {
  mongoLastUseAt = Date.now();
  armMongoFullIdleTimer();
}

function armMongoFullIdleTimer() {
  clearMongoFullIdleTimer();

  const idleMs = readMongoFullIdleDisconnectMs();
  if (!idleMs || !mongoReady || mongoose?.connection?.readyState !== 1) return;

  const elapsed = Math.max(0, Date.now() - (mongoLastUseAt || Date.now()));
  const waitMs = Math.max(1000, idleMs - elapsed);

  mongoFullIdleTimer = setTimeout(async () => {
    mongoFullIdleTimer = null;
    if (mongoIdleDisconnectPromise || mongoConnectingPromise) return;

    const inactiveFor = Date.now() - (mongoLastUseAt || 0);
    if (inactiveFor < idleMs) {
      armMongoFullIdleTimer();
      return;
    }

    mongoIdleDisconnectPromise = disconnectMongoSafe(`idle_timeout_${inactiveFor}ms`)
      .catch(() => {})
      .finally(() => { mongoIdleDisconnectPromise = null; });
    await mongoIdleDisconnectPromise;
  }, waitMs);

  if (mongoFullIdleTimer && typeof mongoFullIdleTimer.unref === 'function') {
    mongoFullIdleTimer.unref();
  }
}


function buildMongoAppName() {
  return `asisto-wweb-${tenantId || 'SIN_TENANT'}-${os.hostname()}-${process.pid}`
    .replace(/[^a-zA-Z0-9_.-]+/g, '_')
    .slice(0, 128);
}

function bindMongoConnectionEventsOnce() {
  if (mongoConnectionEventsBound) return;
  mongoConnectionEventsBound = true;

  mongoose.connection.on('disconnected', () => {
    mongoReady = false;
  });
  mongoose.connection.on('close', () => {
    mongoReady = false;
  });
}

async function disconnectMongoSafe(reason = 'shutdown') {
  clearMongoFullIdleTimer();
  try {
    if (mongoose?.connection?.readyState !== 0) {
      try { EscribirLog(`[MONGO] desconectando reason=${String(reason || '')}`, 'event'); } catch {}
      await mongoose.disconnect();
    }
  } catch (e) {
    try { console.log('[MONGO] error al desconectar:', e?.message || e); } catch {}
    try { EscribirLog('[MONGO] error al desconectar: ' + String(e?.message || e), 'error'); } catch {}
  } finally {
    mongoReady = false;
    mongoConnectingPromise = null;
    mongoLastUseAt = 0;
  }
}


async function ensureMongo() {
  try {
    if (isControlApiConfigured()) {
      controlApi.configure({ tenantId, numero });
      const ok = await controlApi.ensureReady();
      if (ok) {
        initMongoModelsIfNeeded();
        if (!controlApiReadyLogged) {
          controlApiReadyLogged = true;
          const msg = `[CONTROL_API] conectado mode=https url=${control_api_url} tenant=${tenantId} numero=${numero || '(pendiente)'}`;
          try { console.log(msg); } catch {}
          try { EscribirLog(msg, 'event'); } catch {}
        }
      } else if ((Date.now() - controlApiLastErrorLogAt) > 30000) {
        controlApiLastErrorLogAt = Date.now();
        const msg = `[CONTROL_API] no disponible url=${control_api_url} tenant=${tenantId}`;
        try { console.log(msg); } catch {}
        try { EscribirLog(msg, 'error'); } catch {}
      }
      return ok;
    }
    // Desde acá sólo llegamos si esta sesión necesita Mongo directo.
    // Las sesiones que usan Control API HTTPS no cargan mongoose en memoria.
    getMongooseModule();

    if (mongoIdleDisconnectPromise) {
      try { await mongoIdleDisconnectPromise; } catch {}
    }

    // Ya conectado
    if (mongoReady && mongoose?.connection?.readyState === 1 && mongoose?.connection?.db) {
      touchMongoActivity();
      initMongoModelsIfNeeded();
      return true;
    }

    // Una única promesa compartida evita connect() concurrentes dentro del proceso.
    if (mongoConnectingPromise) {
      const ok = await mongoConnectingPromise;
      if (ok) initMongoModelsIfNeeded();
      return ok;
    }

    if (!mongo_uri) return false;

    const maxPoolSize = readMongoInt(['MONGO_MAX_POOL_SIZE', 'MONGODB_MAX_POOL_SIZE'], 1, 1, 5);
    const appName = buildMongoAppName();
    bindMongoConnectionEventsOnce();

    mongoConnectingPromise = (async () => {
      try {
        await mongoose.connect(mongo_uri, {
          dbName: (mongo_db || tenantId || "asisto"),
          autoIndex: true,
          appName,

          // Cada PC mantiene un pool pequeño. Atlas M0 limita conexiones globales.
          maxPoolSize,
          minPoolSize: 0,
          maxConnecting: readMongoInt('MONGO_MAX_CONNECTING', 1, 1, Math.min(5, maxPoolSize)),
          maxIdleTimeMS: readMongoInt('MONGO_MAX_IDLE_TIME_MS', 60000, 1000, 3600000),
          waitQueueTimeoutMS: readMongoInt('MONGO_WAIT_QUEUE_TIMEOUT_MS', 10000, 1000, 120000),

          serverSelectionTimeoutMS: readMongoInt('MONGO_SERVER_SELECTION_TIMEOUT_MS', 15000, 1000, 120000),
          connectTimeoutMS: readMongoInt('MONGO_CONNECT_TIMEOUT_MS', 10000, 1000, 120000),
          socketTimeoutMS: readMongoInt('MONGO_SOCKET_TIMEOUT_MS', 45000, 1000, 300000)
        });

        if (!mongoose.connection.db) {
          await new Promise((resolve, reject) => {
            const t = setTimeout(() => reject(new Error("mongo_db_not_ready")), 15000);
            mongoose.connection.once("connected", () => { clearTimeout(t); resolve(); });
          });
        }

        mongoReady = true;

        try {
          const host = mongoose?.connection?.host || "";
          const dbName = mongoose?.connection?.name || (mongo_db || tenantId || "asisto");
          const msg = `Mongo conectado. dbName=${dbName} host=${host} appName=${appName} maxPoolSize=${maxPoolSize} fullIdleDisconnectMs=${readMongoFullIdleDisconnectMs()}`;
          console.log(msg);
          EscribirLog(msg, "event");
        } catch {}

        initMongoModelsIfNeeded();
        return true;
      } catch (e) {
        try { console.log("Mongo connect error:", e?.message || e); } catch {}
        try { EscribirLog("Mongo connect error: " + String(e?.message || e), "error"); } catch {}
        await disconnectMongoSafe('connect_error');
        return false;
      } finally {
        mongoConnectingPromise = null;
      }
    })();

    const ok = await mongoConnectingPromise;
    if (ok) initMongoModelsIfNeeded();
    return ok;
  } catch (e) {
    try { console.log("ensureMongo error:", e?.message || e); } catch {}
    try { EscribirLog("ensureMongo error: " + String(e?.message || e), "error"); } catch {}
    mongoReady = false;
    mongoConnectingPromise = null;
    return false;
  }
}

// Inicializa modelos una sola vez (lock/policies/history/actions)
function initMongoModelsIfNeeded() {
  try {
    if (isControlApiConfigured()) {
      // Sobrescribir modelos Mongoose que pudieron crearse durante la conexión
      // de transición. Desde este punto todas las operaciones salen por HTTPS.
      PolicyModel = controlApi.model('wa_wweb_policies');
      HistoryModel = controlApi.model('wa_wweb_history');
      LockModel = controlApi.model('wa_locks');
      ActionModel = controlApi.model('wa_wweb_actions');
      MessageLogModel = controlApi.model('wa_wweb_message_log');
      ApiMessageWindowModel = controlApi.model('wa_api_message_windows');
      return;
    }

    if (!mongoose?.connection?.db) return;

    if (!PolicyModel) {
      const PolicySchema = new mongoose.Schema(
        {
          _id: { type: String },
          tenantid: { type: String },
          tenantId: { type: String, index: true },
          numero: { type: String, index: true },
          disabled: { type: Boolean, default: false },
          paused: { type: Boolean, default: false },
          pausado: { type: Boolean, default: false },
          blocked: { type: Boolean, default: false },
          messagesBlocked: { type: Boolean, default: false },
          mensajes_bloqueados: { type: Boolean, default: false },
          bloqueado: { type: Boolean, default: false },
          blockMode: { type: String }
        },
        { collection: "wa_wweb_policies", strict: false }
      );
      PolicyModel = mongoose.models.WaWwebPolicy || mongoose.model("WaWwebPolicy", PolicySchema);
    }

    if (!HistoryModel) {
      const HistorySchema = new mongoose.Schema(
        {
          lockId: { type: String, index: true },
          event: { type: String, index: true },
          host: { type: String },
          pid: { type: Number },
          detail: { type: mongoose.Schema.Types.Mixed },
          at: { type: Date, default: Date.now, index: true }
        },
        { collection: "wa_wweb_history" }
      );
      HistoryModel = mongoose.models.WaWwebHistory || mongoose.model("WaWwebHistory", HistorySchema);
    }

    if (!LockModel) {
      const LockSchema = new mongoose.Schema(
        {
          _id: { type: String },
          tenantId: { type: String },
          tenantid: { type: String, index: true },
          numero: { type: String },
          holderId: { type: String },
          host: { type: String },
          pid: { type: Number },
          state: { type: String },
          startedAt: { type: Date },
          lastSeenAt: { type: Date },
          lastQrAt: { type: String },
          lastQrDataUrl: { type: String }
        },
        { collection: "wa_locks" }
      );
      LockModel = mongoose.models.WaLock || mongoose.model("WaLock", LockSchema);
    }

    if (!ActionModel) {
      const ActionSchema = new mongoose.Schema(
        {
          lockId: { type: String, index: true },
          action: { type: String, index: true },
          reason: { type: String },
          to: { type: String },
          waId: { type: String },
          text: { type: String },
          body: { type: String },
          message: { type: String },
          payload: { type: Object },
          requestedBy: { type: String },
          requestedAt: { type: Date, default: Date.now, index: true },
          consumedAt: { type: Date },
          doneAt: { type: Date, index: true },
          doneBy: { type: String },
          result: { type: String }
        },
        { collection: "wa_wweb_actions" }
      );
      ActionModel = mongoose.models.WaWwebAction || mongoose.model("WaWwebAction", ActionSchema);
    }

    if (!MessageLogModel) {
      const MessageLogSchema = new mongoose.Schema(
        {
          tenantId: { type: String, index: true },
          numero: { type: String, index: true },
          contact: { type: String, index: true },
          direction: { type: String, index: true },
          messageType: { type: String, index: true },
          body: { type: String },
          bodyLength: { type: Number },
          hasMedia: { type: Boolean, default: false },
          at: { type: Date, default: Date.now, index: true },
          atLocal: { type: String },
          dayKey: { type: String, index: true }
        },
        { collection: "wa_wweb_message_log" }
      );
      MessageLogModel = mongoose.models.WaWwebMessageLog || mongoose.model("WaWwebMessageLog", MessageLogSchema);
    }

    if (!ApiMessageWindowModel) {
      const ApiMessageWindowSchema = new mongoose.Schema(
        {
          tenantId: { type: String, index: true },
          numeroFrom: { type: String, index: true },
          contact: { type: String, index: true },
          contactName: { type: String },
          channelType: { type: String, default: 'api_messages', index: true },
          source: { type: String, default: 'consulta_api_mensajes', index: true },
          windowMinutes: { type: Number },
          windowStartedAt: { type: Date, index: true },
          windowEndsAt: { type: Date, index: true },
          lastMessageAt: { type: Date, index: true },
          messageCount: { type: Number, default: 1 },
          unitValue: { type: Number, default: 0 },
          amount: { type: Number, default: 0 },
          currency: { type: String, default: 'ARS' },
          messages: { type: [mongoose.Schema.Types.Mixed], default: [] },
          createdAt: { type: Date, default: Date.now },
          updatedAt: { type: Date, default: Date.now }
        },
        { collection: "wa_api_message_windows", strict: false }
      );
      ApiMessageWindowModel = mongoose.models.WaApiMessageWindow || mongoose.model("WaApiMessageWindow", ApiMessageWindowSchema);
    }
  } catch {}
}

 

// =========================
// Carga configuración por tenant desde MongoDB (colección tenant_config)
// - configuracion.json SOLO: tenantId, mongo_uri, mongo_db
// - el resto (numero, puerto, headless, timers, paths, etc.) viene de BD
// =========================
async function loadTenantConfigFromDbMinimal() {
  try {
    // Necesitamos bootstrap mínimo antes. En modo API ya no hace falta mongo_uri.
    if (!tenantId || (!mongo_uri && !isControlApiConfigured())) return null;
    const startedWithControlApi = isControlApiConfigured();
    const ok = await ensureMongo();
    if (!ok || !dataBackendReady()) return null;

    const collName = String(process.env.ASISTO_CONFIG_COLLECTION || "tenant_config").trim() || "tenant_config";
     const coll = getDataCollection(collName);

    // Soporta doc con _id=tenantId o con campo tenantId
    let doc = await coll.findOne({ _id: tenantId });
    if (!doc) doc = await coll.findOne({ tenantId: tenantId });
    if (!doc) {
      try { console.log(`[CONFIG] No existe config en BD para tenantId=${tenantId} (colección ${collName})`); } catch {}
      return null;
    }

    // Igual que en la carga inicial: si el tenant aún no tiene token, se crea
    // automáticamente usando la última conexión Mongo directa de la PC.
    if (!startedWithControlApi) {
      doc = await ensureControlApiBootstrapInTenantConfig(coll, doc);
     
    }

    const conf = extractTenantConfigFromDoc(doc);

    // Mantener la config completa del tenant en memoria y aplicarla al runtime.
    tenantConfig = conf;
    applyTenantConfig(conf);
    controlApi.configure({ tenantId, numero });

    if (!startedWithControlApi && isControlApiConfigured()) {
      await persistControlApiBootstrap();
      await disconnectMongoSafe('migrated_to_control_api');
      try { console.log('[CONTROL_API] migración completada; MongoDB directo deshabilitado'); } catch {}
      try { EscribirLog('[CONTROL_API] migración completada; MongoDB directo deshabilitado', 'event'); } catch {}
    }
    // Aplicar SOLO si vienen valores definidos (no pisar con vacíos)
    if (!numero && conf.numero) numero = String(conf.numero).trim();

    const forcedWorkerPort = ASISTO_MULTI_WORKER ? Number(process.env.ASISTO_WORKER_PORT || 0) : 0;
    if (Number.isFinite(forcedWorkerPort) && forcedWorkerPort > 0) {
      port = forcedWorkerPort;
    } else if (conf.puerto !== undefined && conf.puerto !== null && conf.puerto !== "") {
      const p = Number(conf.puerto);
      if (!Number.isNaN(p) && p > 0) port = p;
    }

    if (conf.headless !== undefined) {
      headless = conf.headless;
      if (typeof headless === "string") headless = headless.toLowerCase().trim() === "true";
      else headless = !!headless;
    }

    if (conf.lease_ms !== undefined && conf.lease_ms !== null && conf.lease_ms !== "") {
      const v = Number(conf.lease_ms);
      if (!Number.isNaN(v) && v > 0) lease_ms = v;
    }
    if (conf.heartbeat_ms !== undefined && conf.heartbeat_ms !== null && conf.heartbeat_ms !== "") {
      const v = Number(conf.heartbeat_ms);
      if (!Number.isNaN(v) && v > 0) heartbeat_ms = v;
    }
    if (conf.backup_every_ms !== undefined && conf.backup_every_ms !== null && conf.backup_every_ms !== "") {
      const v = Number(conf.backup_every_ms);
      if (!Number.isNaN(v) && v > 0) backup_every_ms = v;
    }
    // En Windows el zip puede demorar bastante; evitamos lease muy bajo aunque venga en tenant_config
    if (!Number.isFinite(lease_ms) || lease_ms < MIN_LEASE_MS) lease_ms = MIN_LEASE_MS;

    const abp = conf.auth_base_path || conf.auth_path;
    if (abp !== undefined && abp !== null && String(abp).trim()) {
      auth_base_path = String(abp).trim();
    }

    if (conf.auth_mode !== undefined && conf.auth_mode !== null && String(conf.auth_mode).trim()) {
      auth_mode = String(conf.auth_mode).trim().toLowerCase();
    }

    const forcedWorkerEngine = ASISTO_MULTI_WORKER ? String(process.env.ASISTO_WORKER_WWEB_ENGINE || '').trim() : '';
    const engineRaw = forcedWorkerEngine || (
      conf.wweb_engine ?? conf.wwebEngine ??
      conf.whatsapp_web_engine ?? conf.whatsappWebEngine ??
      conf.whatsapp_client_engine ?? conf.whatsappClientEngine
    );
    if (engineRaw !== undefined && engineRaw !== null && String(engineRaw).trim()) {
      wweb_engine = normalizeWwebEngine(engineRaw);
    }

    const baileysAuthPathRaw = conf.baileys_auth_base_path ?? conf.baileysAuthBasePath ?? conf.baileys_auth_path ?? conf.baileysAuthPath;
    if (baileysAuthPathRaw !== undefined && baileysAuthPathRaw !== null && String(baileysAuthPathRaw).trim()) {
      baileys_auth_base_path = String(baileysAuthPathRaw).trim();
    }

    if (!status_token && conf.status_token) status_token = String(conf.status_token).trim();

    applyAutoUpdateConfig(conf);

    try { 
      
      //console.log(`[CONFIG] tenantId=${tenantId} numero=${numero} puerto=${port} headless=${headless} auth_mode=${auth_mode || 'local'} lease_ms=${lease_ms} heartbeat_ms=${heartbeat_ms} desiredTag=${auto_update_target_tag || '(auto)'}`);
  
  
  } catch {}
    return conf;
  } catch (e) {
    try { console.log("loadTenantConfigFromDbMinimal error:", e?.message || e); } catch {}
    try { EscribirLog("loadTenantConfigFromDbMinimal error: " + String(e?.message || e), "error"); } catch {}
    return null;
  }
}




async function refreshTenantConfigFromDbPerMessage() {
  try {
    if (!tenantId || (!mongo_uri && !isControlApiConfigured())) return tenantConfig;
    const conf = await loadTenantConfigFromDbMinimal();
    if (conf && typeof conf === "object") {
      tenantConfig = conf;
      applyTenantConfig(conf);
      return conf;
    }
  } catch (e) {
    try { console.log("refreshTenantConfigFromDbPerMessage error:", e?.message || e); } catch {}
    try { EscribirLog("refreshTenantConfigFromDbPerMessage error: " + String(e?.message || e), "error"); } catch {}
  }

  try {
    if (tenantConfig && typeof tenantConfig === "object") {
      applyTenantConfig(tenantConfig);
      return tenantConfig;
    }
  } catch {}

  return null;
}

async function pushHistory(event, detail) {
  try {
    if (!await ensureMongo()) return null;
    if (!HistoryModel || !lockId) return null;
    return await HistoryModel.create({
      lockId,
      event: String(event || ""),
      host: os.hostname(),
      pid: process.pid,
      detail: detail || null,
      at: new Date()
    });
  } catch {
    return null;
  }
}

async function getPolicySafe() {
  try {
    if (!await ensureMongo()) return null;

    const tid = String(tenantId || '').trim();
    const nums = Array.from(new Set([
      onlyDigits(numero || ''),
      onlyDigits(telefono_qr || ''),
     onlyDigits(getApiMensajesNroTelFrom ? getApiMensajesNroTelFrom() : ''),
      onlyDigits(telefono_local || '')
    ].filter(Boolean)));

   const or = [];
    if (lockId) {
      or.push({ _id: String(lockId) });
      or.push({ lockId: String(lockId) });
    }
   for (const n of nums) {
      if (tid) {
        or.push({ tenantId: tid, numero: n });
        or.push({ tenantid: tid, numero: n });
      }
     or.push({ numero: n });
    }

    // Leer directo desde Mongo para no depender de paths declarados en el schema.
   // La pausa del panel puede estar guardada como paused/messagesBlocked/etc.
    if (dataBackendReady() && or.length) {
      const p0 = await getDataCollection('wa_wweb_policies').findOne({ $or: or });
      if (p0) return p0;
    }

    if (!PolicyModel) return null;

    if (lockId) {
      const p2 = await PolicyModel.findById(lockId).lean();
      if (p2) return p2;
    }
    if (tenantId && numero) {

      const p = await PolicyModel.findOne({
        numero: String(numero),
        $or: [
          { tenantId: String(tenantId) },
          { tenantid: String(tenantId) }
        ]
      }).lean();
      if (p) return p;
    }
   
    return null;
  } catch (e) {
    try { EscribirLog('getPolicySafe error: ' + String(e?.message || e), 'error'); } catch {}
    return null;
  }
}

function isPolicyMessagesBlocked(pol) {
  try {
    if (!pol) return false;

    // blockMode indica el tipo de bloqueo, pero NO debe bloquear por sí solo.
    // Antes quedaba blockMode="messages" aunque blocked=false y por eso seguía pausando.
    return !!(
      pol.paused === true ||
      pol.pausado === true ||
      pol.blocked === true ||
      pol.messagesBlocked === true ||
      pol.mensajes_bloqueados === true ||
      pol.bloqueado === true
    );
  } catch {
    return false;
  }
}

async function isWwebMessagesBlockedSafe() {
  try {
    const pol = await getPolicySafe();
    let blocked = isPolicyMessagesBlocked(pol);

    // Refuerzo: si el lock quedó en PAUSED/BLOCKED, también cortar la consulta API.
    // Esto evita que un proceso viejo siga consultando aunque el panel muestre Bot pausado.
    if (!blocked && LockModel && lockId) {
      try {
        const lockDoc = await LockModel.findById(lockId).lean();
        const st = String(lockDoc?.state || lockDoc?.status || '').toLowerCase();
        if (st === 'paused' || st === 'pause' || st === 'blocked' || st === 'bloqueado') blocked = true;
      } catch {}
    }

    if (blocked) {
      lastPolicyBlocked = true;
      localWsPanelState = 'paused';
      return true;
    }

    // No reanudar automáticamente si no hay política bloqueada.
   // La salida de pausa solamente debe venir por una acción explícita resume/reanudar.
    return lastPolicyBlocked === true || String(localWsPanelState || '').toLowerCase() === 'paused';
  } catch {
   return lastPolicyBlocked === true || String(localWsPanelState || '').toLowerCase() === 'paused';
  }
}


async function heartbeatTick() {
  try {
    if (heartbeatBusy) return;
    heartbeatBusy = true;
    if (!isOwner || !lockId) return;

    await updateLockStateSafe(localWsPanelState || 'online').catch(() => {});

    const pol = await getPolicySafe();
    const disabled = !!(pol && pol.disabled === true);
    const policyBlockedNow = isPolicyMessagesBlocked(pol);

    if (policyBlockedNow) {
      if (lastPolicyBlocked !== true || localWsPanelState !== 'paused') {
        try { EscribirLog('Bot pausado por política del panel', 'event'); } catch {}
        try { console.log('Bot pausado por política del panel'); } catch {}
       }
      lastPolicyBlocked = true;
      localWsPanelState = 'paused';
      try { await updateLockStateSafe('paused'); } catch {}
      return;
    }

    // No reanudar por ausencia de política o lectura incompleta.
    // El bot solo sale de pausa cuando llega action=resume/reanudar.
    if (lastPolicyBlocked === true || String(localWsPanelState || '').toLowerCase() === 'paused') {
      localWsPanelState = 'paused';
      try { await updateLockStateSafe('paused'); } catch {}
      return;
    }


    if (disabled) {
      lastPolicyDisabled = true;
      if (clientStarted || localWsPanelState !== 'disabled') {
        try { await updateLockStateSafe('disabled'); } catch {}
      }
      if (clientStarted) {
        try {
          if (client && typeof destroyClientHard === "function") await destroyClientHard(client);
          else if (client) await client.destroy();
        } catch {}
        try { client = null; } catch {}
        clientStarted = false;
      }
      return;
    }

    if (lastPolicyDisabled === true) {
      lastPolicyDisabled = false;
      if (isOwner && !clientStarted && !startingNow) {
        try { await startClientInitialize(); } catch {}
      }
    }
  } catch {}
  finally {
    heartbeatBusy = false;
  }
}

function startHeartbeat() {
  try { if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; } } catch {}

  const intervalMs = Math.max(5000, Number(heartbeat_ms) || 5000);

  heartbeatTick().catch(() => {});

  heartbeatTimer = setInterval(() => {
    heartbeatTick().catch(() => {});
  }, intervalMs);
}

function hostName() {
  return os.hostname();
}

async function getLockDocSafe() {
  try {
    if (await ensureMongo() && LockModel && lockId) {
      const doc = await LockModel.findById(lockId).lean();
      if (doc) return doc;
    }
  } catch {}

  const runtimeInfo = getCurrentRuntimeInfo();
  return {
    _id: lockId || `${tenantId}:${numero}`,
    tenantId,
    tenantid: tenantId,
    numero,
    holderId: instanceId,
    host: os.hostname(),
    pid: process.pid,
    state: localWsPanelState,
    wwebEngine: getWwebEngine(),
    activeWwebEngine: client?.__transport || '',
    startedAt: lockAcquiredAt || null,
    lastSeenAt: new Date(),
    lastQrAt,
    lastQrDataUrl,
    runtimeVersion: runtimeInfo.currentVersion || '',
    desiredTag: runtimeInfo.desiredTag || '',
    autoUpdateSource: runtimeInfo.autoUpdateSource || '',
    autoUpdateEnabled: !!runtimeInfo.autoUpdateEnabled
  };
}

app.get("/status", requireStatusToken, async (req, res) => {
  const lock = await getLockDocSafe();
  let waState = null;
  try { if (client) waState = await client.getState(); } catch {}

  const runtimeInfo = getCurrentRuntimeInfo();
  return res.json({
    ok: true,
    now: nowArgentinaISO(),
    tenantId,
    numero,
    instanceId,
    lockId,
    isOwner,
    clientStarted,
    wwebEngine: getWwebEngine(),
    activeWwebEngine: client?.__transport || null,
    waState,
    telefono_qr,
    runtimeInfo,
    dataBackend: isControlApiConfigured() ? 'https_control_api' : 'mongodb_direct',
    controlApiUrl: isControlApiConfigured() ? control_api_url : null,
    lock
  });
});

app.get("/status/lock", requireStatusToken, async (req, res) => {
  const lock = await getLockDocSafe();
  return res.json({ ok: true, lockId, runtimeInfo: getCurrentRuntimeInfo(), lock });
});

app.get("/status/qr", requireStatusToken, async (req, res) => {
  // Último QR capturado (raw + dataUrl) para poder mostrarlo sin socket.
  // Si ya está autenticado, puede venir null.
  return res.json({
    ok: true,
    now: nowArgentinaISO(),
    tenantId,
    numero,
    instanceId,
    lockId,
    isOwner,
    clientStarted,
    wwebEngine: getWwebEngine(),
    activeWwebEngine: client?.__transport || null,
    lastQrAt,
    lastQrRaw,
    lastQrDataUrl,
    runtimeInfo: getCurrentRuntimeInfo()
  });
});

app.post("/control/release", requireStatusToken, async (req, res) => {
  try {
    try { if (clientStarted && client) await client.destroy(); } catch {}
    clientStarted = false;
    localWsPanelState = 'offline';
    try { if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; } } catch {}
    await updateLockStateSafe('offline');
    isOwner = false;
    return res.json({ ok: true, released: true });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

 

function multiSessionBool(value, fallback = true) {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'boolean') return value;
  return ['1', 'true', 'yes', 'si', 'sí', 'on'].includes(String(value).trim().toLowerCase());
}

function normalizeMultiSessionEntry(item, index, basePort) {
  if (!item || typeof item !== 'object') return null;
  const nested = item.configuracion && typeof item.configuracion === 'object' ? item.configuracion : null;
  const raw = nested ? { ...item, ...nested } : { ...item };
  if (!multiSessionBool(raw.enabled ?? raw.activo ?? raw.active, true)) return null;

  const tenant = String(raw.tenantId ?? raw.tenantid ?? raw.tenant ?? raw.dominio ?? '').trim().toUpperCase();
  const numeroRaw = raw.numero ?? raw.number ?? raw.phone ?? raw.telefono ?? '';
  const phone = String(numeroRaw || '').replace(/\D/g, '');
  if (!tenant || !phone) return null;

  const key = String(raw.session_key ?? raw.sessionKey ?? raw.id ?? `${tenant}:${phone}`).trim() || `${tenant}:${phone}`;
  const explicitPort = Number(raw.puerto ?? raw.port ?? 0);
  const portValue = Number.isFinite(explicitPort) && explicitPort > 0 ? explicitPort : (basePort + index);
  const engineRaw = String(raw.wweb_engine ?? raw.wwebEngine ?? raw.whatsapp_web_engine ?? '').trim();

  const bootstrap = {
    ...raw,
    tenantId: tenant,
    numero: phone,
    puerto: portValue,
  };
  delete bootstrap.configuracion;
  delete bootstrap.multi_sessions;
  delete bootstrap.multiSessions;

  return {
    key,
    tenantId: tenant,
    numero: phone,
    port: portValue,
    engine: engineRaw ? normalizeWwebEngine(engineRaw) : '',
    bootstrap,
  };
}

function getConfiguredMultiSessions(boot = readBootstrapFromFile()) {
  if (!boot || typeof boot !== 'object') return [];
  let rawList = boot.multi_sessions ?? boot.multiSessions ?? boot.sessions ?? null;
  if (typeof rawList === 'string') {
    try { rawList = JSON.parse(rawList); } catch { rawList = null; }
  }
  if (!Array.isArray(rawList) || !rawList.length) return [];

  const basePortRaw = Number(boot.multi_base_port ?? boot.multiBasePort ?? process.env.ASISTO_MULTI_BASE_PORT ?? 8100);
  const basePort = Number.isFinite(basePortRaw) && basePortRaw > 0 ? Math.trunc(basePortRaw) : 8100;

  // Orden estable para que los puertos automáticos no cambien entre reinicios.
  const prelim = rawList
    .map((item) => {
      const nested = item && item.configuracion && typeof item.configuracion === 'object' ? item.configuracion : null;
      const v = nested ? { ...item, ...nested } : (item || {});
      const tenant = String(v.tenantId ?? v.tenantid ?? v.tenant ?? v.dominio ?? '').trim().toUpperCase();
      const phone = String(v.numero ?? v.number ?? v.phone ?? v.telefono ?? '').replace(/\D/g, '');
      return { item, sortKey: `${tenant}:${phone}` };
    })
    .sort((a, b) => a.sortKey.localeCompare(b.sortKey));

  const out = [];
  const seen = new Set();
  const usedPorts = new Set();
  for (let i = 0; i < prelim.length; i++) {
    const session = normalizeMultiSessionEntry(prelim[i].item, i, basePort);
    if (!session) continue;
    if (seen.has(session.key)) {
      try { console.log(`[MULTI] sesión duplicada ignorada key=${session.key}`); } catch {}
      continue;
    }
    seen.add(session.key);

    if (usedPorts.has(session.port)) {
      let candidate = basePort;
      while (usedPorts.has(candidate)) candidate++;
      session.port = candidate;
      session.bootstrap.puerto = candidate;
    }
    usedPorts.add(session.port);
    out.push(session);
  }
  return out;
}
function getMultiPrimarySessionKey(sessions, boot = readBootstrapFromFile()) {
  const list = Array.isArray(sessions) ? sessions : [];
  if (!list.length) return '';

  const rootTenant = String(boot?.tenantId ?? boot?.tenantid ?? '').trim().toUpperCase();
  const rootNumero = String(boot?.numero ?? boot?.number ?? '').replace(/\D/g, '');

  if (rootTenant) {
    const found = list.find((s) =>
      String(s.tenantId || '').trim().toUpperCase() === rootTenant &&
      (!rootNumero || String(s.numero || '').replace(/\D/g, '') === rootNumero)
    );
    if (found) return found.key;
  }

  return list[0].key;
}


function hashMultiSessionDefinition(session) {
  try {
    const stable = { ...(session?.bootstrap || session || {}) };

    // Estos campos se escriben automáticamente durante la migración a Control API.
    // No representan un cambio operativo de sesión y no deben provocar config_changed.
    for (const key of [
      'control_api_token', 'controlApiToken',
      'wweb_control_api_token', 'wwebControlApiToken',
      'control_api_url', 'controlApiUrl',
      'wweb_control_api_url', 'wwebControlApiUrl',
      'control_api_enabled', 'controlApiEnabled',
      'wweb_control_api_enabled', 'wwebControlApiEnabled',
      'status_token', 'statusToken'
    ]) {
      delete stable[key];
    }

    return crypto.createHash('sha1').update(JSON.stringify(stable)).digest('hex');
  } catch {
    return `${session.tenantId}:${session.numero}:${session.port}:${session.engine}`;
  }
}

function pipeWorkerLines(stream, sessionKey, target) {
  if (!stream || typeof stream.on !== 'function') return;
  let pending = '';
  stream.on('data', (chunk) => {
    pending += String(chunk || '');
    const parts = pending.split(/\r?\n/);
    pending = parts.pop() || '';
    for (const line of parts) {
      if (!line) continue;
      try { target(`[${sessionKey}] ${line}\n`); } catch {}
    }
  });
  stream.on('end', () => {
    if (!pending) return;
    try { target(`[${sessionKey}] ${pending}\n`); } catch {}
    pending = '';
  });
}

function createMultiWorkerEnv(session, isPrimary) {
  const env = { ...process.env };

  // Nunca propagar por error un token de otro tenant desde el entorno del supervisor.
  // Si la entrada de la sesión trae token propio, se vuelve a cargar debajo.
  delete env.WWEB_CONTROL_API_TOKEN;
  delete env.CONTROL_API_TOKEN;
  delete env.STATUS_TOKEN;

  env.ASISTO_MULTI_WORKER = '1';
  env.ASISTO_MULTI_SESSION_KEY = session.key;
  env.ASISTO_MULTI_PRIMARY_WORKER = isPrimary ? '1' : '0';
  env.ASISTO_WORKER_PORT = String(session.port);
  env.TENANT_ID = session.tenantId;
  env.NUMERO = session.numero;
  env.PORT = String(session.port);
  env.INSTANCE_ID = `${os.hostname()}-${sanitizeMultiSessionFilePart(session.key)}-t${Date.now()}`;
  env.ASISTO_WORKER_BOOTSTRAP_B64 = Buffer.from(JSON.stringify(session.bootstrap || {}), 'utf8').toString('base64');

  const sessionToken = String(
    session.bootstrap?.control_api_token ?? session.bootstrap?.controlApiToken ??
    session.bootstrap?.wweb_control_api_token ?? session.bootstrap?.wwebControlApiToken ?? ''
  ).trim();
  if (sessionToken) {
    env.WWEB_CONTROL_API_TOKEN = sessionToken;
    env.CONTROL_API_TOKEN = sessionToken;
  }
  const sessionStatusToken = String(session.bootstrap?.status_token ?? session.bootstrap?.statusToken ?? '').trim();
  if (sessionStatusToken) env.STATUS_TOKEN = sessionStatusToken;

  if (session.engine) {
    env.ASISTO_WORKER_WWEB_ENGINE = session.engine;
    env.ASISTO_WWEB_ENGINE = session.engine;
  } else {
    delete env.ASISTO_WORKER_WWEB_ENGINE;
  }
  return env;
}

function stopMultiWorkerRecord(record, reason = 'stop') {
  if (!record || !record.worker) return Promise.resolve();
  record.intentionalStop = true;
  const child = record.worker;
  try { console.log(`[MULTI] deteniendo ${record.session.key} pid=${child.pid || 0} reason=${reason}`); } catch {}

  return new Promise((resolve) => {
    let done = false;
    let timer = null;
    const finish = () => {
      if (done) return;
      done = true;
      if (timer) clearTimeout(timer);
      resolve();
    };

    try { child.once('exit', finish); } catch {}

    try {
      if (child.connected && typeof child.send === 'function') {
        child.send({ type: 'multi_stop_session', reason: String(reason || 'stop') });
      } else {
        child.kill();
      }
    } catch {
      try { child.kill(); } catch {}
    }

    timer = setTimeout(() => {
      try {
        if (child.exitCode === null && child.signalCode === null) child.kill();
      } catch {}
      finish();
    }, 5000);
  });
}

async function restartWholeMultiSessionProcess(state, request = {}) {
  if (!state || state.globalRestartInFlight) return;
  state.globalRestartInFlight = true;
  state.stopping = true;
  try { if (state.timer) clearInterval(state.timer); } catch {}
  state.timer = null;

  const reason = String(request?.reason || 'panel_restart').trim() || 'panel_restart';
  const requestedByKey = String(request?.key || '').trim();

  try {
    console.log(`[MULTI] reinicio GLOBAL solicitado por ${requestedByKey || '(worker)'} reason=${reason}; cerrando todas las sesiones...`);
  } catch {}

  const waits = [];
  for (const record of state.workers.values()) {
   if (!record?.worker) continue;

    waits.push(new Promise((resolve) => {
      let settled = false;
      const done = () => {
        if (settled) return;
        settled = true;
        resolve();
      };

      record.globalRestartReady = done;

      try {
        if (record.worker.connected && typeof record.worker.send === 'function') {
          record.worker.send({
            type: 'multi_prepare_global_restart',
            reason,
            requestedByKey
          });
        } else {
          done();
        }
      } catch {
        done();
      }

      setTimeout(done, 4500);
    }));
  }

  try { await Promise.allSettled(waits); } catch {}

  try {
    console.log('[MULTI] sesiones cerradas para reinicio global; saliendo para que el runner reinicie app_asisto_ws');
  } catch {}

  try { releaseSingleInstanceLock(); } catch {}
  setTimeout(() => {
    try { process.exit(getSupervisorRestartExitCode()); } catch {}
  }, 150);
}


function startMultiWorkerRecord(state, session, isPrimary) {
  const hash = hashMultiSessionDefinition(session);
  const old = state.workers.get(session.key);
  if (old?.worker) return old;

  const record = old || {
    session,
    hash,
    worker: null,
    restartTimer: null,
    intentionalStop: false,
    startedAt: 0,
    restartCount: 0,
    isPrimary: !!isPrimary,
    exitScope: '',
    globalRestartReady: null,
  };
  record.session = session;
  record.hash = hash;
  record.intentionalStop = false;
  record.isPrimary = !!isPrimary;
  record.exitScope = '';
  record.startedAt = Date.now();

  const worker = fork(__filename, [], {
    env: createMultiWorkerEnv(session, isPrimary),
    silent: true
  });
  record.worker = worker;
  state.workers.set(session.key, record);

  pipeWorkerLines(worker.stdout, session.key, (line) => process.stdout.write(line));
  pipeWorkerLines(worker.stderr, session.key, (line) => process.stderr.write(line));

  try { console.log(`[MULTI] proceso iniciado key=${session.key} pid=${worker.pid || 0} port=${session.port} engine=${session.engine || 'tenant_config'}`); } catch {}

  worker.on('message', (message) => {
    try {
      const type = String(message?.type || '');

      if (type === 'multi_primary_bootstrap_ready' && record.isPrimary) {
        if (!state.primaryBootstrapReady) {
          state.primaryBootstrapReady = true;
          console.log(`[MULTI] primario listo para habilitar workers secundarios key=${session.key}`);
          setImmediate(() => { reconcileMultiSessionWorkers(state).catch(() => {}); });
        }
        return;
      }

      if (type === 'multi_worker_exit_scope') {
        record.exitScope = String(message?.scope || '').trim().toLowerCase();
        return;
      }

      if (type === 'multi_global_restart_request') {
        // Reiniciar app_asisto_ws completo: supervisor + TODAS las sesiones.
        // Se difiere unos ms para que el worker que consumió la acción alcance
        // a guardar result en wa_wweb_actions antes de comenzar el cierre.
        setTimeout(() => {
          restartWholeMultiSessionProcess(state, {
            reason: message?.reason || 'panel_restart',
            key: message?.key || session.key
          }).catch((e) => {
            try { console.error('[MULTI] error reinicio global:', e?.stack || e?.message || e); } catch {}
            try { process.exit(getSupervisorRestartExitCode()); } catch {}
          });
        }, 300);
        return;
      }

      if (type === 'multi_worker_global_restart_ready') {
        const done = record.globalRestartReady;
        record.globalRestartReady = null;
        if (typeof done === 'function') done();
        return;
      }
    } catch {}
  });


  worker.on('error', (err) => {
    try { console.error(`[MULTI] proceso error key=${session.key}:`, err?.stack || err?.message || err); } catch {}
  });

  worker.on('exit', (code) => {
    const runtime = Date.now() - (record.startedAt || Date.now());
    record.worker = null;
    const exitScope = String(record.exitScope || '').trim().toLowerCase();
    record.exitScope = '';
    if (record.isPrimary) state.primaryBootstrapReady = false;

    try { console.log(`[MULTI] proceso finalizó key=${session.key} code=${code} runtimeMs=${runtime} scope=${exitScope || 'default'}`); } catch {}
    if (state.stopping || record.intentionalStop || !state.desired.has(session.key)) return;

    // El worker primario es el único autorizado a tocar git. Si sale con el código
    // de supervisor (normalmente 77), reiniciamos TODO el proceso para que supervisor
    // y workers carguen exactamente la misma versión del archivo.
    if (record.isPrimary && Number(code) === getSupervisorRestartExitCode() && exitScope !== 'worker') {
      try { console.log('[MULTI] worker primario solicitó reinicio global; saliendo para que el runner reinicie Asisto'); } catch {}
      setTimeout(() => { try { process.exit(getSupervisorRestartExitCode()); } catch {} }, 250);
      return;
    }

    record.restartCount = runtime > 30000 ? 0 : Math.min(8, (record.restartCount || 0) + 1);
    const delay = Math.min(30000, 1500 * Math.max(1, record.restartCount));
    record.restartTimer = setTimeout(() => {
      record.restartTimer = null;
      if (state.stopping || !state.desired.has(session.key)) return;
      startMultiWorkerRecord(state, state.desired.get(session.key), record.isPrimary);
    }, delay);
  });
  return record;
}

async function reconcileMultiSessionWorkers(state) {
  if (!state || state.stopping || state.reconcileBusy) return;
  state.reconcileBusy = true;
  try {
    const boot = readBootstrapFromFile();
    const sessions = getConfiguredMultiSessions(boot);
    const desired = new Map(sessions.map((s) => [s.key, s]));
    state.desired = desired;

    // El único worker autorizado a tocar Git/package.json es el tenant/numero raíz
    // de configuracion.json (SDG en esta instalación). Fallback: primera sesión.
    const primaryKey = getMultiPrimarySessionKey(sessions, boot);
    if (state.primaryKey !== primaryKey) {
      state.primaryBootstrapReady = false;
      try { console.log(`[MULTI] worker primario=${primaryKey || '(ninguno)'}`); } catch {}
    }
    state.primaryKey = primaryKey;

    for (const [key, record] of Array.from(state.workers.entries())) {
      const next = desired.get(key);
      if (!next) {
        if (record.restartTimer) { clearTimeout(record.restartTimer); record.restartTimer = null; }
        await stopMultiWorkerRecord(record, 'removed_from_config');
        state.workers.delete(key);
        continue;
      }
      const nextHash = hashMultiSessionDefinition(next);
      const shouldBePrimary = key === primaryKey;
      if (record.hash !== nextHash || record.isPrimary !== shouldBePrimary) {
        if (record.isPrimary) state.primaryBootstrapReady = false;
        if (record.restartTimer) { clearTimeout(record.restartTimer); record.restartTimer = null; }
        await stopMultiWorkerRecord(record, 'config_changed');
        state.workers.delete(key);
      }
    }

    // Fase 1: arrancar únicamente el primario. Puede hacer Git/npm sin que ningún
    // otro worker esté importando o modificando node_modules.
    const primarySession = desired.get(primaryKey);
    if (primarySession && !state.workers.get(primaryKey)?.worker) {
      startMultiWorkerRecord(state, primarySession, true);
    }

    // Fase 2: recién cuando el primario terminó su bootstrap/update inicial,
    // habilitamos el resto de las sesiones.
    if (state.primaryBootstrapReady) {
      for (const session of sessions) {
        if (session.key === primaryKey) continue;
        if (!state.workers.get(session.key)?.worker) {
          startMultiWorkerRecord(state, session, false);
        }
      }
    }
  } catch (e) {
    try { console.error('[MULTI] reconcile error:', e?.stack || e?.message || e); } catch {}
 } finally {
    state.reconcileBusy = false;
  }
}

async function stopMultiSessionSupervisor(reason = 'shutdown') {
  const state = multiSessionSupervisorState;
  if (!state || state.stopping) return;
  state.stopping = true;
  try { if (state.timer) clearInterval(state.timer); } catch {}
  state.timer = null;
  const stops = [];
  for (const record of state.workers.values()) {
    if (record.restartTimer) { clearTimeout(record.restartTimer); record.restartTimer = null; }
    stops.push(stopMultiWorkerRecord(record, reason));
  }
  await Promise.allSettled(stops);
  state.workers.clear();
}

async function runMultiSessionSupervisor(boot) {
  if (!isMainThread || ASISTO_MULTI_WORKER) return false;
  const sessions = getConfiguredMultiSessions(boot);
  if (!sessions.length) return false;

  const refreshRaw = Number(boot?.multi_refresh_ms ?? boot?.multiRefreshMs ?? process.env.ASISTO_MULTI_REFRESH_MS ?? 15000);
  const refreshMs = Number.isFinite(refreshRaw) ? Math.max(5000, refreshRaw) : 15000;

  const state = {
    workers: new Map(),
    desired: new Map(),
    stopping: false,
    reconcileBusy: false,
    timer: null,
    primaryKey: '',
    primaryBootstrapReady: false,
    globalRestartInFlight: false,
  };
  multiSessionSupervisorState = state;

  console.log(`[MULTI] supervisor activo pid=${process.pid} sesiones=${sessions.length} refreshMs=${refreshMs}`);
  await reconcileMultiSessionWorkers(state);
  state.timer = setInterval(() => { reconcileMultiSessionWorkers(state).catch(() => {}); }, refreshMs);
  return true;
}


    
(async function startAsistoWs() {

 // Si configuracion.json declara multi_sessions, el hilo principal NO abre una
  // sesión WhatsApp propia: supervisa procesos aislados que ejecutan este mismo archivo.
  try {
    if (isMainThread && !ASISTO_MULTI_WORKER) {
      const multiBoot = readBootstrapFromFile();
      if (getConfiguredMultiSessions(multiBoot).length > 0) {
        await runMultiSessionSupervisor(multiBoot);
        return;
      }
    }
  } catch (e) {
    try { console.error('[MULTI] no se pudo iniciar supervisor:', e?.stack || e?.message || e); } catch {}
  }

  // Bootstrap: configuracion.json (tenantId/mongo_uri/mongo_db) + tenant_config (resto)
  try {
    RecuperarJsonConf();

    // Tomar auto_update desde configuracion.json (bootstrap local)
    try {
      const boot = readBootstrapFromFile();
      applyAutoUpdateConfig(boot);
    } catch (e) {
      try { console.log('applyAutoUpdateConfig bootstrap error:', e?.message || e); } catch {}
      try { EscribirLog('applyAutoUpdateConfig bootstrap error: ' + String(e?.message || e), 'error'); } catch {}
    }

    // Cargar resto de configuración desde Mongo (numero/puerto/headless/etc.)
    await loadTenantConfigFromDbMinimal();
    try {
      const engineBootMsg = `[WWEB_ENGINE] configuración al arranque=${getWwebEngine()} tenant=${tenantId} numero=${numero || '(pendiente)'}${ASISTO_MULTI_WORKER ? ` multiWorker=${ASISTO_MULTI_SESSION_KEY} threadId=${threadId}` : ''}`;
      console.log(engineBootMsg);
      EscribirLog(engineBootMsg, 'event');
    } catch {}

    // Si el tenant pide una TAG concreta, validar al iniciar antes de levantar WhatsApp.
    // En multi-sesión SOLO el worker primario puede tocar git/package.json.
    try {
      if (!ASISTO_MULTI_WORKER || ASISTO_MULTI_PRIMARY_WORKER) {
        await autoUpdateForceTargetTagOnBoot('boot_target_tag_force');
        if (autoUpdateRestarting) return;
      } else {
        try { console.log('[AUTO_UPDATE] skip boot_target_tag_force: worker secundario multi-sesión'); } catch {}
      }
    } catch (e) {
      const detail = String(e?.stderr || e?.stdout || e?.message || e || '').trim();
      try { console.log('boot_target_tag_force auto-update error:', detail || (e?.message || e)); } catch {}
      try { EscribirLog('boot_target_tag_force auto-update error: ' + (detail || String(e?.message || e)), 'error'); } catch {}
    }
 
    if (ASISTO_MULTI_WORKER && ASISTO_MULTI_PRIMARY_WORKER) {
      
      try {
        sendMultiSupervisorMessage({
          type: 'multi_primary_bootstrap_ready',
          key: ASISTO_MULTI_SESSION_KEY,
          pid: process.pid
        });
      } catch {}
    }

    server.listen(port, function() {
      console.log('App running on *: ' + port);
      EscribirLog('App running on *: ' + port,"event");
    });

    startAutoUpdateScheduler();
    startRuntimeConfigPoller();
    startCaducidadMensajesWatcher('startup');

    bootstrapWithLock().catch(e => {
      console.log('bootstrap inicio directo error:', e?.message || e);
      EscribirLog('bootstrap inicio directo error: ' + String(e?.message || e), 'error');
    });
  } catch (e) {
    console.log('FATAL bootstrap:', e?.message || e);
   try { EscribirLog('FATAL bootstrap: ' + String(e?.message || e), 'error'); } catch {}
    // No matamos el proceso: dejamos el server arriba para debug.
    try {
      server.listen(port, function() {
        console.log('App running on *: ' + port);
        EscribirLog('App running on *: ' + port,"event");
      });
    } catch {}
  }
})();

let store = null;
let client = null;
let clearAuthInFlight = false;
let multiGlobalRestartCloseInFlight = false;

if (ASISTO_MULTI_WORKER) {
  onMultiSupervisorMessage((message) => {
    const type = String(message?.type || '');

    if (type === 'multi_stop_session') {
      const reason = String(message?.reason || 'multi_stop_session');
      gracefulShutdown('MULTI_STOP:' + reason).catch(() => {
        try { process.exit(0); } catch {}
      });
      return;
    }

    if (type !== 'multi_prepare_global_restart') return;
    if (multiGlobalRestartCloseInFlight) return;
    multiGlobalRestartCloseInFlight = true;

    (async () => {
      const reason = String(message?.reason || 'panel_restart');
      try { localWsPanelState = 'restarting'; } catch {}
      try { await updateLockStateSafe('restarting'); } catch {}

      // Cierre compartido para ambos motores:
      // - Baileys: cierra socket WebSocket.
      // - whatsapp-web.js: cierra Client/Puppeteer/Chromium con timeout y fallback.
      clearRuntimeTimersForExit('multi_global_restart');
      try { await closeWhatsappClientForProcessExit(client, 'multi_global_restart:' + reason, 2500); } catch {}
      try { client = null; } catch {}
      try { resetClientRuntimeFlags('multi_global_restart'); } catch {}

      try { await Promise.race([forceReleaseLock('restarting'), timeoutPromise(1500, 'release_lock_timeout')]); } catch {}
      try { isOwner = false; } catch {}
      try { await Promise.race([disconnectMongoSafe('multi_global_restart'), timeoutPromise(2500, 'mongo_disconnect_timeout')]); } catch {}
      try { releaseSingleInstanceLock(); } catch {}

      sendMultiSupervisorMessage({
        type: 'multi_worker_global_restart_ready',
        key: ASISTO_MULTI_SESSION_KEY
      });
    })().catch(() => {
      sendMultiSupervisorMessage({
        type: 'multi_worker_global_restart_ready',
        key: ASISTO_MULTI_SESSION_KEY
      });
    });
  });
}
// =========================
// LocalAuth helpers
// =========================
function getAuthBasePath() {
  // priority: config auth_base_path -> env -> default in user home
  if (auth_base_path && String(auth_base_path).trim()) return String(auth_base_path).trim();
  const envp = process.env.ASISTO_AUTH_PATH;
  if (envp && String(envp).trim()) return String(envp).trim();
  return path.join(os.homedir(), ".asisto_wwebjs_auth");
}

function getLocalAuthSessionDir(clientId) {
  // whatsapp-web.js LocalAuth creates: <dataPath>/session-<clientId>
  return path.join(getAuthBasePath(), `session-${clientId}`);
}

function getBaileysAuthBasePath() {
  if (baileys_auth_base_path && String(baileys_auth_base_path).trim()) return String(baileys_auth_base_path).trim();
  const envp = process.env.ASISTO_BAILEYS_AUTH_PATH;
  if (envp && String(envp).trim()) return String(envp).trim();
  return path.join(os.homedir(), '.asisto_baileys_auth');
}

function getBaileysAuthSessionDir(clientId) {
  return path.join(getBaileysAuthBasePath(), `baileys-${clientId}`);
}


function getWwebClientId() {
  return `asisto_${tenantId}_${numero}`;
}

async function removePathSafe(targetPath, label = 'path') {
  try {
    if (!targetPath) return false;
    if (!fs.existsSync(targetPath)) return false;
    await fs.promises.rm(targetPath, {
      recursive: true,
      force: true,
      maxRetries: 10,
      retryDelay: 500
    });
    try { EscribirLog('[CLEAR_AUTH] eliminado ' + label + ': ' + targetPath, 'event'); } catch {}
    return true;
  } catch (e) {
    try { EscribirLog('[CLEAR_AUTH] no se pudo eliminar ' + label + ' ' + targetPath + ': ' + String(e?.message || e), 'error'); } catch {}
    return false;
  }
}

async function clearLocalAuthFilesSafe(clientId) {
  try {
    const sessionDir = getLocalAuthSessionDir(clientId);
    const removed = await removePathSafe(sessionDir, 'LocalAuth sessionDir');
    return { removed, sessionDir, engine: 'wwebjs', authMode: 'local' };
  } catch (e) {
    try { EscribirLog('[CLEAR_AUTH] clearLocalAuthFilesSafe error: ' + String(e?.message || e), 'error'); } catch {}
    return { removed: false, error: String(e?.message || e), engine: 'wwebjs', authMode: 'local' };
  }
}

async function clearBaileysAuthFilesSafe(clientId) {
  try {
    const sessionDir = getBaileysAuthSessionDir(clientId);
    const removed = await removePathSafe(sessionDir, 'Baileys authDir');
    return { removed, sessionDir, engine: 'baileys', authMode: 'local' };
  } catch (e) {
    try { EscribirLog('[CLEAR_AUTH] clearBaileysAuthFilesSafe error: ' + String(e?.message || e), 'error'); } catch {}
    return { removed: false, error: String(e?.message || e), engine: 'baileys', authMode: 'local' };
  }
}

async function clearRemoteAuthStoreSafe(clientId) {
  const result = { attempted: false, removed: false };
  try {
    if (!isRemoteAuthMode()) return result;
    result.attempted = true;
    if (!store) {
        const MongoStoreCtor = await ensureWwebMongoStoreLoaded();
        store = new MongoStoreCtor({ mongoose: getMongooseModule() });
      }

    const candidates = [
      async () => (typeof store.delete === 'function') ? store.delete({ session: clientId }) : undefined,
      async () => (typeof store.delete === 'function') ? store.delete(clientId) : undefined,
      async () => (typeof store.remove === 'function') ? store.remove({ session: clientId }) : undefined,
      async () => (typeof store.remove === 'function') ? store.remove(clientId) : undefined,
      async () => (typeof store.deleteSession === 'function') ? store.deleteSession(clientId) : undefined,
      async () => (typeof store.destroy === 'function') ? store.destroy({ session: clientId }) : undefined,
    ];

    for (const fn of candidates) {
      try {
        const r = await fn();
        if (r !== undefined) result.removed = true;
      } catch {}
    }

    // Fallback defensivo para wwebjs-mongo. No rompe si las colecciones no existen.
    try {
      if (mongoose?.connection?.db) {
        for (const collName of ['whatsapp-RemoteAuth', 'whatsapp-remote-auth', 'sessions', 'wwebjs']) {
          try {
            await mongoose.connection.db.collection(collName).deleteMany({
              $or: [
                { session: clientId },
                { _id: clientId },
                { clientId: clientId },
                { sessionName: clientId }
              ]
            });
          } catch {}
        }
      }
    } catch {}

    try { EscribirLog('[CLEAR_AUTH] RemoteAuth limpiado para clientId=' + clientId, 'event'); } catch {}
    result.removed = true;
    return result;
  } catch (e) {
    try { EscribirLog('[CLEAR_AUTH] clearRemoteAuthStoreSafe error: ' + String(e?.message || e), 'error'); } catch {}
    result.error = String(e?.message || e);
    return result;
  }
}


function dirLooksPopulated(p) {
  try {
    if (!fs.existsSync(p)) return false;
    const items = fs.readdirSync(p);
 return Array.isArray(items) && items.length > 0;
  } catch {
    return false;
  }
}

/**
 * Crea el cliente WhatsApp usando el motor configurado en wweb_engine.
 * - wwebjs  : comportamiento histórico con whatsapp-web.js + Chromium/Puppeteer.
 * - baileys : socket WebSocket sin Chromium, manteniendo una interfaz compatible.
 */
async function createClientIfNeeded(opts = {}) {
  if (client) return client;

  // El backend sigue siendo necesario para lock, configuración y panel en ambos motores.
  const ok = await ensureMongo();
  if (!ok) throw new Error("mongo_not_ready");

  if (!tenantId || !numero) throw new Error("tenant_or_numero_missing");

  const clientId = `asisto_${tenantId}_${numero}`;

  const engine = getWwebEngine();

  if (engine === 'baileys') {
    client = new BaileysCompatClient({
      clientId,
      authDir: getBaileysAuthSessionDir(clientId)
    });
    try { EscribirLog(`[WWEB_ENGINE] usando baileys clientId=${clientId} authDir=${getBaileysAuthSessionDir(clientId)}`, 'event'); } catch {}
  } else {
    // Recién acá cargamos whatsapp-web.js/Puppeteer. Las sesiones Baileys nunca
    // pasan por este require ni dependen de Chromium.
    await ensureWwebJsRuntimeLoaded();

    const useRemoteAuth = isRemoteAuthMode();
    if (useRemoteAuth) {
      if (!store) {
        const MongoStoreCtor = await ensureWwebMongoStoreLoaded();
        store = new MongoStoreCtor({ mongoose: getMongooseModule() });
      }
    }

    client = new WwebClient({
      // Con LocalAuth no queremos que whatsapp-web.js borre la carpeta de sesión en auth_failure.
      // Con RemoteAuth sí conviene reiniciar.
      restartOnAuthFail: useRemoteAuth,
      puppeteer: {
        headless: headless,
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-accelerated-2d-canvas',
          '--no-first-run',
          '--no-zygote',
          '--disable-gpu',
          '--disable-features=IsolateOrigins,site-per-process',
          '--disable-site-isolation-trials'
        ],
      },
      authStrategy: useRemoteAuth
        ? new RemoteAuth({
            clientId,
            store,
            backupSyncIntervalMs: Math.max(60_000, Number(backup_every_ms) || 300_000)
          })
        : new LocalAuth({
            clientId,
            dataPath: getAuthBasePath()
          })
    });
    try { client.__transport = 'wwebjs'; } catch {}
    try { EscribirLog(`[WWEB_ENGINE] usando whatsapp-web.js clientId=${clientId} auth_mode=${useRemoteAuth ? 'remote' : 'local'}`, 'event'); } catch {}
  }

  attachClientHandlers();
  return client;
}


/**
 * Envío robusto con reintentos ante errores de evaluación/recarga en WhatsApp Web
 */
async function safeSend(to, content, opts) {
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      // Estado del cliente (CONNECTED/OPENING/etc.). Si falla, seguimos intentando.
      let state = null;
      try { state = await client.getState(); } catch (e) { /* ignore */ }
      if (state !== 'CONNECTED') {
        await sleep(700 * attempt);
      }
       const sendOpts = (opts && typeof opts === 'object') ? { ...opts } : {};
      if (typeof sendOpts.sendSeen === 'undefined') sendOpts.sendSeen = false;
      const sent = await client.sendMessage(to, content, sendOpts);
      try {
        const logPayload = (content && typeof content === 'object')
          ? { body: sendOpts.caption || '', type: content.mimetype ? 'media' : (content.type || 'text'), mimetype: content.mimetype || '', filename: content.filename || '', data: content.data ? '[data]' : '' }
          : { body: String(content || ''), type: 'text', hasMedia: false };
        await logMessageStat('out', to, logPayload);
        rememberOutgoingStatLogged(sent);
      } catch {}
      return sent;
    } catch (e) {
      const msg = String(e && e.message ? e.message : e);
      const transient = msg.includes('Evaluation failed') ||
                        msg.includes('Execution context was destroyed') ||
                        msg.includes('Protocol error') ||
                        msg.includes('baileys_not_connected') ||
                        msg.includes('Connection Closed') ||
                        msg.includes('Timed Out');
      if (!transient || attempt === 3) {
        throw e;
      }
      await sleep(500 * attempt);
    }
  }
}

// =========================
// Lock / lease logic
// =========================
async function updateLockStateSafe(state) {
  try {
    localWsPanelState = String(state || localWsPanelState || 'idle');
    if (!lockId) return;

    const now = new Date();
    const runtimeInfo = getCurrentRuntimeInfo();
    const update = {
      $set: {
        tenantId: tenantId,
        tenantid: tenantId,
        numero: numero,
        holderId: instanceId,
        host: os.hostname(),
        pid: process.pid,
        state: localWsPanelState,
        wwebEngine: getWwebEngine(),
        activeWwebEngine: client?.__transport || '',
        startedAt: lockAcquiredAt || now,
        lastSeenAt: now,
        runtimeVersion: runtimeInfo.currentVersion || '',
        desiredTag: runtimeInfo.desiredTag || '',
        autoUpdateSource: runtimeInfo.autoUpdateSource || '',
        autoUpdateEnabled: !!runtimeInfo.autoUpdateEnabled
      }
    };

    if (state && state !== 'qr') {
      update.$unset = { lastQrAt: "", lastQrDataUrl: "" };
      lastQrAt = null;
      lastQrDataUrl = null;
    }

    if (!await ensureMongo()) return;
    if (!LockModel) return;
    await LockModel.updateOne({ _id: lockId }, update, { upsert: true });
  } catch {}
}

// Guarda el último QR en el lock para poder verlo desde el panel admin (/admin/wweb)
async function updateLockQrDataSafe(qrDataUrl, qrAtIso) {
  try {
    if (qrDataUrl) lastQrDataUrl = String(qrDataUrl);
    if (qrAtIso) lastQrAt = String(qrAtIso);
    localWsPanelState = 'qr';

    if (!lockId) return false;
    if (!await ensureMongo()) return false;
    if (!LockModel) return false;

    const now = new Date();
    const runtimeInfo = getCurrentRuntimeInfo();
    await LockModel.updateOne(
      { _id: lockId },
      {
        $set: {
          tenantId: tenantId,
          tenantid: tenantId,
          numero: numero,
          holderId: instanceId,
          host: os.hostname(),
          pid: process.pid,
          state: 'qr',
          wwebEngine: getWwebEngine(),
          activeWwebEngine: client?.__transport || '',
          startedAt: lockAcquiredAt || now,
          lastSeenAt: now,
          lastQrAt: String(qrAtIso || ""),
          lastQrDataUrl: String(qrDataUrl || ""),
          runtimeVersion: runtimeInfo.currentVersion || '',
          desiredTag: runtimeInfo.desiredTag || '',
          autoUpdateSource: runtimeInfo.autoUpdateSource || '',
          autoUpdateEnabled: !!runtimeInfo.autoUpdateEnabled
        }
      },
      { upsert: true }
    );
    return true;
  } catch (e) {
    try { EscribirLog('[QR] error persistiendo QR en panel: ' + String(e?.message || e), 'error'); } catch {}
    return false;
  }
}

async function waitForQrPersistedAfterClearAuth(timeoutMs = 30000) {
  const started = Date.now();
  const timeout = Math.max(5000, Number(timeoutMs) || 30000);

  while ((Date.now() - started) < timeout) {
    const state = String(localWsPanelState || '').trim().toLowerCase();

    // Si durante la espera la sesión llegó a quedar online, ya no corresponde mostrar QR.
    if (state === 'online' || state === 'authenticated') return false;

    if (state === 'qr' && lastQrDataUrl) {
      const ok = await updateLockQrDataSafe(lastQrDataUrl, lastQrAt || nowArgentinaISO());
      if (ok) {
        try { EscribirLog('[CLEAR_AUTH] QR generado y persistido para el panel', 'event'); } catch {}
        return true;
      }
    }

    await sleep(300);
  }

  try {
    EscribirLog('[CLEAR_AUTH] timeout esperando QR persistido en panel state=' + String(localWsPanelState || '') + ' hasQr=' + String(!!lastQrDataUrl), 'error');
  } catch {}
  return false;
}

// Lock/lease multi-PC removido en modo simplificado.


function clearAuthReadyWatchdog(reason = '') {
  try {
    authReadyWatchdogSeq += 1;
    if (authReadyWatchdogTimer) {
      clearTimeout(authReadyWatchdogTimer);
      authReadyWatchdogTimer = null;
    }
  } catch {}
}

function resetClientRuntimeFlags(reason = '') {
  try { clientStarted = false; } catch {}
  try { startingNow = false; } catch {}
  try { authFailureHandling = false; } catch {}
  try { clearAuthReadyWatchdog(reason); } catch {}
}

function quoteCmdArg(value) {
  const s = String(value ?? '');
  return '"' + s.replace(/"/g, '\\"') + '"';
}

function quoteShArg(value) {
  return "'" + String(value ?? '').replace(/'/g, "'\\''") + "'";
}

function quotePowerShellSingle(value) {
  return "'" + String(value ?? '').replace(/'/g, "''") + "'";
}
function buildRestartCommand(delaySec = 6, parentPid = process.pid) {
  const args = Array.isArray(process.argv) && process.argv.length > 1 ? process.argv.slice(1) : [];
  const waitSeconds = Math.max(1, Number(delaySec) || 6);
  const parent = Math.max(1, Number(parentPid) || process.pid);

  if (process.platform === 'win32') {
    const psArgs = args.map(quotePowerShellSingle).join(', ');
    const restartLog = path.join(process.cwd(), 'logs', 'asisto-restart-helper.log');
    const psCommand = [
      `$env:ASISTO_RESTARTED_FROM_PANEL='1'`,
      `$parent=${parent}`,
      `$log=${quotePowerShellSingle(restartLog)}`,
      `function L($m){ try { Add-Content -LiteralPath $log -Value ((Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ' ' + $m) } catch {} }`,
      `L 'helper_start parent=${parent}'`,
      `$deadline=(Get-Date).AddSeconds(25)`,
      `while ((Get-Date) -lt $deadline -and (Get-Process -Id $parent -ErrorAction SilentlyContinue)) { Start-Sleep -Milliseconds 500 }`,
      `L 'helper_parent_released_or_timeout'`,
      `Start-Sleep -Seconds ${waitSeconds}`,
      `Set-Location -LiteralPath ${quotePowerShellSingle(process.cwd())}`,
      `L 'helper_starting_node'`,
      `& ${quotePowerShellSingle(process.execPath)} ${psArgs}`,
      `L 'helper_node_finished'`
    ].join('; ');

    // Importante en tarea programada Windows:
    // - start desacopla el helper del proceso actual.
    // - El helper espera al PID viejo, pero con timeout máximo para no quedar eterno.
    // - El proceso viejo hace salida rápida para que el helper pueda arrancar Node.
    return {
      command: 'cmd.exe',
      args: ['/d', '/s', '/c', 'start "" /min powershell.exe -NoProfile -ExecutionPolicy Bypass -Command ' + quoteCmdArg(psCommand)]
    };
  }

  const nodeCmd = [quoteShArg(process.execPath), ...args.map(quoteShArg)].join(' ');
  return {
    command: 'sh',
    args: ['-lc', `ASISTO_RESTARTED_FROM_PANEL=1; while kill -0 ${parent} 2>/dev/null; do sleep 0.5; done; sleep ${waitSeconds}; cd ${quoteShArg(process.cwd())}; exec ${nodeCmd}`]
  };
}

async function restartFullProcessFromPanel(reason = 'panel_restart_script') {
  if (fullProcessRestartInFlight) {
   try { EscribirLog('[PROCESS_RESTART] ya hay reinicio de script en curso: ' + String(reason || ''), 'event'); } catch {}
    return false;
  }

  fullProcessRestartInFlight = true;
  restartInFlight = true;

  try {
    const restartReason = String(reason || 'panel_restart_script');
    try { EscribirLog('[PROCESS_RESTART] inicio -> ' + restartReason, 'event'); } catch {}
    try { await updateLockStateSafe('restarting'); } catch {}
   try { await pushHistory('process_restart', { reason: restartReason, pid: process.pid, at: new Date().toISOString() }); } catch {}

   const restartCmd = buildRestartCommand(1, process.pid);
    try { EscribirLog('[PROCESS_RESTART] comando reinicio: ' + restartCmd.command + ' ' + JSON.stringify(restartCmd.args), 'event'); } catch {}
    const child = spawn(restartCmd.command, restartCmd.args, {
      cwd: process.cwd(),
      env: { ...process.env, ASISTO_RESTARTED_FROM_PANEL: '1' },
      detached: true,
      stdio: 'ignore',
      windowsHide: true
    });

    child.on('error', (e) => {
      try { EscribirLog('[PROCESS_RESTART] spawn error: ' + String(e?.message || e), 'error'); } catch {}
    });

    try { child.unref(); } catch {}
    try { EscribirLog('[PROCESS_RESTART] nuevo proceso programado; cerrando proceso actual pid=' + process.pid, 'event'); } catch {}

    setTimeout(async () => {
      // Cierre con timeout: evita dejar Chromium huérfano pero tampoco permite que
      // Puppeteer bloquee indefinidamente el reinicio.
      try { EscribirLog('[PROCESS_RESTART] preparando salida del proceso actual pid=' + process.pid, 'event'); } catch {}
      clearRuntimeTimersForExit('process_restart_full');
      try { await closeWhatsappClientForProcessExit(client, 'process_restart_full', 2200); } catch {}
      try { client = null; } catch {}
      try { resetClientRuntimeFlags('process_restart_full'); } catch {}
      try { localWsPanelState = 'restarting'; } catch {}
      try { releaseSingleInstanceLock(); } catch {}
      try { isOwner = false; } catch {}
      try { process.exit(0); } catch {}
    }, 250);

    return true;
  } catch (e) {
    fullProcessRestartInFlight = false;
    restartInFlight = false;
    try { EscribirLog('[PROCESS_RESTART] error: ' + String(e?.message || e), 'error'); } catch {}
    return false;
  }
}


async function restartClientSession(reason = 'restart', waitMs = 6500) {
  if (restartInFlight) {
    try { EscribirLog('[RESTART] ya hay un reinicio en curso: ' + String(reason || ''), 'event'); } catch {}
    return false;
  }

  restartInFlight = true;
  const delay = Math.max(3500, Number(waitMs) || 6500);

 try {
    try { EscribirLog('[RESTART] inicio -> ' + String(reason || ''), 'event'); } catch {}
    try { await updateLockStateSafe('restarting'); } catch {}

    clearAuthReadyWatchdog('restart:' + String(reason || ''));

    try {
      if (client && typeof destroyClientHard === 'function') await destroyClientHard(client);
      else if (client) await client.destroy();
    } catch (e) {
      try { EscribirLog('[RESTART] destroy error: ' + String(e?.message || e), 'error'); } catch {}
    }

    try { client = null; } catch {}
    resetClientRuntimeFlags('restart:' + String(reason || ''));

    await sleep(delay);

    if (!isOwner) {
      try { EscribirLog('[RESTART] cancelado porque la instancia ya no es owner', 'event'); } catch {}
      return false;
    }

    const restartReason = String(reason || '');
    const skipVersionCheck = restartReason.startsWith('panel_restart:') || restartReason.includes('phone_web_restart');
    await startClientInitialize({
      source: restartReason,
      skipVersionCheck
    });

    return true;
  } catch (e) {
   try { EscribirLog('[RESTART] error: ' + String(e?.message || e), 'error'); } catch {}
    return false;
  } finally {
    restartInFlight = false;
  }
}

function armAuthReadyWatchdog(source = 'authenticated', waitMs = AUTH_READY_WATCHDOG_MS) {
  clearAuthReadyWatchdog('rearm:' + String(source || ''));
  const seq = ++authReadyWatchdogSeq;
  const delay = Math.max(30000, Number(waitMs) || AUTH_READY_WATCHDOG_MS);

  authReadyWatchdogTimer = setTimeout(async () => {
    try {
      if (seq !== authReadyWatchdogSeq) return;
      authReadyWatchdogTimer = null;

      if (!isOwner) return;
      if (localWsPanelState === 'online') return;
      if (restartInFlight) return;

      const currentState = String(localWsPanelState || '');
      if (currentState !== 'authenticated' && currentState !== 'starting' && currentState !== 'restarting') return;

      try { EscribirLog('[WATCHDOG] autenticado sin ready -> reiniciando (' + String(source || '') + ')', 'event'); } catch {}
      await restartClientSession('watchdog_' + String(source || ''), 7000);
    } catch (e) {
      try { EscribirLog('[WATCHDOG] error: ' + String(e?.message || e), 'error'); } catch {}
    }
  }, delay);
}


async function ensureTenantVersionBeforeWhatsAppStart(reason = 'before_whatsapp_start') {
  try {
    await loadTenantConfigFromDbMinimal();
  } catch (e) {
    try { EscribirLog('ensureTenantVersionBeforeWhatsAppStart config error: ' + String(e?.message || e), 'error'); } catch {}
  }

  const desiredTag = String(auto_update_target_tag || '').trim();
  if (!desiredTag) return { checked: false, desiredTag: '', changed: false, restartScheduled: false };

  try {
    const changed = await autoUpdateForceTargetTagOnBoot(reason);
    return {
      checked: true,
      desiredTag,
      changed: !!changed,
     restartScheduled: !!autoUpdateRestarting
    };
  } catch (e) {
    const detail = String(e?.stderr || e?.stdout || e?.message || e || '').trim();
    try { EscribirLog('ensureTenantVersionBeforeWhatsAppStart update error: ' + (detail || String(e?.message || e)), 'error'); } catch {}
    throw e;
  }
}


async function startClientInitialize(options = {}) {
  const initOptions = options && typeof options === 'object' ? options : {};
  const skipVersionCheck = initOptions.skipVersionCheck === true;
  const initSource = String(initOptions.source || '');

  // Inicializa WhatsApp SOLO si esta instancia es dueña del lock.
  if (clientStarted) return;
  if (!isOwner) return;
  // Guard temprano: evita dobles initialize() cuando se dispara reinicio,
  // heartbeat/watchdog o poll de acciones casi al mismo tiempo.
  if (startingNow) {
    try { EscribirLog('[INIT] skip: ya hay inicialización en curso', 'event'); } catch {}
    return;
  }

  startingNow = true;
  clearAuthReadyWatchdog('before_initialize');

   try {
    // Antes de cada inicio real del cliente, refrescamos tenant_config y
    // verificamos si la versión/tag objetivo cambió en Mongo.
    // Si el inicio viene de Reiniciar desde el panel, NO forzamos auto-update acá:
    // ese update puede ejecutar npm install y dejar la sesión en 'restarting'.
    // El auto-update normal por startup/interval sigue funcionando fuera de este flujo.
    if (skipVersionCheck) {
      try { EscribirLog('[AUTO_UPDATE] skip before_whatsapp_start por reinicio desde panel: ' + (initSource || 'manual_restart'), 'event'); } catch {}
      try { await loadTenantConfigFromDbMinimal(); } catch {}
    } else {
      try {
        const versionCheck = await ensureTenantVersionBeforeWhatsAppStart('before_whatsapp_start');
        if (versionCheck?.restartScheduled) {
          try { EscribirLog('[AUTO_UPDATE] reinicio programado antes de iniciar WhatsApp. Se cancela init actual.', 'event'); } catch {}
          return;
        }
      } catch (e) {
        const detail = String(e?.stderr || e?.stdout || e?.message || e || '').trim();
        console.log("Chequeo de versión antes de iniciar WhatsApp falló:", detail || (e?.message || e));
        EscribirLog("Chequeo de versión antes de iniciar WhatsApp falló: " + (detail || String(e?.message || e)), "error");
        try {
          EscribirLog('[AUTO_UPDATE] el chequeo de versión falló; se continúa iniciando WhatsApp con la versión instalada.', 'event');
        } catch {}
        // Un fallo de Git/auto-update NO debe dejar la sesión clavada en "starting".
        // Continuamos con createClientIfNeeded()/initializeWithRetry usando el código instalado.
        return;
      }
    
    }
    // Política: si está deshabilitado desde el panel, NO inicializamos WhatsApp.
    try {
      const pol = await getPolicySafe();
      if (pol && pol.disabled === true) {
        lastPolicyDisabled = true;
        await updateLockStateSafe("disabled");
        await pushHistory("policy_disabled", { by: "policy", disabled: true });
        return;
      }
      if (pol && pol.disabled === false) lastPolicyDisabled = false;
    } catch {}

  try {
    await createClientIfNeeded();
  } catch (e) {
    clientStarted = false;
    console.log("No se pudo crear cliente WhatsApp:", e?.message || e);
    EscribirLog("No se pudo crear cliente WhatsApp: " + String(e?.message || e), "error");
    startingNow = false;
    return;
  }

    console.log("LOCK OK -> inicializando WhatsApp...");
    pushHistory('lock_acquired', { holderId: instanceId, host: os.hostname() }).catch(()=>{});
    EscribirLog("LOCK OK -> inicializando WhatsApp...", "event");
    updateLockStateSafe("starting").catch(() => {});

    try {
      await initializeWithRetry(client, 5);
      clientStarted = true;
    } catch (e) {
      clientStarted = false;
      console.log("Error al inicializar WhatsApp:", e?.message || e);
      EscribirLog("Error al inicializar WhatsApp: " + String(e?.message || e), "error");

      // Este error aparece cuando el poll intenta inicializar 2 veces y el Chrome anterior sigue vivo
      const msg = String(e?.message || e || "");
      if (msg.includes("browser is already running")) {
        console.log("TIP: Se detectó un Chrome ya corriendo para este userDataDir. Revisá que no haya dos instancias del script abiertas.");
        EscribirLog("TIP: Se detectó un Chrome ya corriendo para este userDataDir. Evitar doble instancia.", "error");
      }

      // Si la inicialización falla, limpiamos fuerte para permitir reintentos limpios
      try { await destroyClientHard(client); } catch {}
      try { client = null; } catch {}
      clearAuthReadyWatchdog('initialize_error');
    }

  } finally {
    startingNow = false;
  }
}
async function bootstrapWithLock() {
  // Modo simplificado:
  // - NO usa standby
  // - NO espera takeover de otra PC
  // - inicia WhatsApp apenas corre el script
  try {
    lockId = `${tenantId}:${numero}`;
    isOwner = true;
    if (!lockAcquiredAt) lockAcquiredAt = new Date();

    try { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } } catch {}
    try { if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; } } catch {}
    try { if (actionTimer) { clearInterval(actionTimer); actionTimer = null; } } catch {}

    await updateLockStateSafe('starting');
    startHeartbeat();
    startActionPoller();

    console.log("Inicio directo sin standby -> inicializando WhatsApp...");
    EscribirLog("Inicio directo sin standby -> inicializando WhatsApp...", "event");

    await startClientInitialize();
    return true;
  } catch (e) {
    console.log("bootstrap directo error:", e?.message || e);
    EscribirLog("bootstrap directo error: " + String(e?.message || e), "error");
    return false;
  }
}


async function forceReleaseLock(finalState) {
  const st = String(finalState || 'offline');
  try {
    if (!await ensureMongo()) return;
    if (!lockId || !LockModel) return;

    const runtimeInfo = getCurrentRuntimeInfo();
    await LockModel.updateOne(
      { _id: lockId },
      {
        $set: {
          tenantId,
          tenantid: tenantId,
          numero,
          holderId: instanceId,
          host: os.hostname(),
          pid: process.pid,
          state: st,
          lastSeenAt: new Date(),
          releasedAt: new Date(),
          releasedBy: instanceId,
          runtimeVersion: runtimeInfo.currentVersion || '',
          desiredTag: runtimeInfo.desiredTag || '',
          autoUpdateSource: runtimeInfo.autoUpdateSource || '',
          autoUpdateEnabled: !!runtimeInfo.autoUpdateEnabled
        }
      },
      { upsert: true }
    );
  } catch (e) {
    try { EscribirLog('forceReleaseLock error: ' + String(e?.message || e), 'error'); } catch {}
  }
}

async function clearAuthenticationAndRequestQr(reason = 'clear_auth') {
  if (clearAuthInFlight) {
    try { EscribirLog('[CLEAR_AUTH] ya hay un borrado en curso: ' + String(reason || ''), 'event'); } catch {}
    return false;
  }

  clearAuthInFlight = true;
  const clientId = getWwebClientId();

  try {
    try { EscribirLog('[CLEAR_AUTH] inicio -> ' + String(reason || '') + ' clientId=' + clientId, 'event'); } catch {}
    try { await updateLockStateSafe('restarting'); } catch {}
    clearAuthReadyWatchdog('clear_auth');

    // 1) Intentar logout para que WhatsApp invalide la sesión.
    // Si falla por navegador roto, igual seguimos y borramos archivos/local store.
    try {
      if (client && typeof client.logout === 'function') await client.logout();
    } catch (e) {
      try { EscribirLog('[CLEAR_AUTH] logout error/skip: ' + String(e?.message || e), 'event'); } catch {}
    }

    // 2) Cerrar Chromium/cliente para liberar locks de archivos.
    try {
      if (client && typeof destroyClientHard === 'function') await destroyClientHard(client);
      else if (client) await client.destroy();
    } catch (e) {
      try { EscribirLog('[CLEAR_AUTH] destroy error: ' + String(e?.message || e), 'error'); } catch {}
    }

    try { client = null; } catch {}
    resetClientRuntimeFlags('clear_auth');
    localWsPanelState = 'starting';

    // 3) Borrar autenticación real según motor/modo.
    let clearResult;
    let authModeLabel;
    if (isBaileysEngine()) {
      clearResult = await clearBaileysAuthFilesSafe(clientId);
      authModeLabel = 'baileys-local';
    } else if (isRemoteAuthMode()) {
      clearResult = await clearRemoteAuthStoreSafe(clientId);
      authModeLabel = 'remote';
    } else {
      clearResult = await clearLocalAuthFilesSafe(clientId);
      authModeLabel = 'local';
    }

    try { await pushHistory('clear_auth', { reason, clientId, engine: getWwebEngine(), authMode: authModeLabel, result: clearResult }); } catch {}

    // 4) Mantener el lock/owner y reiniciar WhatsApp para que vuelva a emitir QR.
    isOwner = true;
    if (!lockAcquiredAt) lockAcquiredAt = new Date();
    await updateLockStateSafe('starting');
    await sleep(1500);
    await startClientInitialize();

    // Baileys initialize() retorna apenas crea el socket; el QR llega después.
    // Esperamos a que el QR tenga dataURL y quede persistido en wa_locks para que
    // el botón QR del panel se habilite sin necesitar un segundo "Reiniciar".
    if (isBaileysEngine()) {
      await waitForQrPersistedAfterClearAuth(30000);
    }
    return true;
  } catch (e) {
    try { EscribirLog('[CLEAR_AUTH] error: ' + String(e?.message || e), 'error'); } catch {}
    return false;
  } finally {
    clearAuthInFlight = false;
  }
}

async function restartScriptFromPanel(reason = 'panel_restart_script') {
  if (restartInFlight) {
    try { EscribirLog('[PROCESS_RESTART] ya hay reinicio en curso: ' + String(reason || ''), 'event'); } catch {}
    return false;
  }

  restartInFlight = true;

  try {
    const restartReason = String(reason || 'panel_restart_script');
    const restartMode = getPanelRestartMode();
    const defaultExitCode = restartMode === 'pm2' ? 0 : 77;
    const exitCode = Number(process.env.ASISTO_PANEL_RESTART_EXIT_CODE || defaultExitCode);

    try { EscribirLog('[PROCESS_RESTART] inicio -> ' + restartReason, 'event'); } catch {}
    try { await pushHistory('process_restart', { reason: restartReason, pid: process.pid, exitCode, mode: restartMode, at: new Date().toISOString() }); } catch {}
    // En modo multi-sesión un "Reiniciar" del panel reinicia app_asisto_ws COMPLETO,
    // porque supervisor y sesiones viven en el mismo proceso Node.
    // El supervisor coordina el cierre limpio de todos los transports antes de salir.
    if (ASISTO_MULTI_WORKER) {
      try { localWsPanelState = 'restarting'; } catch {}
      try { await updateLockStateSafe('restarting'); } catch {}
      try {
        const sent = sendMultiSupervisorMessage({
          type: 'multi_global_restart_request',
          reason: restartReason,
          key: ASISTO_MULTI_SESSION_KEY
        });
        if (!sent) throw new Error('multi_supervisor_ipc_not_available');
        try { EscribirLog('[PROCESS_RESTART] reinicio global solicitado al supervisor multi-sesión', 'event'); } catch {}
        return true;
      } catch (e) {
        restartInFlight = false;
        try { EscribirLog('[PROCESS_RESTART] no se pudo solicitar reinicio global: ' + String(e?.message || e), 'error'); } catch {}
        return false;
      }
    }

        // IMPORTANTE:
    // - task_runner: no lanzamos otro node.exe desde este proceso; salimos con exitCode=77
    //   y asisto_ws_runner.cmd lo vuelve a iniciar.
    // - pm2: salimos con exitCode=0 para que PM2 reinicie el proceso.
    try { localWsPanelState = 'restarting'; } catch {}
    try { await updateLockStateSafe('restarting'); } catch {}

    // El botón Reiniciar del panel llegaba hasta acá y salía sin cerrar el Client.
    // Con whatsapp-web.js eso podía dejar chrome.exe huérfano. Intentamos un cierre
    // normal por un tiempo corto y, si Puppeteer no responde, terminamos únicamente
    // el Chromium perteneciente a esta sesión antes de salir.
    clearRuntimeTimersForExit('panel_restart');
    try { await closeWhatsappClientForProcessExit(client, 'panel_restart', 2200); } catch {}
    try { client = null; } catch {}
    try { resetClientRuntimeFlags('panel_restart'); } catch {}

    try { await forceReleaseLock('restarting'); } catch {}
    try { await Promise.race([disconnectMongoSafe('panel_restart'), timeoutPromise(3000, 'mongo_disconnect_timeout')]); } catch {}
    try { releaseSingleInstanceLock(); } catch {}



    try { EscribirLog('[PROCESS_RESTART] modo=' + restartMode + ' saliendo con exitCode=' + exitCode + ' pid=' + process.pid, 'event'); } catch {}
    setTimeout(() => {
      try { process.exit(exitCode); } catch {}
    }, 250);

    return true;
  } catch (e) {
    restartInFlight = false;
    try { EscribirLog('[PROCESS_RESTART] error: ' + String(e?.message || e), 'error'); } catch {}
    return false;
  }
}



async function handleActionDoc(doc) {
  const action = String(doc?.action || '').toLowerCase();
  const reason = String(doc?.reason || '');
  const reasonLower = reason.toLowerCase();
  const isPanelRestartButton = reasonLower.includes('phone_web_restart') || reasonLower.includes('panel_restart');

  try {
    // El botón Reiniciar del panel debe reiniciar TODO el script Node.
    // Compatibilidad: si el panel todavía envía restart_whatsapp/restart_wweb
    // con reason=phone_web_restart, igual lo tratamos como reinicio completo.
    if (action === 'restart' || action === 'restart_script' || action === 'full_restart') {
      const restartMode = getPanelRestartMode();
      if (restartMode === 'whatsapp') {
        EscribirLog('Accion RESTART WHATSAPP recibida por modo=whatsapp: action=' + action + ' reason=' + reason, 'event');
        const ok = await restartClientSession('panel_restart_whatsapp:' + reason, 7000);
        return ok ? 'whatsapp_restarted' : 'whatsapp_restart_skipped';
      }
      EscribirLog('Accion RESTART SCRIPT recibida: action=' + action + ' reason=' + reason, 'event');
      const ok = await restartScriptFromPanel('panel_restart:' + (reason || action));
      return ok ? 'script_restart_exit_scheduled' : 'script_restart_skipped';
    }

    if (action === 'restart_whatsapp' || action === 'restart_wweb') {
      EscribirLog('Accion RESTART WHATSAPP recibida: action=' + action + ' reason=' + reason, 'event');
      const ok = await restartClientSession('panel_restart_whatsapp:' + reason, 7000);
      return ok ? 'whatsapp_restarted' : 'whatsapp_restart_skipped';
    }

    if (['send_message', 'send_text', 'admin_send_message', 'panel_send_message'].includes(action)) {
      const payload = (doc && doc.payload && typeof doc.payload === 'object') ? doc.payload : {};
      const toRaw = String(doc?.to || doc?.waId || payload.to || payload.waId || '').trim();
      const text = String(doc?.text || doc?.body || doc?.message || payload.text || payload.body || payload.message || '').trim();
      const target = normalizeWwebTargetChatId(toRaw);
      if (!target || !text) {
        EscribirLog('Accion SEND_MESSAGE incompleta: to=' + toRaw + ' textLen=' + text.length, 'error');
        return 'send_message_missing_to_or_text';
      }
      EscribirLog('Accion SEND_MESSAGE recibida: to=' + target + ' len=' + text.length, 'event');
      await safeSendMessage(target, text, { sendSeen: false });
      return 'message_sent';
    }



    if (action === 'release') {
      EscribirLog('Accion RELEASE recibida: ' + reason, 'event');
      try {
        if (client && typeof destroyClientHard === "function") await destroyClientHard(client);
        else if (client) await client.destroy();
      } catch {}
      try { client = null; } catch {}
      resetClientRuntimeFlags('release');
      localWsPanelState = 'offline';
      await forceReleaseLock('offline');
      isOwner = false;
      return 'released';
    }

    if (['pause', 'pausar', 'pause_messages', 'block_messages'].includes(action)) {
      EscribirLog('Accion PAUSA recibida: ' + reason, 'event');
      try { console.log('Accion PAUSA recibida: ' + reason); } catch {}
      lastPolicyBlocked = true;
      localWsPanelState = 'paused';
      try { await updateLockStateSafe('paused'); } catch {}
      return 'paused';
    }
    if (['resume', 'reanudar', 'resume_messages', 'unblock_messages', 'enable', 'habilitar'].includes(action)) {
      EscribirLog('Accion REANUDAR recibida: ' + reason, 'event');
      try { console.log('Accion REANUDAR recibida: ' + reason); } catch {}
      lastPolicyBlocked = false;
      if (client && client.info && client.info.me && client.info.me.user) {
        localWsPanelState = 'online';
        try { await updateLockStateSafe('online'); } catch {}
        try { startConsultaApiMensajesIfEnabled('resume'); } catch {}
      } else {
        localWsPanelState = 'starting';
        try { await updateLockStateSafe('starting'); } catch {}
      }
      return 'resumed';
    }


    if ([
      'resetauth',
      'reset_auth',
      'clear_auth',
      'delete_auth',
      'borrar_auth',
      'borrar_autenticacion',
     'nuevo_qr'
    ].includes(action)) {
      EscribirLog('Accion CLEAR AUTH recibida: ' + reason, 'event');
      const ok = await clearAuthenticationAndRequestQr(reason || action);
      return ok ? 'clear_auth_requested' : 'clear_auth_failed';
    }
    

    return 'ignored';
  } catch (e) {
    try { EscribirLog('Error manejando accion ' + action + ': ' + String(e?.message || e), 'error'); } catch {}
    return 'error';
  }
}

async function pollActionsOnce() {
  if (actionBusy) return;
  if (!isOwner) return;
  if (!lockId) return;
  if (!await ensureMongo()) return;
  if (!ActionModel) return;

  actionBusy = true;
  try {
    const doc = await ActionModel.findOneAndUpdate(
      { lockId, doneAt: { $exists: false } },
      { $set: { doneAt: new Date(), doneBy: instanceId } },
      { sort: { requestedAt: 1 }, returnDocument: 'after' }
    ).lean();

    if (!doc) return;

    try {
      const reqAt = doc.requestedAt ? new Date(doc.requestedAt) : null;
      if (lockAcquiredAt && reqAt && reqAt.getTime() < lockAcquiredAt.getTime()) {
        await ActionModel.updateOne({ _id: doc._id }, { $set: { result: 'stale_ignored' } });
        return;
      }
    } catch {}

    const result = await handleActionDoc(doc);
    await ActionModel.updateOne({ _id: doc._id }, { $set: { result } });
  } catch (e) {
    try { EscribirLog('pollActionsOnce error: ' + String(e?.message || e), 'error'); } catch {}
  } finally {
    actionBusy = false;
  }
}

function startActionPoller() {
  try { if (actionTimer) { clearInterval(actionTimer); actionTimer = null; } } catch {}
  actionTimer = setInterval(() => {
    pollActionsOnce().catch(() => {});
  }, 4000);
}



async function gracefulShutdown(signal) {
  if (gracefulShutdown.inFlight) return;
  gracefulShutdown.inFlight = true;

  if (multiSessionSupervisorState && !ASISTO_MULTI_WORKER) {
    try { console.log(`[MULTI] shutdown supervisor signal=${signal}`); } catch {}
    try { await stopMultiSessionSupervisor(String(signal || 'shutdown')); } catch {}
    try { releaseSingleInstanceLock(); } catch {}
    process.exit(0);
    return;
  }


  if (String(signal || '').startsWith('AUTO_UPDATE')) {
    return fastExitForSupervisorRestart(signal);
  }
  try { sessionLog(`[SHUTDOWN] ${signal} -> cerrando WhatsApp...`); } catch {}
  try { if (autoUpdateTimer) { clearInterval(autoUpdateTimer); autoUpdateTimer = null; } } catch {}
  try { if (runtimeConfigPollTimer) { clearInterval(runtimeConfigPollTimer); runtimeConfigPollTimer = null; } } catch {}
  try { if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; } } catch {}
  try { if (actionTimer) { clearInterval(actionTimer); actionTimer = null; } } catch {}
  try { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } } catch {}
  try { clearAuthReadyWatchdog('shutdown'); } catch {}
  try { await closeWhatsappClientForProcessExit(client, 'shutdown:' + String(signal || ''), 3000); } catch {}
  try { client = null; } catch {}
  try { resetClientRuntimeFlags('shutdown'); } catch {}
  try { localWsPanelState = 'offline'; } catch {}
  try { await updateLockStateSafe('offline'); } catch {}
  try { await forceReleaseLock('offline'); } catch {}
  try { isOwner = false; } catch {}

  process.exit(0);

}
process.on('unhandledRejection', (reason) => {
  try {
    const msg = '[FATAL] unhandledRejection: ' + String(reason?.stack || reason?.message || reason);
    console.error(msg);
    EscribirLog(msg, 'error');
  } catch {}
  fastExitForSupervisorRestart('FATAL_UNHANDLED_REJECTION').catch(() => { try { process.exit(getSupervisorRestartExitCode()); } catch {} });
});

process.on('uncaughtException', (err) => {
  try {
    const msg = '[FATAL] uncaughtException: ' + String(err?.stack || err?.message || err);
    console.error(msg);
    EscribirLog(msg, 'error');
  } catch {}
  fastExitForSupervisorRestart('FATAL_UNCAUGHT_EXCEPTION').catch(() => { try { process.exit(getSupervisorRestartExitCode()); } catch {} });
});

process.on("SIGINT", () => { gracefulShutdown("SIGINT"); });
process.on("SIGTERM", () => { gracefulShutdown("SIGTERM"); });
// Windows: cerrar consola / Ctrl+Break
process.on("SIGBREAK", () => { gracefulShutdown("SIGBREAK"); });




////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Horario de funcionamiento para ConsultaApiMensajes.
// Usa el mismo documento que el panel existente de horarios:
// settings._id = "store_hours:<tenantId>", campo hours = { monday:[{from,to}], ... }.
// Si no hay horarios cargados, mantiene el comportamiento anterior: consulta habilitada todo el día.
const CONSULTA_MENSAJES_DAY_KEYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"];

function _consultaMensajesHHMMToMinutes(value) {
  const m = /^([01]\d|2[0-3]):([0-5]\d)$/.exec(String(value || '').trim());
  if (!m) return null;
  return Number(m[1]) * 60 + Number(m[2]);
}

function normalizeConsultaMensajesHoursPayload(raw) {
  const out = {};
  try {
    const src = raw && typeof raw === 'object' ? raw : {};
    for (const day of CONSULTA_MENSAJES_DAY_KEYS) {
      const ranges = Array.isArray(src[day]) ? src[day] : [];
      const norm = [];
      for (const r of ranges) {
        if (!r || typeof r !== 'object') continue;
        const from = String(r.from ?? r.desde ?? '').trim();
        const to = String(r.to ?? r.hasta ?? '').trim();
        const fromM = _consultaMensajesHHMMToMinutes(from);
        const toM = _consultaMensajesHHMMToMinutes(to);
        if (fromM == null || toM == null || fromM >= toM) continue;
        norm.push({ from, to, fromM, toM });
        if (norm.length >= 2) break;
      }
      if (norm.length) out[day] = norm;
    }
  } catch {}
  return out;
}

function _consultaMensajesHasAnyHours(hours) {
  try {
    return !!(hours && CONSULTA_MENSAJES_DAY_KEYS.some((d) => Array.isArray(hours[d]) && hours[d].length > 0));
  } catch {
    return false;
  }
}

async function loadConsultaMensajesHoursFromDb(force = false) {
  try {
    if (consulta_mensajes_respetar_horarios !== true) return null;
    const now = Date.now();
    if (!force && consultaMensajesHoursCache.expiresAt > now) return consultaMensajesHoursCache.hours;

    if (!tenantId || !await ensureMongo() || !dataBackendReady()) {
      consultaMensajesHoursCache = { expiresAt: now + 30000, hours: null, updatedAt: null };
      return null;
    }

    const tenant = String(tenantId || '').trim();
    const coll = getDataCollection('settings');
    let doc = await coll.findOne({ _id: `store_hours:${tenant}` });
    if (!doc) doc = await coll.findOne({ tenantId: tenant, _id: /^store_hours:/ });

    const hours = normalizeConsultaMensajesHoursPayload(doc?.hours || {});
    consultaMensajesHoursCache = {
      expiresAt: now + 30000,
      hours: _consultaMensajesHasAnyHours(hours) ? hours : null,
      updatedAt: doc?.updatedAt || null
    };
    return consultaMensajesHoursCache.hours;
  } catch (e) {
    try { EscribirLog('loadConsultaMensajesHoursFromDb error: ' + String(e?.message || e), 'error'); } catch {}
    return null;
  }
}

function getConsultaMensajesNowArgentinaParts(date = new Date()) {
  try {
    const dayKey = new Intl.DateTimeFormat('en-US', { timeZone: AR_TZ, weekday: 'long' }).format(date).toLowerCase();
    const parts = new Intl.DateTimeFormat('sv-SE', {
      timeZone: AR_TZ,
      hour: '2-digit',
      minute: '2-digit',
      hour12: false
    }).formatToParts(date);
    const map = {};
    for (const p of parts || []) {
      if (p && p.type) map[p.type] = p.value;
    }
    const hh = String(map.hour || '00').padStart(2, '0');
    const mm = String(map.minute || '00').padStart(2, '0');
    return { dayKey, hhmm: `${hh}:${mm}`, minutes: Number(hh) * 60 + Number(mm) };
  } catch {
    const d = date || new Date();
    return { dayKey: '', hhmm: '', minutes: d.getHours() * 60 + d.getMinutes() };
  }
}

async function getConsultaMensajesScheduleStatus() {
  if (consulta_mensajes_respetar_horarios !== true) {
    return { allowed: true, reason: 'schedule_disabled' };
  }

  const hours = await loadConsultaMensajesHoursFromDb();
  if (!_consultaMensajesHasAnyHours(hours)) {
    return { allowed: true, reason: 'no_hours_configured' };
  }

  const now = getConsultaMensajesNowArgentinaParts();
  const ranges = Array.isArray(hours[now.dayKey]) ? hours[now.dayKey] : [];
  const slots = ranges.map((r) => `${r.from}-${r.to}`).join(', ');

  if (!ranges.length) {
    return { allowed: false, reason: 'day_closed', dayKey: now.dayKey, hhmm: now.hhmm, slots: '' };
  }

  const inside = ranges.some((r) => now.minutes >= r.fromM && now.minutes <= r.toM);
  return {
    allowed: inside,
    reason: inside ? 'inside_range' : 'outside_range',
    dayKey: now.dayKey,
    hhmm: now.hhmm,
    slots
  };
}

function logConsultaMensajesScheduleStatus(status) {
  try {
    if (!status || status.reason === 'no_hours_configured' || status.reason === 'schedule_disabled') return;
    const key = status.allowed
      ? `open:${status.dayKey}:${status.slots || ''}`
      : `closed:${status.reason}:${status.dayKey}:${status.slots || ''}`;
    if (key === lastConsultaMensajesHorarioLogKey) return;
    lastConsultaMensajesHorarioLogKey = key;

    const msg = status.allowed
      ? `ConsultaApiMensajes dentro de horario (${status.dayKey} ${status.hhmm}, franjas: ${status.slots || '-'})`
      : `ConsultaApiMensajes fuera de horario (${status.reason}, ${status.dayKey || '-'} ${status.hhmm || '-'}, franjas: ${status.slots || '-'})`;
    console.log(msg);
    EscribirLog(msg, 'event');
  } catch {}
}

async function sleepConsultaMensajesFueraDeHorario() {
  const waitMs = Math.max(5000, Number(consulta_mensajes_fuera_horario_sleep_ms) || 60000);
  await sleep(waitMs);
}

function apiMensajesConfirmacionCollection() {
  try {
    if (!dataBackendReady()) return null;
    return getDataCollection('wa_api_mensajes_confirmaciones');
  } catch {
    return null;
  }
}

function apiMensajesConfirmacionId(nroTel) {
  const t = String(tenantId || 'DEFAULT').trim().toUpperCase();
  const from = getApiMensajesNroTelFrom();
  const to = onlyDigits(nroTel || '');
  return `${t}:${from}:${to}`;
}

function apiMensajesConfirmacionTenantId() {
  return String(tenantId || '').trim().toUpperCase();
}

function apiMensajesConfirmacionNumeroFrom() {
  return getApiMensajesNroTelFrom();
}

function addUniquePhoneCandidate(list, value) {
  const phone = onlyDigits(value || '');
 if (!phone) return;
  if (!list.includes(phone)) list.push(phone);
}

async function phoneCandidatesConfirmacionApiMensajes(message) {
  const out = [];
  try {
    const resolved = onlyDigits(await resolvePhoneFromIncomingMessage(message));
    addUniquePhoneCandidate(out, resolved);
  } catch {}

 try {
    const rawFrom = String(message?.from || '').trim();
    if (rawFrom && rawFrom !== 'status@broadcast' && !looksLikeLid(rawFrom)) {
      addUniquePhoneCandidate(out, stripWhatsappSuffix(rawFrom));
    }
  } catch {}

  try {
    if (typeof message?.getContact === 'function') {
      const c = await message.getContact();
      addUniquePhoneCandidate(out, c?.number || '');
      addUniquePhoneCandidate(out, c?.id?.user || '');
    }
  } catch {}

  return out;
}

function queryConfirmacionApiMensajesByPhones(phoneCandidates) {
  const phones = Array.isArray(phoneCandidates) ? phoneCandidates.map(onlyDigits).filter(Boolean) : [];
  const ids = phones.map(apiMensajesConfirmacionId);
  const ors = [];
  if (ids.length) ors.push({ _id: { $in: ids } });
  if (phones.length) {
    ors.push({
      tenantId: apiMensajesConfirmacionTenantId(),
      numeroFrom: apiMensajesConfirmacionNumeroFrom(),
      nroTel: { $in: phones }
    });
  }
  return ors.length ? { $or: ors } : null;
}

function getOutgoingConfirmacionTargetRaw(message) {
  try {
    const own = onlyDigits(telefono_qr || numero || '');
    const candidates = [
      message?.to,
      message?._data?.to,
      message?._data?.id?.remote,
      message?.id?.remote,
      message?._data?.chatId,
      message?._data?.remote,
      message?.from,
      message?._data?.from
    ];

    for (const raw of candidates) {
      const v = String(raw || '').trim();
      if (!v || v === 'status@broadcast') continue;
      const digits = onlyDigits(stripWhatsappSuffix(v));
      if (digits && own && digits === own) continue;
      if (digits || looksLikeLid(v) || v.endsWith('@c.us')) return v;
    }
  } catch {}
  return '';
}

// Sincroniza con Render los mensajes que un operador envía directamente desde
// el mismo WhatsApp/telefono vinculado. Así, para el bot de pedidos, un importe
// informado desde el teléfono tiene el mismo efecto que escribirlo desde el panel.
// Los mensajes enviados por el propio bot/panel también pueden disparar
// message_create; el servidor los deduplica contra la conversación persistida.
async function notifyWwebOperatorOutgoingMessage(message) {
  try {
    if (!message || message.fromMe !== true) return false;

    const body = getMessageBodyText(message);
    if (!body) return false;

    const ownPhone = onlyDigits(telefono_qr || numero || client?.info?.me?.user || '');
    if (!ownPhone || !tenantId) return false;

    const logicMode = await getWwebBotLogicModeForPhone(ownPhone);
    if (logicMode !== 'chatgpt') return false;

    if (!controlApi?.isConfigured?.()) {
      try { EscribirLog('[WWEB_OPERATOR] Control API no configurada; no se sincroniza mensaje saliente', 'error'); } catch {}
      return false;
    }

    const targetRaw = getOutgoingConfirmacionTargetRaw(message);
    if (!targetRaw) return false;

    const customerPhone = onlyDigits(await normalizeContactForStats(targetRaw));
    if (!customerPhone) {
      try { EscribirLog('[WWEB_OPERATOR] no se pudo resolver teléfono destino raw=' + String(targetRaw || ''), 'error'); } catch {}
      return false;
    }

    const messageId = getMessageStableId(message);
    const result = await controlApi.request('/operator-message', {
      Tel_Origen: customerPhone,
      Tel_Destino: ownPhone,
      Mensaje: body,
      MessageId: messageId,
      source: 'message_create_fromMe'
    });

    try {
      const msg = '[WWEB_OPERATOR] sincronizado to=' + customerPhone +
        ' advanced=' + String(!!result?.transferAdvanced) +
        ' ignored=' + String(!!result?.ignored) +
        ' reason=' + String(result?.reason || '');
      console.log(msg);
      EscribirLog(msg, 'event');
    } catch {}
    return true;
  } catch (e) {
    try {
      const msg = '[WWEB_OPERATOR] error sincronizando mensaje saliente: ' + String(e?.message || e);
      console.log(msg);
      EscribirLog(msg, 'error');
    } catch {}
    return false;
  }
}


function logConfirmacionDebug(msg) {
  try { console.log(msg); } catch {}
  try { EscribirLog(msg, 'event'); } catch {}
}


function buildSetAceptadoConfirmacionApiMensajes(now, phone, respuesta) {
  return {
    tenantId: apiMensajesConfirmacionTenantId(),
    numeroFrom: apiMensajesConfirmacionNumeroFrom(),
    nroTel: onlyDigits(phone || ''),
    estado: 'aceptado',
    aceptadoAt: now,
    respuesta: String(respuesta || '').trim(),
    updatedAt: now
  };
}

function buildSetCanceladoConfirmacionApiMensajes(now, phone, respuesta, motivo) {
  return {
    tenantId: apiMensajesConfirmacionTenantId(),
    numeroFrom: apiMensajesConfirmacionNumeroFrom(),
    nroTel: onlyDigits(phone || ''),
    estado: 'cancelado',
    canceladoAt: now,
    respuestaCancelacion: String(respuesta || '').trim(),
    motivoCancelacion: String(motivo || 'confirmacion_cancelada'),
    updatedAt: now
  };
}

function keyPendienteConfirmacionApiMensajes(idDest, idRenglon) {
  const raw = String(idDest || '') + '_' + String(idRenglon || '');
  return raw.replace(/[^a-zA-Z0-9_-]/g, '_') || ('k_' + Date.now());
}

function pendientesConfirmacionApiMensajesArray(doc) {
  try {
    const p = doc && doc.pendientes;
    if (!p) return [];
    if (Array.isArray(p)) return p.filter(Boolean);
    if (typeof p === 'object') return Object.values(p).filter(Boolean);
  } catch {}
  return [];
}

function buildUrlConfirmaApiMensajes() {
  const nroTelFrom = getApiMensajesNroTelFrom();
  return buildUrlWithParams(api3, { key, nro_tel_from: nroTelFrom });
}

async function guardarPendienteConfirmacionApiMensajes(nroTel, data) {
  try {
    const to = onlyDigits(nroTel || '');
    if (!to || !await ensureMongo()) return false;
    const col = apiMensajesConfirmacionCollection();
    if (!col) return false;
    const now = new Date();
    const idDest = data?.id_msj_dest ?? data?.Id_msj_dest ?? '';
    const idRenglon = data?.id_msj_renglon ?? data?.Id_msj_renglon ?? '';
    const k = keyPendienteConfirmacionApiMensajes(idDest, idRenglon);
    const item = {
      key: k,
      tenantId: apiMensajesConfirmacionTenantId(),
      numeroFrom: apiMensajesConfirmacionNumeroFrom(),
      nroTel: to,
      id_msj_dest: idDest,
      id_msj_renglon: idRenglon,
      msj: String(data?.msj ?? data?.Msj ?? ''),
      content: data?.content ?? data?.Content ?? null,
      content_nombre: data?.content_nombre ?? data?.Content_nombre ?? null,
      guardadoAt: now,
      updatedAt: now
    };

    await col.updateOne(
      { _id: apiMensajesConfirmacionId(to) },
      {
        $setOnInsert: {
          createdAt: now,
          tenantId: apiMensajesConfirmacionTenantId(),
          numeroFrom: apiMensajesConfirmacionNumeroFrom(),
          nroTel: to
        },
        $set: {
          [`pendientes.${k}`]: item,
          pendientesUpdatedAt: now,
          updatedAt: now
        }
      },
      { upsert: true }
    );

    const log = '[API_MENSAJES_CONFIRMACION] pendiente guardado en Mongo nro=' + to +
      ' id_msj_dest=' + String(idDest || '') +
      ' id_msj_renglon=' + String(idRenglon || '') +
      ' key=' + k;
    console.log(log);
    EscribirLog(log, 'event');
    return true;
  } catch (e) {
    try { EscribirLog('[API_MENSAJES_CONFIRMACION] error guardando pendiente: ' + String(e?.message || e), 'error'); } catch {}
    return false;
  }
}

async function getInfoContactoApiMensajes(nroTelFormat) {
  let tipo = null, contacto = null, email = null, direccion = null, nombre = null;
  try {
    const contact = await client.getContactById(nroTelFormat);
    if (contact?.isBusiness === true) {
      tipo = 'B';
      contacto = contact.pushname || null;
      email = contact.businessProfile?.email || null;
      direccion = contact.businessProfile?.address || null;
      nombre = contact.name || null;
    } else {
      tipo = 'C';
      nombre = contact?.name || null;
      contacto = contact?.shortName || contact?.pushname || null;
    }
  } catch (e) {
    try { EscribirLog('getContactById error ' + nroTelFormat + ': ' + String(e?.message || e), 'error'); } catch {}
  }
  return { tipo, nombre, contacto, direccion, email };
}

async function procesarPendientesDocConfirmacionApiMensajes(doc, accion, motivo) {
  const col = apiMensajesConfirmacionCollection();
  if (!col || !doc) return { total: 0, ok: 0 };
  const pendientes = pendientesConfirmacionApiMensajesArray(doc);
  if (!pendientes.length) return { total: 0, ok: 0 };

  const url_confirma_msg = buildUrlConfirmaApiMensajes();
  let ok = 0;
  let ultimoNro = '';

  for (const item of pendientes) {
    const to = onlyDigits(item.nroTel || doc.nroTel || '');
    const nroTelFormat = to + '@c.us';
    const idDest = item.id_msj_dest;
    const idRenglon = item.id_msj_renglon;
    const pendingKey = item.key || keyPendienteConfirmacionApiMensajes(idDest, idRenglon);

    try {
      if (!to || !idDest || !idRenglon) {
        const logBad = '[API_MENSAJES_CONFIRMACION] pendiente invalido; no se procesa key=' + pendingKey + ' nro=' + to;
        console.log(logBad);
        EscribirLog(logBad, 'error');
        continue;
      }

      if (accion === 'C') {
        const updOk = await actualizar_estado_mensaje(url_confirma_msg, 'C', null, null, null, null, null, idRenglon, idDest);
        const logC = '[API_MENSAJES_CONFIRMACION] mensaje actualizado a C por ' + String(motivo || 'confirmacion_cancelada') +
          ' nro=' + to +
          ' id_msj_dest=' + String(idDest || '') +
          ' id_msj_renglon=' + String(idRenglon || '') +
          ' ok=' + String(updOk);
        console.log(logC);
        EscribirLog(logC, updOk ? 'event' : 'error');
        if (updOk) {
          ok++;
          await col.updateOne({ _id: doc._id }, { $unset: { [`pendientes.${pendingKey}`]: '' }, $set: { pendientesUpdatedAt: new Date(), updatedAt: new Date() } });
        }
        continue;
      }

      if (accion === 'E') {
        if (ultimoNro) await sleep(calcularDelayConsultaMensajesMs(ultimoNro, to));
        let contentNombre = item.content_nombre;
        if (contentNombre == null || contentNombre === '') contentNombre = 'archivo';
        const msj = String(item.msj || '');
        const contenido = item.content;

        if (contenido != null && String(contenido) !== '') {
          const mimeType = detectMimeType(String(contenido)) || mime.lookup(contentNombre) || 'application/octet-stream';
          const media = new MessageMedia(mimeType, String(contenido), contentNombre);
          await io.emit('message', 'Mensaje: ' + nroTelFormat + ': ' + msj);
          const sentApiMensaje = await safeSend(nroTelFormat, media, { caption: msj });
          await recordApiMensajesBillingWindow(to, {
            sentMessage: sentApiMensaje,
            messageType: 'media',
            text: msj,
            idDest,
            idRenglon
          });
          const logEnvioApi = '[API_MENSAJES] enviado adjunto pendiente a ' + to +
            ' id_msj_dest=' + String(idDest || '') +
            ' id_msj_renglon=' + String(idRenglon || '') +
            ' archivo=' + String(contentNombre || '') +
            ' mime=' + String(mimeType || '') +
            ' texto=' + msj.slice(0, 120);
          console.log(logEnvioApi);
          EscribirLog(logEnvioApi, 'event');
        } else {
          await io.emit('message', 'Mensaje: ' + nroTelFormat + ': ' + msj);
          const sentApiMensaje = await safeSend(nroTelFormat, msj);
          await recordApiMensajesBillingWindow(to, {
            sentMessage: sentApiMensaje,
            messageType: 'text',
            text: msj,
            idDest,
            idRenglon
          });
          const logEnvioApi = '[API_MENSAJES] enviado texto pendiente a ' + to +
            ' id_msj_dest=' + String(idDest || '') +
            ' id_msj_renglon=' + String(idRenglon || '') +
            ' texto=' + msj.slice(0, 160);
          console.log(logEnvioApi);
          EscribirLog(logEnvioApi, 'event');
        }

        const info = await getInfoContactoApiMensajes(nroTelFormat);
        const updOk = await actualizar_estado_mensaje(url_confirma_msg, 'E', info.tipo, info.nombre, info.contacto, info.direccion, info.email, idRenglon, idDest);
        const logE = '[API_MENSAJES_CONFIRMACION] pendiente enviado y actualizado a E nro=' + to +
          ' id_msj_dest=' + String(idDest || '') +
          ' id_msj_renglon=' + String(idRenglon || '') +
          ' ok=' + String(updOk);
        console.log(logE);
        EscribirLog(logE, updOk ? 'event' : 'error');
        
        ultimoNro = to;
        if (updOk) {
          ok++;
          await col.updateOne({ _id: doc._id }, { $unset: { [`pendientes.${pendingKey}`]: '' }, $set: { pendientesUpdatedAt: new Date(), updatedAt: new Date() } });
        }
      }
    } catch (e) {
      try { EscribirLog('[API_MENSAJES_CONFIRMACION] error procesando pendiente key=' + pendingKey + ': ' + String(e?.message || e), 'error'); } catch {}
    }
  }

  return { total: pendientes.length, ok };
}

async function procesarPendientesConfirmacionApiMensajes(phoneCandidates, accion, motivo) {
  try {
    if (!await ensureMongo()) return { total: 0, ok: 0 };
    const col = apiMensajesConfirmacionCollection();
    if (!col) return { total: 0, ok: 0 };
    const query = queryConfirmacionApiMensajesByPhones(phoneCandidates);
    if (!query) return { total: 0, ok: 0 };
    const docs = await col.find(query).limit(20).toArray();
    let total = 0, ok = 0;
    for (const doc of docs) {
      const res = await procesarPendientesDocConfirmacionApiMensajes(doc, accion, motivo);
      total += Number(res.total || 0);
      ok += Number(res.ok || 0);
    }
    return { total, ok };
  } catch (e) {
    try { EscribirLog('[API_MENSAJES_CONFIRMACION] error procesando pendientes: ' + String(e?.message || e), 'error'); } catch {}
    return { total: 0, ok: 0 };
  }
}

async function procesarTimeoutsPendientesConfirmacionApiMensajes() {
  try {
    if (api_mensajes_confirmacion_habilitada !== true) return;
    const reenviarMs = Math.max(0, Number(api_mensajes_confirmacion_reenviar_ms) || 0);
    if (reenviarMs <= 0) return;
    if (!await ensureMongo()) return;
    const col = apiMensajesConfirmacionCollection();
    if (!col) return;
    const cutoff = new Date(Date.now() - reenviarMs);
    const docs = await col.find({
      tenantId: apiMensajesConfirmacionTenantId(),
      numeroFrom: apiMensajesConfirmacionNumeroFrom(),
      estado: 'pendiente',
      pedidoAt: { $lte: cutoff },
      pendientes: { $exists: true }
    }).limit(50).toArray();

    for (const doc of docs) {
      const now = new Date();
      await col.updateOne(
        { _id: doc._id },
        { $set: buildSetCanceladoConfirmacionApiMensajes(now, doc.nroTel, '', 'sin_respuesta_timeout') }
      );
      const logTimeout = '[API_MENSAJES_CONFIRMACION] timeout con pendientes guardados; se actualiza a C nro=' + String(doc.nroTel || '') +
        ' ventana_ms=' + String(reenviarMs);
      console.log(logTimeout);
      EscribirLog(logTimeout, 'event');
      await procesarPendientesDocConfirmacionApiMensajes({ ...doc, estado: 'cancelado' }, 'C', 'sin_respuesta_timeout');
    }
  } catch (e) {
    try { EscribirLog('[API_MENSAJES_CONFIRMACION] error procesando timeouts: ' + String(e?.message || e), 'error'); } catch {}
  }
}

function normalizarRespuestaConfirmacionApiMensajes(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
}

function respuestasOkApiMensajesConfirmacion() {
  const raw = api_mensajes_confirmacion_respuestas_ok;
  const arr = Array.isArray(raw) ? raw : String(raw || 'OK').split(/[|,;]/g);
  const out = arr.map(normalizarRespuestaConfirmacionApiMensajes).filter(Boolean);
  return out.length ? out : ['OK'];
}

function respuestaConfirmaApiMensajes(body) {
  const b = normalizarRespuestaConfirmacionApiMensajes(body);
  if (!b) return false;
  return respuestasOkApiMensajesConfirmacion().includes(b);
}

function textoSolicitudConfirmacionApiMensajes() {
  return String(api_mensajes_confirmacion_mensaje || '').trim() || 'Hola, vas a recibir un mensaje de nuestra parte. Respondé OK para autorizar la recepción.';
}

function esTextoSolicitudConfirmacionApiMensajes(body) {
  const b = normalizarRespuestaConfirmacionApiMensajes(body);
  if (!b) return false;
  return b === normalizarRespuestaConfirmacionApiMensajes(textoSolicitudConfirmacionApiMensajes());
}

function esRespuestaNoValidaConfirmacionApiMensajes(body) {
  const raw = String(body || '').trim();
  if (!raw) return false;
  if (respuestaConfirmaApiMensajes(raw)) return false;
  if (esTextoSolicitudConfirmacionApiMensajes(raw)) return false;
  return true;
}

function apiMensajesConfirmacionAceptada(doc) {
  try {
    if (!doc || doc.estado !== 'aceptado') return false;
    if (!doc.aceptadoAt) return false;
    const validez = Number(api_mensajes_confirmacion_validez_ms) || 0;
    if (validez <= 0) return true;
    const acceptedMs = new Date(doc.aceptadoAt).getTime();
    if (!Number.isFinite(acceptedMs)) return false;
    return (Date.now() - acceptedMs) <= validez;
  } catch {
    return false;
  }
}

function getWhatsappMessageTimestampMs(message) {
  try {
    const raw = message?.timestamp ?? message?._data?.t ?? message?._data?.timestamp;
    const n = Number(raw);
    if (!Number.isFinite(n) || n <= 0) return 0;
    return n > 1000000000000 ? n : n * 1000;
  } catch {
    return 0;
  }
}

async function detectarOkConfirmacionApiMensajesEnChat(nroTel, doc) {
  try {
    if (api_mensajes_confirmacion_habilitada !== true) return false;
    if (!client || typeof client.getChatById !== 'function') return false;
    const to = onlyDigits(nroTel || '');
    if (!to) return false;
    if (!doc || doc.estado !== 'pendiente') return false;

    const pedidoMs = doc?.pedidoAt ? new Date(doc.pedidoAt).getTime() : 0;
    const chatId = to + '@c.us';
    const chat = await client.getChatById(chatId);
    if (!chat || typeof chat.fetchMessages !== 'function') return false;

    const messages = await chat.fetchMessages({ limit: 15 });
    const list = Array.isArray(messages) ? messages : [];
    for (const m of list) {
     const body = String(m?.body || m?._data?.body || '').trim();
      if (!respuestaConfirmaApiMensajes(body)) continue;

      const msgMs = getWhatsappMessageTimestampMs(m);
      if (pedidoMs && msgMs && msgMs < (pedidoMs - 5000)) continue;

      const col = apiMensajesConfirmacionCollection();
      if (!col) return false;
      const now = new Date();
      await col.updateOne(
        { _id: doc._id || apiMensajesConfirmacionId(to) },
        {
          $set: {
            ...buildSetAceptadoConfirmacionApiMensajes(now, to, body),
           aceptadoPor: m?.fromMe ? 'whatsapp_web_from_me' : 'cliente',
            aceptadoSource: 'chat_history'
          },
          $setOnInsert: { createdAt: now }
        },
        { upsert: true }
      );

      const log = '[API_MENSAJES_CONFIRMACION] OK detectado en chat de ' + to +
        ' texto=' + body +
        ' fromMe=' + String(!!m?.fromMe) +
        ' msgMs=' + String(msgMs || '');
      console.log(log);
      EscribirLog(log, 'event');
      return true;
    }
  } catch (e) {
    try { EscribirLog('[API_MENSAJES_CONFIRMACION] error leyendo chat para OK: ' + String(e?.message || e), 'error'); } catch {}
  }
  return false;
}

async function detectarNoValidaConfirmacionApiMensajesEnChat(nroTel, doc) {
  try {
    if (api_mensajes_confirmacion_habilitada !== true) return false;
    if (!client || typeof client.getChatById !== 'function') return false;
    const to = onlyDigits(nroTel || '');
    if (!to) return false;
    if (!doc || doc.estado !== 'pendiente') return false;

    const pedidoMs = doc?.pedidoAt ? new Date(doc.pedidoAt).getTime() : 0;
    const chatId = to + '@c.us';
    const chat = await client.getChatById(chatId);
    if (!chat || typeof chat.fetchMessages !== 'function') return false;

    const messages = await chat.fetchMessages({ limit: 15 });
    const list = Array.isArray(messages) ? messages : [];
    for (const m of list) {
      const body = String(m?.body || m?._data?.body || '').trim();
      if (!esRespuestaNoValidaConfirmacionApiMensajes(body)) continue;

      const msgMs = getWhatsappMessageTimestampMs(m);
      if (pedidoMs && msgMs && msgMs < (pedidoMs - 5000)) continue;

      const col = apiMensajesConfirmacionCollection();
      if (!col) return false;
      const now = new Date();
      const setCancelado = buildSetCanceladoConfirmacionApiMensajes(now, to, body, 'respuesta_no_valida');
      await col.updateOne(
        { _id: doc._id || apiMensajesConfirmacionId(to) },
        {
          $set: {
            ...setCancelado,
            canceladoPor: m?.fromMe ? 'whatsapp_web_from_me' : 'cliente',
            canceladoSource: 'chat_history'
          },
          $setOnInsert: { createdAt: now }
        },
        { upsert: true }
      );

      const log = '[API_MENSAJES_CONFIRMACION] respuesta no valida detectada en chat de ' + to +
        ' texto=' + body +
        ' fromMe=' + String(!!m?.fromMe) +
        ' msgMs=' + String(msgMs || '');
      console.log(log);
      EscribirLog(log, 'event');
      return true;
    }
  } catch (e) {
    try { EscribirLog('[API_MENSAJES_CONFIRMACION] error leyendo chat para respuesta no valida: ' + String(e?.message || e), 'error'); } catch {}
  }
  return false;
}

async function estadoConfirmacionApiMensajes(nroTel) {
  if (api_mensajes_confirmacion_habilitada !== true) return { autorizado: true, motivo: 'disabled' };
  const to = onlyDigits(nroTel || '');
  if (!to) return { autorizado: false, motivo: 'sin_numero' };
  if (!await ensureMongo()) return { autorizado: false, motivo: 'mongo_no_disponible' };
  const col = apiMensajesConfirmacionCollection();
  if (!col) return { autorizado: false, motivo: 'coleccion_no_disponible' };

  const now = new Date();
  const _id = apiMensajesConfirmacionId(to);
  const reenviarMs = Math.max(0, Number(api_mensajes_confirmacion_reenviar_ms) || 0);
  let doc = await col.findOne({ _id });


  if (apiMensajesConfirmacionAceptada(doc)) return { autorizado: true, motivo: 'aceptado', doc };
  if (doc && doc.estado === 'cancelado') {
    const baseCancelMs = new Date(doc.canceladoAt || doc.updatedAt || doc.pedidoAt || 0).getTime();
    const cancelacionVigente = reenviarMs <= 0 || !Number.isFinite(baseCancelMs) || baseCancelMs <= 0 || (Date.now() - baseCancelMs) < reenviarMs;

    if (cancelacionVigente) {
      try {
        const logCancelVigente = '[API_MENSAJES_CONFIRMACION] cancelacion vigente; se actualizara a C nro=' + to +
          ' ventana_ms=' + String(reenviarMs) +
          ' motivo=' + String(doc.motivoCancelacion || 'confirmacion_cancelada');
        console.log(logCancelVigente);
        EscribirLog(logCancelVigente, 'event');
      } catch {}
      return {
        autorizado: false,
        motivo: doc.motivoCancelacion || 'confirmacion_cancelada',
        solicitudEnviada: false,
        cancelarMensaje: true,
        doc
      };
    }

    const logReset = '[API_MENSAJES_CONFIRMACION] cancelacion vencida; se vuelve a pedir confirmacion a ' + to +
      ' ventana_ms=' + String(reenviarMs) +
      ' motivo_anterior=' + String(doc.motivoCancelacion || 'confirmacion_cancelada');
    console.log(logReset);
    EscribirLog(logReset, 'event');

    await col.updateOne(
      { _id },
      {
        $set: {
          estado: 'vencido',
          vencidoAt: now,
          motivoVencimiento: 'cancelacion_fuera_de_ventana',
          updatedAt: now
       }
      }
    );
    doc = null;
  }

 // Respaldo importante: si habilitar_bot=false, o si WhatsApp Web no entrega
  // el evento message/message_create del OK, igual detectamos el OK leyendo
  // los últimos mensajes del chat antes de volver a pedir confirmación.
  if (doc && doc.estado === 'pendiente') {
    const okDetectado = await detectarOkConfirmacionApiMensajesEnChat(to, doc);
    if (okDetectado) {
      doc = await col.findOne({ _id });
      await procesarPendientesConfirmacionApiMensajes([to], 'E', 'aceptado_chat_history');
      return { autorizado: true, motivo: 'aceptado_chat_history', doc };
    }
   const noValidaDetectada = await detectarNoValidaConfirmacionApiMensajesEnChat(to, doc);
   if (noValidaDetectada) {
      doc = await col.findOne({ _id });
      await procesarPendientesConfirmacionApiMensajes([to], 'C', 'respuesta_no_valida');
      return {
        autorizado: false,
        motivo: 'respuesta_no_valida',
        solicitudEnviada: false,
        cancelarMensaje: true,
        doc
      };
    }
  }


 const ultimoPedidoMs = doc?.pedidoAt ? new Date(doc.pedidoAt).getTime() : 0;
   const expiroVentana = !!doc && doc.estado === 'pendiente' && Number.isFinite(ultimoPedidoMs) && ultimoPedidoMs > 0 && reenviarMs > 0 && (Date.now() - ultimoPedidoMs) >= reenviarMs;

  if (expiroVentana) {
    const setCancelado = buildSetCanceladoConfirmacionApiMensajes(now, to, '', 'sin_respuesta_timeout');
    await col.updateOne(
      { _id },
      {
        $setOnInsert: { createdAt: now },
        $set: setCancelado
      },
      { upsert: true }
    );
    const logTimeout = '[API_MENSAJES_CONFIRMACION] confirmacion cancelada por timeout a ' + to +
      ' ventana_ms=' + String(reenviarMs);
    console.log(logTimeout);
    EscribirLog(logTimeout, 'event');
    return { autorizado: false, motivo: 'sin_respuesta_timeout', solicitudEnviada: false, cancelarMensaje: true, doc: { ...(doc || {}), ...setCancelado } };
  }

  const debePedir = !doc || !Number.isFinite(ultimoPedidoMs) || ultimoPedidoMs <= 0;
 

  if (debePedir) {
    const texto = textoSolicitudConfirmacionApiMensajes();
    await safeSend(to + '@c.us', texto);
    await col.updateOne(
      { _id },
      {
        $setOnInsert: { createdAt: now },
        $set: {
          tenantId: String(tenantId || '').toUpperCase(),
          numeroFrom: getApiMensajesNroTelFrom(),
          nroTel: to,
          estado: 'pendiente',
          pedidoAt: now,
          pedidoTexto: texto,
          respuestasOk: respuestasOkApiMensajesConfirmacion(),
          updatedAt: now
        }
      },
      { upsert: true }
    );
    const log = '[API_MENSAJES_CONFIRMACION] solicitud enviada a ' + to + ' reenviar_ms=' + String(reenviarMs);
    console.log(log);
    EscribirLog(log, 'event');
    return { autorizado: false, motivo: 'solicitud_enviada', solicitudEnviada: true };
  }

  return { autorizado: false, motivo: 'pendiente', solicitudEnviada: false, doc };
}

async function registrarRespuestaConfirmacionApiMensajes(message) {
  try {
    const bodyRaw = String(message?.body || message?._data?.body || '').trim();
    if (api_mensajes_confirmacion_habilitada !== true) {
      if (bodyRaw) logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] ignorado: confirmacion deshabilitada ' );
      return false;
    }
    if (!message) return false;
    if (message.type && message.type !== 'chat') {
      if (respuestaConfirmaApiMensajes(bodyRaw)) logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] OK ignorado por type=' + String(message.type));
     return false;
    }
    if (!respuestaConfirmaApiMensajes(bodyRaw)) return false;

    const fromRaw = String(message.from || message._data?.from || '').trim();
    if (!fromRaw || fromRaw === 'status@broadcast') {
      logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] OK sin from valido from=' + fromRaw + ' body=' + bodyRaw);
      return false;
    }
    if (!await ensureMongo()) {
      logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] OK recibido pero Mongo no disponible from=' + fromRaw);
      return false;
    }
    const col = apiMensajesConfirmacionCollection();
    if (!col) return false;
    const phoneCandidates = await phoneCandidatesConfirmacionApiMensajes(message);

    logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] OK candidato from=' + fromRaw +
      ' body=' + bodyRaw +
      ' candidatos=' + JSON.stringify(phoneCandidates) +
      ' source=' + String(message?._confirmacionSource || 'message'));

    const now = new Date();
    const respuesta = bodyRaw;
    let matched = 0;
    let acceptedPhone = phoneCandidates[0] || '';

    const query = queryConfirmacionApiMensajesByPhones(phoneCandidates);
    if (query) {
      const setData = buildSetAceptadoConfirmacionApiMensajes(now, acceptedPhone, respuesta);
      const upd = await col.updateMany(
        query,
        {
          $set: setData,
          $setOnInsert: { createdAt: now }
        }
      );
      matched = Number(upd?.matchedCount || upd?.modifiedCount || 0);
    }

    if (!matched && acceptedPhone) {
      const _id = apiMensajesConfirmacionId(acceptedPhone);
      await col.updateOne(
        { _id },
        {
          $setOnInsert: { createdAt: now },
          $set: buildSetAceptadoConfirmacionApiMensajes(now, acceptedPhone, respuesta)
        },
        { upsert: true }
      );
      matched = 1;
    }

    // Si por LID no se pudo resolver el teléfono pero hay una única confirmación pendiente
    // para este tenant/número, asociamos ese OK a esa pendiente. Evita que quede esperando
    // cuando WhatsApp Web entrega @lid y no hay mapeo manual todavía.
    if (!matched) {
      const pending = await col.find({
        tenantId: apiMensajesConfirmacionTenantId(),
        numeroFrom: apiMensajesConfirmacionNumeroFrom(),
        estado: 'pendiente'
      }).sort({ pedidoAt: -1 }).limit(2).toArray();

      if (pending.length === 1) {
        acceptedPhone = onlyDigits(pending[0].nroTel || '');
        await col.updateOne(
          { _id: pending[0]._id },
          {
            $set: buildSetAceptadoConfirmacionApiMensajes(now, acceptedPhone, respuesta)
          }
        );
        matched = 1;
      }
    }

    if (!matched) {
      const logNoMatch = '[API_MENSAJES_CONFIRMACION] respuesta OK recibida pero sin pendiente asociada from=' + fromRaw +
        ' candidatos=' + JSON.stringify(phoneCandidates) +
        ' texto=' + respuesta;
      console.log(logNoMatch);
      EscribirLog(logNoMatch, 'error');
      return true;
    }

    const log = '[API_MENSAJES_CONFIRMACION] respuesta OK recibida de ' + (acceptedPhone || phoneCandidates.join(',')) +
      ' texto=' + respuesta +
      ' docs=' + String(matched);
    console.log(log);
    EscribirLog(log, 'event');

    const proc = await procesarPendientesConfirmacionApiMensajes(
      acceptedPhone ? [acceptedPhone] : phoneCandidates,
      'E',
      'confirmacion_ok'
    );
    const logProc = '[API_MENSAJES_CONFIRMACION] pendientes procesados por OK total=' + String(proc.total || 0) + ' ok=' + String(proc.ok || 0);
    console.log(logProc);
    EscribirLog(logProc, 'event');

    // Si la consulta de mensajes quedó detenida, la despertamos. Si ya está corriendo,
    // no hace nada por el guard interno.

    try { startConsultaApiMensajesIfEnabled('confirmacion_ok'); } catch {}
    return true;
  } catch (e) {
    try { EscribirLog('[API_MENSAJES_CONFIRMACION] error respuesta: ' + String(e?.message || e), 'error'); } catch {}
    return false;
  }
}

async function registrarRespuestaNoValidaConfirmacionApiMensajes(message) {
  try {
    const bodyRaw = String(message?.body || message?._data?.body || '').trim();
    if (api_mensajes_confirmacion_habilitada !== true) return false;
    if (!message || !bodyRaw) return false;
    if (message.type && message.type !== 'chat') return false;
    if (!esRespuestaNoValidaConfirmacionApiMensajes(bodyRaw)) return false;

    const fromRaw = String(message.from || message._data?.from || '').trim();
    const remoteRaw = String(message?.id?.remote || message?._data?.id?.remote || '').trim();
    // No validar respuestas no válidas de grupos/estados/broadcast.
    // El chat puede recibir mensajes normales continuamente; solo importan los contactos
    // que tienen mensajes API pendientes de confirmación.
    if (!fromRaw || fromRaw === 'status@broadcast' || remoteRaw === 'status@broadcast') return false;
    if (fromRaw.endsWith('@g.us') || remoteRaw.endsWith('@g.us')) return false;
    if (!await ensureMongo()) {
      logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] respuesta no valida pero Mongo no disponible from=' + fromRaw + ' body=' );
      return false;
    }

    const col = apiMensajesConfirmacionCollection();
    if (!col) return false;

    const phoneCandidates = await phoneCandidatesConfirmacionApiMensajes(message);
    const queryBase = queryConfirmacionApiMensajesByPhones(phoneCandidates);
    if (!queryBase) return false;

    // Regla importante:
    // NO loguear ni cancelar una respuesta no válida si no hay mensajes API
    // previamente leídos y guardados como pendientes para ESTE teléfono.
    // Esto evita que cualquier mensaje común del chat/grupo se tome como rechazo.
    const docsPendientes = await col.find({
      $and: [
        queryBase,
        { estado: 'pendiente' },
        { pendientes: { $exists: true } }
      ]
    }).limit(20).toArray();

   const docsConPendientes = docsPendientes.filter((d) => pendientesConfirmacionApiMensajesArray(d).length > 0);
    if (!docsConPendientes.length) return false;

    const docIds = docsConPendientes.map((d) => d._id).filter(Boolean);
    const phonesPendientes = docsConPendientes
      .map((d) => onlyDigits(d?.nroTel || ''))
      .filter(Boolean);
    const now = new Date();
    let matched = docsConPendientes.length;
    let cancelPhone = phonesPendientes[0] || phoneCandidates[0] || '';

    logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] respuesta no valida candidata from=' + fromRaw +
      ' body=' + bodyRaw +
      ' candidatos=' + JSON.stringify(phoneCandidates) +
      ' pendientes=' + JSON.stringify(phonesPendientes) +
      ' source=' + String(message?._confirmacionSource || 'message'));

    
     const setData = buildSetCanceladoConfirmacionApiMensajes(now, cancelPhone, bodyRaw, 'respuesta_no_valida');
    const upd = await col.updateMany({ _id: { $in: docIds } }, { $set: setData });
    matched = Number(upd?.matchedCount || upd?.modifiedCount || matched || 0);

    if (!matched) return false;

    const log = '[API_MENSAJES_CONFIRMACION] respuesta no valida recibida; se cancelan mensajes pendientes de ' +
      (phonesPendientes.join(',') || cancelPhone || phoneCandidates.join(',')) +
      ' texto=' + bodyRaw +
      ' docs=' + String(matched);
    console.log(log);
    EscribirLog(log, 'event');
 
    const proc = await procesarPendientesConfirmacionApiMensajes(
      phonesPendientes.length ? phonesPendientes : (cancelPhone ? [cancelPhone] : phoneCandidates),
      'C',
      'respuesta_no_valida'
    );
    const logProc = '[API_MENSAJES_CONFIRMACION] pendientes actualizados a C por respuesta_no_valida total=' + String(proc.total || 0) + ' ok=' + String(proc.ok || 0);
    console.log(logProc);
    EscribirLog(logProc, proc.ok ? 'event' : 'error');

    try { startConsultaApiMensajesIfEnabled('confirmacion_respuesta_no_valida'); } catch {}
    return true;
  } catch (e) {
    try { EscribirLog('[API_MENSAJES_CONFIRMACION] error respuesta no valida: ' + String(e?.message || e), 'error'); } catch {}
    return false;
  }
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

function randomDelayMsBetween(desde, hasta, fallbackDesde, fallbackHasta) {
  const d = Number.isFinite(Number(desde)) ? Number(desde) : Number(fallbackDesde);
  const h = Number.isFinite(Number(hasta)) ? Number(hasta) : Number(fallbackHasta);
  const min = Math.max(0, Math.min(d, h));
  const max = Math.max(0, Math.max(d, h));
  if (!Number.isFinite(min) || !Number.isFinite(max)) return 0;
  if (max <= min) return min;
  return Math.floor(Math.random() * (max - min) + min);
}

function calcularDelayConsultaMensajesMs(nroTelAnterior, nroTelActual) {
  const anterior = onlyDigits(nroTelAnterior || '');
  const actual = onlyDigits(nroTelActual || '');
  const mismoNumero = !!anterior && !!actual && anterior === actual;

  const delay = randomDelayMsBetween(
    mismoNumero ? seg_desde : seg_desde2,
    mismoNumero ? seg_hasta : seg_hasta2,
    seg_desde,
    seg_hasta
  );

  try {
    const tipo = mismoNumero ? 'mismo_numero' : 'distinto_numero';
    console.log('[ConsultaApiMensajes] delay ' + tipo + ': ' + delay + 'ms (' + (anterior || '-') + ' -> ' + (actual || '-') + ')');
  } catch {}

  seg_msg = delay;
  return delay;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
 
 


async function ConsultaApiMensajes(){
  if (consultaApiMensajesRunning) {
    console.log("ConsultaApiMensajes ya está corriendo");
    return;
  }

  consultaApiMensajesRunning = true;
  console.log("Consultando a API de mensajes salientes");
  EscribirLog("Consultando a API de mensajes salientes", "event");

  try {
    await sleep(1000);

    while (consulta_api_mensajes_habilitado === true) {
      await refreshRuntimeDomainConfig('ready');

      if (consulta_api_mensajes_habilitado !== true) break;

      if (String(localWsPanelState || '').toLowerCase() === 'paused' || lastPolicyBlocked === true) {
        try { console.log('[WAIT] ConsultaApiMensajes detenida: bot en pausa'); } catch {}
        try { EscribirLog('[WAIT] ConsultaApiMensajes detenida: bot en pausa', 'event'); } catch {}
        return;
      }


      // Si la sesión no está ONLINE no hay que consultar la API de mensajes salientes.
      // Al desloguearse queda QR, pero este loop puede seguir vivo desde el ready anterior.
      const consultaWsState = String(localWsPanelState || '').toLowerCase();
      const consultaTieneSesionActiva = !!(client && client.info && client.info.me && client.info.me.user);
      if (consultaWsState !== 'online' || consultaTieneSesionActiva !== true) {
        const waitMs = Math.max(5000, Number(devolver_seg_tele()) || 30000);
        try { console.log('[WAIT] ConsultaApiMensajes pausada: sesión WhatsApp no online state=' + consultaWsState); } catch {}
        try { EscribirLog('[WAIT] ConsultaApiMensajes pausada: sesión WhatsApp no online state=' + consultaWsState, 'event'); } catch {}
        await sleep(waitMs);
        continue;
      }

      // Si el panel dejó la sesión en PAUSED / mensajes bloqueados, no se debe leer
      // la API de mensajes salientes. Leerla reserva los mensajes y luego el API
      // puede volver a liberarlos si no se actualizan a tiempo.
      try { await pollActionsOnce(); } catch {}
      if (String(localWsPanelState || '').toLowerCase() === 'paused' || lastPolicyBlocked === true || await isWwebMessagesBlockedSafe()) {
        try { console.log('[WAIT] ConsultaApiMensajes detenida antes de leer API: bot en pausa'); } catch {}
        try { EscribirLog('[WAIT] ConsultaApiMensajes detenida antes de leer API: bot en pausa', 'event'); } catch {}
        return;
      }

      await pollActionsOnce();
      if (await isWwebMessagesBlockedSafe()) {
        const waitMs = Math.max(5000, Number(devolver_seg_tele()) || 30000);
        try { console.log('[WAIT] ConsultaApiMensajes pausada: bot en pausa, no consulta API'); } catch {}
        try { EscribirLog('[WAIT] ConsultaApiMensajes pausada: bot en pausa, no consulta API', 'event'); } catch {}
        await sleep(waitMs);
        continue;
      }

      await procesarTimeoutsPendientesConfirmacionApiMensajes();

      const horarioConsulta = await getConsultaMensajesScheduleStatus();
      logConsultaMensajesScheduleStatus(horarioConsulta);
      if (!horarioConsulta.allowed) {
        await sleepConsultaMensajesFueraDeHorario();
        continue;
      }

      const nroTelFrom = getApiMensajesNroTelFrom();
      if (!api2 || !api3 || !key || !nroTelFrom) {
        const detalle = `ConsultaApiMensajes sin configuración completa api2=${!!api2} api3=${!!api3} key=${!!key} nro_tel_from=${nroTelFrom || '(vacío)'}`;
        console.log(detalle);
        EscribirLog(detalle, "error");
        await sleep(Math.max(5000, Number(devolver_seg_tele()) || 30000));
        continue;
      }

      const url = buildUrlWithParams(api2, { key, nro_tel_from: nroTelFrom });
      const url_confirma_msg = buildUrlWithParams(api3, { key, nro_tel_from: nroTelFrom });

      //seg_msg = Math.random() * (devolver_seg_hasta() - devolver_seg_desde()) + devolver_seg_desde();


      try {
        //console.log("Conectando a API " + url);
        //EscribirLog("Conectando a API " + url, "event");
        const resp = await fetch(url, {
          method: "GET",
          compress: false,
          timeout: 60000,
          headers: {
            'Accept': 'application/json,text/plain,*/*',
            'Accept-Encoding': 'identity',
            'Connection': 'close'
          }
        }).catch(err => {
          EscribirLog("ConsultaApiMensajes fetch error: " + String(err?.message || err), "error");
          return null;
        });
     

        if (!resp) {
          await sleep(Math.max(5000, Number(devolver_seg_tele()) || 30000));
          continue;
        }

        let raw = '';
        try {
          raw = await resp.text();
        } catch (e) {
          const msg = 'ConsultaApiMensajes response body error: ' + String(e?.message || e);
          console.log(msg);
          EscribirLog(msg, 'error');
          await sleep(Math.max(5000, Number(devolver_seg_tele()) || 30000));
          continue;
        }

        let jsonResp = null;
        try { jsonResp = raw ? JSON.parse(raw) : null; } catch {}

        if (!resp.ok) {
          const detalle = jsonResp ? JSON.stringify(jsonResp) : raw;
          if (msg_errores) {
            console.log("ApiWhatsapp - Response ERROR " + detalle);
            EscribirLog("ApiWhatsapp - Response ERROR " + detalle, "error");
          }
          await sleep(Math.max(5000, Number(devolver_seg_tele()) || 30000));
          continue;
        }

        if (!Array.isArray(jsonResp) || !jsonResp[0]) {
          await sleep(Number(devolver_seg_tele()) || 30000);
          continue;
        }
 
        if (String(localWsPanelState || '').toLowerCase() === 'paused' || lastPolicyBlocked === true || await isWwebMessagesBlockedSafe()) {
          try { console.log('[WAIT] ConsultaApiMensajes detenida luego de leer API: bot en pausa'); } catch {}
          try { EscribirLog('[WAIT] ConsultaApiMensajes detenida luego de leer API: bot en pausa', 'event'); } catch {}
          return;
        }

        await pollActionsOnce();
        if (await isWwebMessagesBlockedSafe()) {
          try { console.log('[WAIT] ConsultaApiMensajes detenida luego de leer API: bot en pausa'); } catch {}
          try { EscribirLog('[WAIT] ConsultaApiMensajes detenida luego de leer API: bot en pausa', 'event'); } catch {}
          return;
        }


        const mensajes = Array.isArray(jsonResp[0].mensajes) ? jsonResp[0].mensajes : [];
        const destinatarios = Array.isArray(jsonResp[0].destinatarios) ? jsonResp[0].destinatarios : [];
        let ultimoNroTelConsultaMensajes = '';

        for (let i = 0; i < destinatarios.length; i++) {
          const dest = destinatarios[i] || {};
          const idDestRenglon = dest.Id_msj_renglon;
          const respuesta = mensajes.filter(m => String(m?.Id_msj_renglon) === String(idDestRenglon));

          for (let j = 0; j < respuesta.length; j++) {
            await pollActionsOnce();
            if (await isWwebMessagesBlockedSafe()) {
              try { console.log('[WAIT] ConsultaApiMensajes detenida antes de procesar mensaje: bot en pausa'); } catch {}
              try { EscribirLog('[WAIT] ConsultaApiMensajes detenida antes de procesar mensaje: bot en pausa', 'event'); } catch {}
              return;
            }

            try { await pollActionsOnce(); } catch {}
            if (String(localWsPanelState || '').toLowerCase() === 'paused' || lastPolicyBlocked === true || await isWwebMessagesBlockedSafe()) {
              try { console.log('[WAIT] ConsultaApiMensajes detenida antes de enviar: bot en pausa'); } catch {}
              try { EscribirLog('[WAIT] ConsultaApiMensajes detenida antes de enviar: bot en pausa', 'event'); } catch {}
              return;
            }

            const msg = respuesta[j] || {};
            const Id_msj_dest_local = dest.Id_msj_dest;
            const Id_msj_renglon_local = dest.Id_msj_renglon;
            const Nro_tel = onlyDigits(dest.Nro_tel || '');
            const Nro_tel_format = Nro_tel + '@c.us';
            const Msj = msg.Msj == null ? '' : String(msg.Msj);
            const contenido = msg.Content;
            let Content_nombre = msg.Content_nombre;

            console.log('--------------------------------------------------');
            console.log("Id_msj_dest " + JSON.stringify(Id_msj_dest_local));
            console.log("Id_msj_renglon " + JSON.stringify(Id_msj_renglon_local));
            console.log("Nro_tel " + JSON.stringify(Nro_tel));
            console.log("Msj " + JSON.stringify(Msj));
            console.log("Content_nombre " + JSON.stringify(Content_nombre));
            console.log('--------------------------------------------------');

            if (!Nro_tel || isNaN(Number(Nro_tel))) {
              console.log("numero invalido");
              await io.emit('message', 'Mensaje: ' + Nro_tel_format + ': Número Inválido');
              await actualizar_estado_mensaje(url_confirma_msg, 'I', null, null, null, null, null, Id_msj_renglon_local, Id_msj_dest_local);
              continue;
             
            }

            let registered = false;
            try { registered = await client.isRegisteredUser(Nro_tel_format); } catch (e) {
              EscribirLog('isRegisteredUser error ' + Nro_tel_format + ': ' + String(e?.message || e), "error");
            }

            if (!registered) {
              EscribirLog('Mensaje: ' + Nro_tel_format + ': Número no Registrado', "event");
              console.log("numero no registrado");
              await io.emit('message', 'Mensaje: ' + Nro_tel_format + ': Número no Registrado');
              await actualizar_estado_mensaje(url_confirma_msg, 'I', null, null, null, null, null, Id_msj_renglon_local, Id_msj_dest_local);
              continue;
            }

          if (ultimoNroTelConsultaMensajes) {
              await sleep(calcularDelayConsultaMensajesMs(ultimoNroTelConsultaMensajes, Nro_tel));
              try { await pollActionsOnce(); } catch {}
            }

            if (String(localWsPanelState || '').toLowerCase() === 'paused' || lastPolicyBlocked === true || await isWwebMessagesBlockedSafe()) {
              try { console.log('[WAIT] ConsultaApiMensajes detenida despues del delay: bot en pausa'); } catch {}
              try { EscribirLog('[WAIT] ConsultaApiMensajes detenida despues del delay: bot en pausa', 'event'); } catch {}
              return;
            }

            await pollActionsOnce();
            if (await isWwebMessagesBlockedSafe()) {
              try { console.log('[WAIT] ConsultaApiMensajes detenida despues del delay: bot en pausa'); } catch {}
              try { EscribirLog('[WAIT] ConsultaApiMensajes detenida despues del delay: bot en pausa', 'event'); } catch {}
              return;
            }


            const permisoConfirmacion = await estadoConfirmacionApiMensajes(Nro_tel);
            if (!permisoConfirmacion.autorizado) {
              const log = '[API_MENSAJES_CONFIRMACION] envío retenido a ' + Nro_tel +
                ' motivo=' + String(permisoConfirmacion.motivo || '') +
                ' id_msj_dest=' + String(Id_msj_dest_local || '') +
                ' id_msj_renglon=' + String(Id_msj_renglon_local || '');
              console.log(log);
              EscribirLog(log, 'event');

              if (permisoConfirmacion.cancelarMensaje !== true) {
                await guardarPendienteConfirmacionApiMensajes(Nro_tel, {
                  id_msj_dest: Id_msj_dest_local,
                  id_msj_renglon: Id_msj_renglon_local,
                  msj: Msj,
                  content: contenido,
                  content_nombre: Content_nombre
                });
              }


              if (permisoConfirmacion.cancelarMensaje === true) {
                const okCancel = await actualizar_estado_mensaje(url_confirma_msg, 'C', null, null, null, null, null, Id_msj_renglon_local, Id_msj_dest_local);
                const logCancel = '[API_MENSAJES_CONFIRMACION] mensaje actualizado a C por ' + String(permisoConfirmacion.motivo || 'confirmacion_cancelada') +
                  ' nro=' + Nro_tel +
                  ' id_msj_dest=' + String(Id_msj_dest_local || '') +
                  ' id_msj_renglon=' + String(Id_msj_renglon_local || '') +
                  ' ok=' + String(okCancel);
                console.log(logCancel);
                EscribirLog(logCancel, okCancel ? 'event' : 'error');
              }


              if (permisoConfirmacion.solicitudEnviada) ultimoNroTelConsultaMensajes = Nro_tel;
              continue;
            }

            if (Content_nombre == null || Content_nombre === '') Content_nombre = 'archivo';

            await pollActionsOnce();
            if (await isWwebMessagesBlockedSafe()) {
              try { console.log('[WAIT] ConsultaApiMensajes detenida antes de enviar: bot en pausa'); } catch {}
              try { EscribirLog('[WAIT] ConsultaApiMensajes detenida antes de enviar: bot en pausa', 'event'); } catch {}
              return;
            }


            if (contenido != null && String(contenido) !== '') {
              const mimeType = detectMimeType(String(contenido)) || mime.lookup(Content_nombre) || 'application/octet-stream';
              console.log('tipo de dato: ' + mimeType);
              const media = new MessageMedia(mimeType, String(contenido), Content_nombre);
              try { await pollActionsOnce(); } catch {}
              if (String(localWsPanelState || '').toLowerCase() === 'paused' || lastPolicyBlocked === true || await isWwebMessagesBlockedSafe()) {
                try { console.log('[WAIT] ConsultaApiMensajes detenida justo antes de enviar adjunto: bot en pausa'); } catch {}
                try { EscribirLog('[WAIT] ConsultaApiMensajes detenida justo antes de enviar adjunto: bot en pausa', 'event'); } catch {}
                return;
              }
              await io.emit('message', 'Mensaje: ' + Nro_tel_format + ': ' + Msj);
              const sentApiMensaje = await safeSend(Nro_tel_format, media, { caption: Msj });
              await recordApiMensajesBillingWindow(Nro_tel, {
                sentMessage: sentApiMensaje,
                messageType: 'media',
                text: Msj,
                idDest: Id_msj_dest_local,
                idRenglon: Id_msj_renglon_local
              });
              const logEnvioApi = '[API_MENSAJES] enviado adjunto a ' + Nro_tel +
                ' id_msj_dest=' + String(Id_msj_dest_local || '') +
                ' id_msj_renglon=' + String(Id_msj_renglon_local || '') +
                ' archivo=' + String(Content_nombre || '') +
                ' mime=' + String(mimeType || '') +
                ' texto=' + String(Msj || '').slice(0, 120);
              console.log(logEnvioApi);
              EscribirLog(logEnvioApi, 'event');
            } else {
              console.log("msj texto");
             try { await pollActionsOnce(); } catch {}
              if (String(localWsPanelState || '').toLowerCase() === 'paused' || lastPolicyBlocked === true || await isWwebMessagesBlockedSafe()) {
                try { console.log('[WAIT] ConsultaApiMensajes detenida justo antes de enviar texto: bot en pausa'); } catch {}
                try { EscribirLog('[WAIT] ConsultaApiMensajes detenida justo antes de enviar texto: bot en pausa', 'event'); } catch {}
                return;
              }
              await io.emit('message', 'Mensaje: ' + Nro_tel_format + ': ' + Msj);
              const sentApiMensaje = await safeSend(Nro_tel_format, Msj);
              await recordApiMensajesBillingWindow(Nro_tel, {
                sentMessage: sentApiMensaje,
                messageType: 'text',
                text: Msj,
                idDest: Id_msj_dest_local,
               idRenglon: Id_msj_renglon_local
              });
              const logEnvioApi = '[API_MENSAJES] enviado texto a ' + Nro_tel +
                ' id_msj_dest=' + String(Id_msj_dest_local || '') +
                ' id_msj_renglon=' + String(Id_msj_renglon_local || '') +
                ' texto=' + String(Msj || '').slice(0, 160);
              console.log(logEnvioApi);
              EscribirLog(logEnvioApi, 'event');
            }
 
            let tipo = null, contacto = null, email = null, direccion = null, nombre = null;
            try {
              const contact = await client.getContactById(Nro_tel_format);
              if (contact?.isBusiness === true) {
                tipo = 'B';
                contacto = contact.pushname || null;
                email = contact.businessProfile?.email || null;
                direccion = contact.businessProfile?.address || null;
                nombre = contact.name || null;
              } else {
                tipo = 'C';
                nombre = contact?.name || null;
                contacto = contact?.shortName || contact?.pushname || null;
              }
            } catch (e) {
              EscribirLog('getContactById error ' + Nro_tel_format + ': ' + String(e?.message || e), "error");
            }


                
            const okEstadoE = await actualizar_estado_mensaje(url_confirma_msg, 'E', tipo, nombre, contacto, direccion, email, Id_msj_renglon_local, Id_msj_dest_local);
            const logEstadoE = '[API_MENSAJES] estado E actualizado nro=' + Nro_tel +
              ' id_msj_dest=' + String(Id_msj_dest_local || '') +
              ' id_msj_renglon=' + String(Id_msj_renglon_local || '') +
              ' ok=' + String(okEstadoE);
            console.log(logEstadoE);
            EscribirLog(logEstadoE, okEstadoE ? 'event' : 'error');
            ultimoNroTelConsultaMensajes = Nro_tel;
          }
          
        }
      } catch (err) {
        console.log(err);
        EscribirLog('ConsultaApiMensajes error: ' + String(err?.message || err), "error");
      }
    

      try { RecuperarJsonConfMensajes(); } catch {}


  

      startCaducidadMensajesWatcher('ready');
      seg_tele = devolver_seg_tele();
      await sleep(Math.max(1000, Number(seg_tele) || 30000));
    }
  } finally {
    consultaApiMensajesRunning = false;
    console.log("ConsultaApiMensajes detenido");
    EscribirLog("ConsultaApiMensajes detenido", "event");
  }
}

function startConsultaApiMensajesIfEnabled(source = '') {
  try {
    if (consulta_api_mensajes_habilitado !== true) {
      console.log("ConsultaApiMensajes deshabilitado" + (source ? " source=" + source : ""));
      return;
    }
    
    if (String(localWsPanelState || '').toLowerCase() === 'paused' || lastPolicyBlocked === true) {
      const msg = "ConsultaApiMensajes no inicia: bot pausado" + (source ? " source=" + source : "");
      try { console.log(msg); } catch {}
      try { EscribirLog(msg, "event"); } catch {}
      return;
    }
    
    isWwebMessagesBlockedSafe().then((blocked) => {
      if (blocked === true) {
        const msg = "ConsultaApiMensajes no inicia: bot pausado" + (source ? " source=" + source : "");
        try { console.log(msg); } catch {}
        try { EscribirLog(msg, "event"); } catch {}
        return;
      }
      if (consulta_api_mensajes_habilitado !== true || consultaApiMensajesRunning) return;
      ConsultaApiMensajes().catch((e) => {
        consultaApiMensajesRunning = false;
        console.log("ConsultaApiMensajes fatal:", e?.message || e);
        EscribirLog("ConsultaApiMensajes fatal: " + String(e?.message || e), "error");
      });
    }).catch((e) => {
      console.log("startConsultaApiMensajesIfEnabled policy error:", e?.message || e);
      ("ConsultaApiMensajes fatal: " + String(e?.message || e), "error");
      EscribirLog("startConsultaApiMensajesIfEnabled policy error: " + String(e?.message || e), "error");
    });
  } catch (e) {
    console.log("startConsultaApiMensajesIfEnabled error:", e?.message || e);
    EscribirLog("startConsultaApiMensajesIfEnabled error: " + String(e?.message || e), "error");
  }
}


function getRuntimeConfigSnapshot() {
  return {
    wweb_engine: getWwebEngine(),
    habilitar_bot: habilitar_bot === true,
    habilitar_consulta_mensajes: consulta_api_mensajes_habilitado === true,
    habilitar_mensajes_info: habilitar_mensajes_info === true,
    habilitar_odbc_manager: habilitar_odbc_manager === true,
    api2: String(api2 || ''),
    api3: String(api3 || ''),
    key_configurada: !!key,
    api_mensajes_alta: String(api_mensajes_alta || ''),
    compra_mensajes_usar_api_alta: compra_mensajes_usar_api_alta === true,
    entrega_mensajes_usar_api_alta: entrega_mensajes_usar_api_alta === true,
    runtime_config_refresh_ms: Number(runtime_config_refresh_ms) || 0,
    consulta_mensajes_respetar_horarios: consulta_mensajes_respetar_horarios === true,
    consulta_mensajes_fuera_horario_sleep_ms: Number(consulta_mensajes_fuera_horario_sleep_ms) || 0,
    api_mensajes_confirmacion_habilitada: api_mensajes_confirmacion_habilitada === true,
    api_mensajes_confirmacion_reenviar_ms: Number(api_mensajes_confirmacion_reenviar_ms) || 0,
    api_mensajes_confirmacion_validez_ms: Number(api_mensajes_confirmacion_validez_ms) || 0,
    seg_desde: Number(seg_desde) || 0,
    seg_hasta: Number(seg_hasta) || 0,
    seg_desde2: Number(seg_desde2) || 0,
    seg_hasta2: Number(seg_hasta2) || 0,
    time_cad_ms: Number(time_cad) || 0
  };
}

function logRuntimeConfigChanges(prev, next, source = '') {
  try {
    if (!prev || !next) return;
    const cambios = [];
    for (const k of Object.keys(next)) {
      if (prev[k] !== next[k]) cambios.push(k + '=' + String(prev[k]) + '->' + String(next[k]));
    }
    if (!cambios.length) return;
    const msg = '[CONFIG] cambios runtime' + (source ? ' source=' + source : '') + ': ' + cambios.join(', ');
    console.log(msg);
    EscribirLog(msg, 'event');
  } catch {}
}

function canStartConsultaApiMensajesNow() {
  try {
    if (consulta_api_mensajes_habilitado !== true) return false;
    if (consultaApiMensajesRunning) return false;
    if (lastPolicyBlocked === true) return false;
    if (!client) return false;
    if (localWsPanelState !== 'online') return false;
    if (!getApiMensajesNroTelFrom()) return false;
    return true;
  } catch {
    return false;
  }
}

async function refreshRuntimeDomainConfig(source = 'runtime_config_poll') {
  if (runtimeConfigPollBusy) return;
 runtimeConfigPollBusy = true;

  const prev = getRuntimeConfigSnapshot();
  try {
    await loadTenantConfigFromDbMinimal();
    try { RecuperarJsonConfMensajes(); } catch {}

    const next = getRuntimeConfigSnapshot();
    logRuntimeConfigChanges(prev, next, source);
    lastRuntimeConfigSnapshot = next;

    // Si el dominio cambió wweb_engine con el proceso vivo, la variable en memoria
    // cambia al refrescar tenant_config, pero el Client ya creado sigue usando el
    // transporte anterior. Recrearlo para que el cambio wwebjs <-> baileys se aplique
    // sin necesitar acceso a la terminal ni reiniciar Windows.
    const desiredEngine = getWwebEngine();
    const activeEngine = String(client?.__transport || '').trim().toLowerCase();
    if (client && activeEngine && activeEngine !== desiredEngine) {
      const engineMsg = `[WWEB_ENGINE] cambio detectado active=${activeEngine} configured=${desiredEngine} source=${String(source || '')}`;
      try { console.log(engineMsg); } catch {}
      try { EscribirLog(engineMsg, 'event'); } catch {}

      if (isOwner && !restartInFlight && !startingNow && !clearAuthInFlight && !fullProcessRestartInFlight) {
        await restartClientSession(`wweb_engine_changed:${activeEngine}->${desiredEngine}`, 3500);
      } else {
        try { EscribirLog('[WWEB_ENGINE] cambio pendiente; no se reinicia ahora porque hay otra operación/reinicio en curso', 'event'); } catch {}
      }
      return;
    }

    if (next.habilitar_consulta_mensajes === true && canStartConsultaApiMensajesNow()) {
      startConsultaApiMensajesIfEnabled(source);
    }

    if (client && localWsPanelState === 'online') {
      startCompraEntregaLoopIfEnabled(source);
    }

    if (prev.habilitar_consulta_mensajes === true && next.habilitar_consulta_mensajes !== true && consultaApiMensajesRunning) {
      const msg = 'ConsultaApiMensajes deshabilitado por configuración; se detendrá al finalizar el ciclo actual';
      console.log(msg);
      EscribirLog(msg, 'event');
    }
  } catch (e) {
    console.log('refreshRuntimeDomainConfig error:', e?.message || e);
    EscribirLog('refreshRuntimeDomainConfig error: ' + String(e?.message || e), 'error');
  } finally {
    runtimeConfigPollBusy = false;
  }
}

function startRuntimeConfigPoller() {
  try {
    if (runtimeConfigPollTimer) return;

    const everyMs = Math.max(5000, Number(runtime_config_refresh_ms) || 30000);
    lastRuntimeConfigSnapshot = getRuntimeConfigSnapshot();

    const msg = '[CONFIG] refresco runtime activado cada ' + everyMs + 'ms';
    console.log(msg);
    EscribirLog(msg, 'event');

    runtimeConfigPollTimer = setInterval(() => {
      refreshRuntimeDomainConfig('interval').catch(() => {});
    }, everyMs);
  } catch (e) {
    console.log('startRuntimeConfigPoller error:', e?.message || e);
    EscribirLog('startRuntimeConfigPoller error: ' + String(e?.message || e), 'error');
  }
}



function escapeRegExp(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function replaceUrlPlaceholders(baseUrl, params) {
  let raw = String(baseUrl || '').trim();
  const used = new Set();

  for (const [k, v] of Object.entries(params || {})) {
    if (v === undefined || v === null || String(v) === '') continue;
    const encoded = encodeURIComponent(String(v));
    const before = raw;
    raw = raw
      .replace(new RegExp('\\{\\{\\s*' + escapeRegExp(k) + '\\s*\\}\\}', 'gi'), encoded)
      .replace(new RegExp('<\\s*' + escapeRegExp(k) + '\\s*>', 'gi'), encoded);
    if (raw !== before) used.add(k);
  }

  return { url: raw, used };
}

function urlAlreadyHasParam(rawUrl, keyName) {
  try {
    return new RegExp('(?:[?&])' + escapeRegExp(keyName) + '=', 'i').test(String(rawUrl || ''));
  } catch {
    return false;
  }
}


function buildUrlWithParams(baseUrl, params) {
  const replaced = replaceUrlPlaceholders(baseUrl, params);
  let raw = replaced.url;
  if (!raw) return '';
  const qs = Object.entries(params || {})
    .filter(([k, v]) => {
      if (v === undefined || v === null || String(v) === '') return false;
      if (replaced.used.has(k)) return false;
      if (urlAlreadyHasParam(raw, k)) return false;
      return true;
    })
    .map(([k, v]) => encodeURIComponent(k) + '=' + encodeURIComponent(String(v)))
    .join('&');
  if (!qs) return raw;
  return raw + (raw.includes('?') ? '&' : '?') + qs;
}

function formatFechaEnvioApiMensajes(date = new Date()) {
  try {
    const parts = new Intl.DateTimeFormat('sv-SE', {
      timeZone: AR_TZ,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    }).formatToParts(date);
    const map = {};
    for (const p of parts || []) map[p.type] = p.value;
    return `${map.year}-${map.month}-${map.day}`;
  } catch {
    return new Date(date).toISOString().slice(0, 10);
  }
}

function getApiMensajesAltaKey() {
  const candidates = [
    api_mensajes_alta_key,
    tenantConfig?.api_mensajes_alta_key,
    tenantConfig?.apiMensajesAltaKey,
    tenantConfig?.key_mensajes_alta,
    tenantConfig?.keyMensajesAlta,
    process.env.API_MENSAJES_ALTA_KEY,
    process.env.API_ALTA_MENSAJES_KEY,
    key
  ];
  for (const c of candidates) {
    const v = String(c || '').trim();
    if (v) return v;
  }
  return '';
}

async function altaApiMensaje({ nroTel, mensaje, identificacion1, tipo = 'MENSAJE' }) {
  const nroDestino = onlyDigits(nroTel);
  // El API Alta usa el telefono emisor local, como en la URL que funciona:
  // nro_tel_from=3462514448. WhatsApp/tenant puede venir como 5493462514448.
  const nroFrom = normalizarNroTelFromApiMensajes(getApiMensajesNroTelFrom())
    .replace(/^549(\d{10})$/, '$1')
    .replace(/^54(\d{10})$/, '$1');
  const altaKey = getApiMensajesAltaKey();

  if (!api_mensajes_alta) throw new Error('api_mensajes_alta_sin_configurar');
  if (!altaKey) throw new Error('api_mensajes_alta_key_sin_configurar');
  if (!nroFrom) throw new Error('api_mensajes_alta_nro_tel_from_sin_configurar');
  if (!nroDestino) throw new Error('api_mensajes_alta_destinatario_invalido');

  const url = buildUrlWithParams(api_mensajes_alta, {
    key: altaKey,
    nro_tel_from: nroFrom
  });

  const payload = {
    mensaje: [
      { fecha_envio: formatFechaEnvioApiMensajes() }
    ],
    lineas: [
      { orden: 1, mensaje: String(mensaje || '') }
    ],
    destinatarios: [
      {
        orden: 1,
        nro_tel: nroDestino,
        identificacion1: String(identificacion1 || '')
      }
    ]
  };

  const res = await fetchTextSafe(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify(payload)
  });

  if (!res.ok || apiMensajesResponseIndicaError(res.text)) {
    const urlLog = String(url || '').replace(/([?&]key=)[^&]*/i, '$1***');
    const detalle = '[' + String(tipo || 'MENSAJE') + '] API Alta ERROR -> ' + nroDestino +
      ' HTTP ' + String(res.status || 0) +
      ' url=' + urlLog +
      ' body=' + JSON.stringify(payload) +
      ' resp=' + apiMensajesResponseDetalle(res.text);
    console.log(detalle);
    EscribirLog(detalle, 'error');
    throw new Error('api_mensajes_alta_error_' + String(res.status || 0));
  }

  try {
    console.log(String(tipo || 'MENSAJE') + ': API Alta OK -> ' + nroDestino);
    io.emit('message', String(tipo || 'MENSAJE') + ': API Alta OK -> ' + nroDestino);
  } catch {}

  try { return res.text ? JSON.parse(res.text) : true; } catch { return res.text || true; }
}

async function altaApiMensajeCompra({ nroTel, mensaje, identificacion1 }) {
  const nroDestino = onlyDigits(nroTel);
  const nroFrom = getApiMensajesNroTelFrom();
  if (!api_mensajes_alta) throw new Error('api_mensajes_alta_sin_configurar');
  if (!api_mensajes_alta_key) throw new Error('api_mensajes_alta_key_sin_configurar');
  if (!nroFrom) throw new Error('api_mensajes_alta_nro_tel_from_sin_configurar');
  if (!nroDestino) throw new Error('api_mensajes_alta_destinatario_invalido');

  const url = buildUrlWithParams(api_mensajes_alta, {
    key: api_mensajes_alta_key,
    nro_tel_from: nroFrom
  });

  const body = {
    mensaje: [
      { fecha_envio: formatFechaEnvioApiMensajes(),
        agente:'SUPER'
       }
    ],
    lineas: [
      { orden: 1, mensaje: String(mensaje || '') }
    ],
    destinatarios: [
      {
        orden: 1,
        nro_tel: nroDestino,
        identificacion1: String(identificacion1 || '')
      }
    ]
  };

  let resp;
  try {
    resp = await axios.post(url, body, {
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
      timeout: 30000
    });
  } catch (e) {
    const status = e?.response?.status || 0;
    const data = e?.response?.data;
    const urlLog = String(url || '').replace(/([?&]key=)[^&]*/i, '$1***');
    const detalle = 'API Alta HTTP ' + status + ' -> nroFrom=' + nroFrom + ' nroDestino=' + nroDestino +
      ' url=' + urlLog + ' body=' + JSON.stringify(body) + ' resp=' +
      (typeof data === 'string' ? data : JSON.stringify(data || {}));
    console.log(detalle);
    try { EscribirLog(detalle, 'error'); } catch {}
    throw e;
  }

  try {
    console.log('COMPRA: API Alta OK -> ' + nroDestino + ' status=' + resp.status);
    io.emit('message', 'COMPRA: API Alta OK -> ' + nroDestino);
  } catch {}

  return resp.data;
}

async function altaApiMensajeEntrega({ nroTel, mensaje, identificacion1 }) {
  const data = await altaApiMensajeCompra({ nroTel, mensaje, identificacion1 });
  try {
   const nroDestino = onlyDigits(nroTel);
    console.log('ENTREGA: API Alta OK -> ' + nroDestino);
    io.emit('message', 'ENTREGA: API Alta OK -> ' + nroDestino);
  } catch {}
  return data;
}


async function fetchTextSafe(url, options) {
  const resp = await fetch(url, options).catch((err) => {
    EscribirLog('fetchTextSafe error: ' + String(err?.message || err), 'error');
    return null;
  });
  if (!resp) return { ok: false, status: 0, text: '' };
  const text = await resp.text().catch(() => '');
  return { ok: !!resp.ok, status: resp.status, text };
}

function apiMensajesResponseIndicaError(text) {
  try {
    const raw = String(text || '').trim();
    if (!raw) return false;
    const data = JSON.parse(raw);
    const arr = Array.isArray(data) ? data : [data];
    for (const item of arr) {
      if (!item || typeof item !== 'object') continue;
      const code = item.Error_Code ?? item.error_code ?? item.codigo_error ?? item.CodigoError;
      if (code !== undefined && code !== null && String(code).trim() !== '' && String(code).trim() !== '0') return true;
      const ok = item.Ok ?? item.ok ?? item.Success ?? item.success;
      if (ok === false || String(ok).trim().toLowerCase() === 'false') return true;
    }
  } catch {}
  return false;
}

function apiMensajesResponseDetalle(text) {
  const raw = String(text || '').trim();
  return raw ? raw.slice(0, 500) : '(sin cuerpo)';
}


async function actualizar_estado_mensaje(urlBase, estado, tipo, nombre, contacto, direccion, email, id_msj_renglon, id_msj_dest) {
  try {
    if (!urlBase) return false;

    // La URL del API de actualización sale de Mongo (api3), porque puede cambiar
    // por cliente/instalación. No se fuerza host, versión ni path acá.
    const urlPost = String(urlBase || '').trim();

    // Mismo body que funciona en Postman: ids + estado; el resto nulo.
   
    const payloadPost = {
      Id_Msj_Renglon: id_msj_renglon,
      Id_Msj_Dest: id_msj_dest,
      Estado: estado,
      Tipo: null,
      Nombre: null,
      Contacto: null,
      Direccion: null,
      Email: null
    };

 
    const logPrefix = 'actualizar_estado_mensaje estado=' + String(estado || '') +
      ' id_msj_dest=' + String(id_msj_dest || '') +
      ' id_msj_renglon=' + String(id_msj_renglon || '');

    const res = await fetchTextSafe(urlPost, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payloadPost)
    });

    if (res.ok && !apiMensajesResponseIndicaError(res.text)) return true;

    const urlLog = String(urlPost || '').replace(/([?&]key=)[^&]*/i, '$1***');
    const detalle = logPrefix + ' POST HTTP ' + res.status + ': ' + apiMensajesResponseDetalle(res.text) +
      ' url=' + urlLog +
      ' body=' + JSON.stringify(payloadPost);
    console.log(detalle);
    EscribirLog(detalle, 'error');
    return false;
  } catch (e) {
    EscribirLog('actualizar_estado_mensaje error: ' + String(e?.message || e), 'error');
    return false;
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
function toChatId(rawPhone) {
  const raw = String(rawPhone ?? "");
  let digits = raw.replace(/\D/g, "");
  if (!digits) return null;
  if (digits.startsWith("549")) return `${digits}@c.us`;
  if (digits.startsWith("54")) return `549${digits.slice(2)}@c.us`;
  return `549${digits}@c.us`;
}

function RecuperarTelefonos() {
  try {
    const jsonTel = JSON.parse(fs.readFileSync('C:/Chatbot_pb/telefonos.json'));
    return jsonTel;
  } catch (error) {
    const jsonTel = '[{"telefono":"999999999","nombre":"-","permitir":"N"}]';
    console.log("Sin bloqueo de telefonos...");
    return jsonTel;
  }
}

function validarTelefono(tel) {
  var telefono = RecuperarTelefonos();
  var permitir = telefono[0].permitir;
  console.log("permitir " + permitir);

  if (permitir == undefined) {
    return true;
  }

 if (permitir == 'N') {
    var tam = telefono.length;
    for (var i = 0; i < tam; i++) {
      console.log("busca en N " + '549' + telefono[i].telefono + ' ' + tel);
      if ('549' + telefono[i].telefono == tel) {
        console.log("existe en N " + '549' + telefono[i].telefono + ' ' + tel);
        return false;
      }
    }
    return true;
  }

  if (permitir == 'S') {
    var tam = telefono.length;
   for (var i = 0; i < tam; i++) {
      console.log("busca en S " + '549' + telefono[i].telefono + ' ' + tel);
      if ('549' + telefono[i].telefono == tel) {
        console.log("existe en S " + '549' + telefono[i].telefono + ' ' + tel);
        return true;
      }
    }
    return false;
  }
}

let compraEntregaQueryRunning = false;
let compraEntregaQueryStopRequested = false;
let compraEntregaConnection = null;

function getCompraEntregaExplicitConfig() {
  const conf = tenantConfig && typeof tenantConfig === 'object' ? tenantConfig : {};
  const keys = [
    'habilitar_compras_entregas','habilitarComprasEntregas',
    'compras_entregas_habilitado','comprasEntregasHabilitado'
  ];
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(conf, key)) {
      return parseBoolLike(conf[key], false);
    }
  }
  return null;
}

function isCompraEntregaSessionEnabled() {
  if (habilitar_odbc_manager !== true) return false;
  if (!ASISTO_MULTI_WORKER) return true;

  // En multi-sesión ODBC puede estar disponible para varias funciones, pero el
  // loop de compras/entregas debe tener UN solo dueño. Si no se indica uno
  // explícitamente, queda en la sesión de API Mensajes (SDG hoy).
  const explicit = getCompraEntregaExplicitConfig();
  if (explicit !== null) return explicit === true;
  return consulta_api_mensajes_habilitado === true;
}


async function startCompraEntregaLoopIfEnabled(source = '') {
  try {
    
    if (!isCompraEntregaSessionEnabled()) {
      const motivo = habilitar_odbc_manager !== true ? 'habilitar_odbc_manager=false' : 'multi_session_no_designada';
      try {
        const msg = 'queryAccessComprasEntregas no inicia: ' + motivo +
          ' tenant=' + String(tenantId || '') + ' numero=' + String(numero || '') +
          (source ? ' source=' + source : '');
        console.log(msg);
        EscribirLog(msg, 'event');
      } catch {} 
      return;
    }
    if (compraEntregaQueryRunning) return;
    queryAccessComprasEntregas(source).catch((e) => {
      compraEntregaQueryRunning = false;
      try { console.log('queryAccessComprasEntregas fatal:', e?.message || e); } catch {}
      try { EscribirLog('queryAccessComprasEntregas fatal: ' + String(e?.message || e), 'error'); } catch {}
    });
  } catch (e) {
    try { console.log('startCompraEntregaLoopIfEnabled error:', e?.message || e); } catch {}
    try { EscribirLog('startCompraEntregaLoopIfEnabled error: ' + String(e?.message || e), 'error'); } catch {}
  }
}

async function queryAccessComprasEntregas(source = '') {
  if (!isCompraEntregaSessionEnabled()) {
    try { EscribirLog('queryAccessComprasEntregas: sesión no habilitada tenant=' + String(tenantId || '') + ' numero=' + String(numero || ''), 'event'); } catch {}
    return;
  }

  if (compraEntregaQueryRunning) {
    try { EscribirLog('queryAccessComprasEntregas: ya estaba corriendo, no se inicia otro loop', 'event'); } catch {}
    return;
  }

  compraEntregaQueryRunning = true;
  compraEntregaQueryStopRequested = false;

  try {
   const odbcRuntime = getOdbcModule();
    if (!odbcRuntime) {
      try { EscribirLog('queryAccessComprasEntregas: odbc no disponible', 'error'); } catch {}
      return;
    }

    if (!client || !client.info || !client.info.me) {
      try { EscribirLog('queryAccessComprasEntregas: client.info no disponible, se omite inicio del loop', 'event'); } catch {}
      return;
    }

    var telefono = normalizarNroTelFromApiMensajes(telefono_qr || numero) + '@c.us';
    console.log("Telefono Habilitado:" + telefono);
    console.log("cliente:" + normalizarNroTelFromApiMensajes(client.info.me.user) + '@c.us');
    telefono_local = normalizarNroTelFromApiMensajes(client.info.me.user) + '@c.us';

    if (telefono != telefono_local) {
      console.log(telefono_local + ' ' + telefono);
      console.log("TELEFONO NO AUTORIZADO A UTILIZAR WSCHATBOT!!!");
     io.emit('message', "TELEFONO NO AUTORIZADO A UTILIZAR WSCHATBOT!!!");
      return;
    }

    try { if (compraEntregaConnection && typeof compraEntregaConnection.close === 'function') await compraEntregaConnection.close(); } catch {}
    try {
      compraEntregaConnection = await odbcRuntime.connect('DSN=' + dsn + '; charset=UTF8');
    } catch (e) {
      const msg = 'queryAccessComprasEntregas: no conecta ODBC DSN=' + dsn + ' -> ' + String(e?.message || e);
      console.log(msg);
      EscribirLog(msg, 'error');
      return;
    }

    console.log("conectado a Manager..." + dsn);
    console.log("esperando...");
    await sleep(1000);

    while (!compraEntregaQueryStopRequested && isOwner && clientStarted && client) {
      if (!isCompraEntregaSessionEnabled()) {
        try { EscribirLog('queryAccessComprasEntregas: detenido; sesión no habilitada tenant=' + String(tenantId || '') + ' numero=' + String(numero || ''), 'event'); } catch {}
        break;
      }
      RecuperarJsonConfMensajes();
      await enviar_mensajes_compra();
      await enviar_mensajes_entrega();
      if (habilitar_mensajes_info === true) {
        await enviar_mensajes_info();
      }
      if (compraEntregaQueryStopRequested || !isOwner || !clientStarted || !client) break;
      await sleep(seg_msg);
    }
  } finally {
    compraEntregaQueryRunning = false;
    try {
      if (compraEntregaConnection && typeof compraEntregaConnection.close === 'function') {
        await compraEntregaConnection.close();
      }
    } catch {}
    try { compraEntregaConnection = null; } catch {}
  }
}

async function enviar_mensajes_compra() {
  if (!compraEntregaConnection) return;
  const data2 = await compraEntregaConnection.query("select  codigo, clientes.razon_social, clientes.telefono, direccion_entrega, hora_desde, hora_hasta, total from clientes, es_datos_entregas, ven_remitos_cabecera, es_horarios where es_datos_entregas.cod_horario = es_horarios.cod_horario and ven_remitos_cabecera.transaccion = es_datos_entregas.transaccion and ven_remitos_cabecera.letra = es_datos_entregas.letra and ven_remitos_cabecera.nrotransaccion = es_datos_entregas.nrotransaccion and  ven_remitos_cabecera.ptodeventa = es_datos_entregas.ptodeventa and  ven_remitos_cabecera.cliente = clientes.codigo and     es_datos_entregas.observaciones = 'obs'");
  var tam2 = data2.length;

  for (let i = 0; i <= tam2 - 1; i++) {
    const jid = toChatId(data2[i].telefono);
    if (!jid) {
      console.log("COMPRA: tel invalido ->", data2[i].telefono, data2[i].razon_social);
      io.emit('message', 'COMPRA: tel invalido -> ' + data2[i].razon_social);
      continue;
   }

    console.log('COMPRA: ' + data2[i].codigo + ' ' + data2[i].razon_social + ' ' + jid);
    io.emit('message', 'COMPRA: ' + data2[i].codigo + ' ' + data2[i].razon_social + ' ' + jid);

    await sleep(1000);

    const isReg = await client.isRegisteredUser(jid).catch(() => false);
    if (!isReg) {
      console.log("COMPRA: numero NO registrado en WhatsApp ->", jid, data2[i].razon_social);
      io.emit('message', 'COMPRA: numero NO registrado -> ' + data2[i].razon_social + ' ' + jid);
      continue;
    }

    try {
      const msgCompraCliente = '*👋 Hola ' + data2[i].razon_social + '*\nGracias por su compra...\n🛒 Tu súper Online en Venado Tuerto\n\nwww.supermercadodigital.com.ar\n\n_Mensaje enviado por Asisto Bot_\n_https://www.asistobot.com.ar_';
      const msgCompraAdmin = '*COMPRA: ' + data2[i].razon_social + '* \n ' + data2[i].hora_desde + '\n' + '$ ' + data2[i].total;

      if (compra_mensajes_usar_api_alta) {
        await altaApiMensajeCompra({
          nroTel: jid,
          mensaje: msgCompraCliente,
          identificacion1: String(data2[i].codigo || data2[i].razon_social || 'compra')
        });
        await altaApiMensajeCompra({
          nroTel: '5493462674128',
          mensaje: msgCompraAdmin,
          identificacion1: 'COMPRA_ADMIN_1'
        });
        await altaApiMensajeCompra({
          nroTel: '5493462541989',
          mensaje: msgCompraAdmin,
          identificacion1: 'COMPRA_ADMIN_2'
        });
      } else {
        await client.sendMessage(jid, msgCompraCliente, { sendSeen: false });
        await client.sendMessage('5493462674128@c.us', msgCompraAdmin, { sendSeen: false });
        await client.sendMessage('5493462541989@c.us', msgCompraAdmin, { sendSeen: false });
      }

      await compraEntregaConnection.query("update es_datos_entregas set observaciones = '*'");
    } catch (e) {
      console.log("COMPRA: API Alta/sendMessage ERROR ->", jid, e?.message || e);
      io.emit('message', 'COMPRA: API Alta/sendMessage ERROR -> ' + data2[i].razon_social + ' ' + jid);
    }
  }
}

async function enviar_mensajes_entrega() {
  if (!compraEntregaConnection) return;
  const data1 = await compraEntregaConnection.query("select  codigo, clientes.razon_social, clientes.telefono, direccion_entrega, hora_desde, hora_hasta from clientes, es_datos_entregas, ven_remitos_cabecera, es_horarios where es_datos_entregas.cod_horario = es_horarios.cod_horario and ven_remitos_cabecera.transaccion = es_datos_entregas.transaccion and ven_remitos_cabecera.letra = es_datos_entregas.letra and ven_remitos_cabecera.nrotransaccion = es_datos_entregas.nrotransaccion and  ven_remitos_cabecera.ptodeventa = es_datos_entregas.ptodeventa and  ven_remitos_cabecera.cliente = clientes.codigo and     es_datos_entregas.observaciones = 'e'");
  var tam1 = data1.length;

  for (let j = 0; j <= tam1 - 1; j++) {
    const jid = toChatId(data1[j].telefono);
    if (!jid) continue;

    console.log("telefono " + jid);
    var telefono_api = validarTelefono(jid.replace("@c.us", ""));
    console.log("telefono_api " + telefono_api);
    console.log('ENTREGA: ' + data1[j].codigo + ' ' + data1[j].razon_social + ' ' + jid);
    io.emit('message', 'ENTREGA: ' + data1[j].codigo + ' ' + data1[j].razon_social + ' ' + jid);
    await sleep(5000);

    const desde = data1[j].hora_desde;
    const hora_d = String(desde || '').substr(10, 6);
    const hasta = data1[j].hora_hasta;
    const hora_h = String(hasta || '').substr(10, 6);

    const isReg = await client.isRegisteredUser(jid).catch(() => false);
    if (!isReg) {
      console.log("ENTREGA: numero NO registrado ->", jid);
      continue;
    }

    const msgEntregaAdmin = 'Mensaje Entrega enviado a: ' + data1[j].razon_social + ' ' + hora_d + ' a ' + hora_h + ' en la direccion ' + data1[j].direccion_entrega;
    const msgEntregaCliente = '*👋 Hola ' + data1[j].razon_social + '*\nTu pedido está en camino...\nserá entregado de ' + hora_d + ' a ' + hora_h + ' en la direccion ' + data1[j].direccion_entrega + ' \n🛒 Tu súper Online en Venado Tuerto\n\nwww.supermercadodigital.com.ar\n\n_Mensaje enviado por Asisto Bot_\n_https://www.asistobot.com.ar_';

    try {
      await safeSendMessage('5493462674128@c.us', msgEntregaAdmin);
      if (entrega_mensajes_usar_api_alta) {
        await altaApiMensajeEntrega({
          nroTel: jid,
          mensaje: msgEntregaCliente,
          identificacion1: String(data1[j].codigo || data1[j].razon_social || 'entrega')
        });
      } else {
        await safeSendMessage(jid, msgEntregaCliente);
      }

      await compraEntregaConnection.query("update es_datos_entregas set observaciones = '*'");
    } catch (e) {
      console.log("ENTREGA: API Alta/sendMessage ERROR ->", jid, e?.message || e);
      io.emit('message', 'ENTREGA: API Alta/sendMessage ERROR -> ' + data1[j].razon_social + ' ' + jid);
    }
  }
}



async function enviar_mensajes_info() {
  if (!compraEntregaConnection) return;

  const origenLocal = onlyDigits(telefono_qr).slice(-10);
  console.log('MENSAJES_INFO: consultando es_mensajes origen=' + telefono_qr + ' origen_local=' + origenLocal);
  const data = await compraEntregaConnection.query(
    "select first * from es_mensajes " +
    "where estado <> 'S' and tipo = 'WS' " +
    "and right(cast(origen as varchar(30)), 10) = '" + origenLocal + "' " +
    "order by prioridad asc"
  );
 const tam = data.length;
  console.log('MENSAJES_INFO: pendientes=' + tam);

  for (let i = 0; i < tam; i++) {
    const data_img = await compraEntregaConnection.query("select first * from gen_imagenes where cod_imagen =" + data[i].cod_imagen);
    const tam_img = data_img.length;
    const segundos = Math.random() * (seg_hasta - seg_desde) + seg_desde;

    let arrayTelefono = String(data[i].destino || '').split(';');
    const tam2 = arrayTelefono.length;

    for (let j = 0; j < tam2; j++) {
      // Misma lógica de app_chatbot_super: normalizo y armo JID antes de validar/enviar.
      const jid = toChatId(arrayTelefono[j]);
      if (!jid) continue;

      const telefono_api = validarTelefono(jid.replace('@c.us', ''));

      if (telefono_api == true) {
        const msg_utf = new String(data[i].cuerpo, 'UTF-8');
        console.log('MENSAJE: ' + data[i].asunto + ' ' + msg_utf + ' ' + jid);
        io.emit('message', 'MENSAJE: ' + data[i].asunto + ' ' + msg_utf + ' ' + jid + ' ' + segundos);

        if (tam_img > 0) {
          console.log('img ' + data_img[0].path);

          function detectMimeType(b64) {
            for (var s in signatures) {
              if (b64.indexOf(s) === 0) return signatures[s];
            }
            return mime.lookup(data_img[0].nombre || data_img[0].path || '') || 'application/octet-stream';
          }

          const fileData = fs.readFileSync(data_img[0].path, { encoding: 'base64' });
          console.log('tipo de dato: ' + detectMimeType(fileData));

          const media = new MessageMedia(detectMimeType(fileData), fileData, data_img[0].nombre);
          const isReg = await client.isRegisteredUser(jid).catch(() => false);
          if (!isReg) {
            console.log('numero no registrado');
            await compraEntregaConnection.query("update es_mensajes set motivo_no_envio = 'numero no registrado' where id='" + data[i].id + "'");
          } else {
            // Guardamos wsMsgId -> id DB para que message_ack no dependa sólo de id_msg.
            const sent = await safeSendMessage(jid, media, { caption: String(msg_utf) });
            if (sent?.id?.id) pendingAck.set(sent.id.id, data[i].id);
            if (!id_msg) id_msg = data[i].id;
          }
        } else {
          const isReg2 = await client.isRegisteredUser(jid).catch(() => false);
          if (!isReg2) {
            console.log('numero no registrado');
            await compraEntregaConnection.query("update es_mensajes set motivo_no_envio = 'numero no registrado' where id='" + data[i].id + "'");
          } else {
            const sent2 = await safeSendMessage(jid, String(msg_utf));
            if (sent2?.id?.id) pendingAck.set(sent2.id.id, data[i].id);
            if (!id_msg) id_msg = data[i].id;
          }
        }
      } else {
        console.log('TELEFONO BLOQUEADO: ' + arrayTelefono[j]);
        io.emit('message', 'TELEFONO BLOQUEADO: ' + arrayTelefono[j]);
      }

      await compraEntregaConnection.query("update es_mensajes set estado = 'S' where id='" + data[i].id + "'");

      let l_fecha = new Date();
      const numberOfMlSeconds = l_fecha.getTime();
      const addMlSeconds = 180 * 60000;
      l_fecha = new Date(numberOfMlSeconds - addMlSeconds);
      let l_fecha_txt = l_fecha.toISOString();
      l_fecha_txt = l_fecha_txt.replace('T', ' ');
      l_fecha_txt = l_fecha_txt.replace('Z', '');

      await compraEntregaConnection.query("update es_mensajes set fecha_envio = '" + l_fecha_txt + "' where id='" + data[i].id + "'");
      // No pisar id_msg si todavía esperamos ACK de otro envío.
      if (!id_msg) id_msg = data[i].id;

      console.log('segundos espera. ' + segundos);
      await sleep(segundos);
    }
  }
}


const ADMIN_COMMAND_PHONE_FALLBACK = '5493462674128';

function samePhoneDigits(a, b) {
  const da = onlyDigits(a);
  const db = onlyDigits(b);
  if (!da || !db) return false;
  if (da === db) return true;
  if (da.length >= 10 && db.length >= 10 && da.slice(-10) === db.slice(-10)) return true;
  return false;
}

function getAdminCommandPhones() {
  const raw = [];
  const addRaw = (v) => {
    if (Array.isArray(v)) {
      for (const item of v) addRaw(item);
      return;
    }
    if (v && typeof v === 'object') {
      for (const item of Object.values(v)) addRaw(item);
      return;
    }
    const s = String(v || '').trim();
    if (s) raw.push(s);
  };

  addRaw(process.env.ASISTO_ADMIN_COMMAND_PHONE);
  addRaw(process.env.ADMIN_COMMAND_PHONE);
  addRaw(process.env.SUPER_ADMIN_PHONE);
  addRaw(tenantConfig?.admin_command_phone);
  addRaw(tenantConfig?.adminCommandPhone);
  addRaw(tenantConfig?.admin_command_phones);
  addRaw(tenantConfig?.adminCommandPhones);
  addRaw(tenantConfig?.api_mensajes_admin_phone);
  addRaw(tenantConfig?.apiMensajesAdminPhone);
  addRaw(getApiMensajesNroTelFrom());
  addRaw(ADMIN_COMMAND_PHONE_FALLBACK);

  return Array.from(new Set(raw.map(onlyDigits).filter(Boolean)));
}

async function resolveMessagePhoneCandidates(message) {
  const out = new Set();
  const add = (value) => {
    const raw = String(value || '').trim();
    if (!raw || looksLikeLid(raw)) return;
    const d = onlyDigits(raw);
    if (d) out.add(d);
  };

  add(message?.from);
  add(message?.author);
  add(message?.to);
  add(message?.id?.remote);
  add(message?._data?.from);
  add(message?._data?.author);
  add(message?._data?.to);
  add(message?._data?.id?.remote);

  try {
    const resolved = await resolvePhoneFromIncomingMessage(message);
    add(resolved);
  } catch {}

  try {
    if (typeof message?.getContact === 'function') {
      const c = await message.getContact();
      add(c?.number);
      add(c?.id?.user);
      add(c?.id?._serialized);
      add(c?._data?.id?.user);
      add(c?._data?.id?._serialized);
      add(c?._data?.number);
      add(c?._data?.wid?.user);
      add(c?._data?.wid?._serialized);
      add(c?._data?.userid);
      add(c?._data?.phone);
    }
  } catch {}

  for (const id of [message?.from, message?.author, message?.to].map(x => String(x || '').trim()).filter(Boolean)) {
    try {
      const c = await client.getContactById(id);
      add(c?.number);
      add(c?.id?.user);
      add(c?.id?._serialized);
      add(c?._data?.id?.user);
      add(c?._data?.id?._serialized);
      add(c?._data?.number);
      add(c?._data?.wid?.user);
      add(c?._data?.wid?._serialized);
      add(c?._data?.userid);
      add(c?._data?.phone);
    } catch {}
  }

  return Array.from(out).filter(Boolean);
}

async function isAdminCommandSender(message) {
  if (!message) return false;
  if (message.fromMe === true) return true;

  const adminPhones = getAdminCommandPhones();
  if (!adminPhones.length) return false;

  const candidates = await resolveMessagePhoneCandidates(message);
  const ok = candidates.some(c => adminPhones.some(a => samePhoneDigits(c, a)));
  if (!ok && String(message?.from || '').includes('@lid')) {
    try {
      console.log('[admin-command] remitente @lid no resuelto como admin. from=' + message.from + ' candidates=' + candidates.join(','));
    } catch {}
  }
  return ok;
}

function adminReplyTarget(message) {
  if (message?.fromMe === true) return String(message?.to || message?.from || '').trim();
  const from = String(message?.from || '').trim();
  if (from) return from;
  const admins = getAdminCommandPhones();
  return admins.length ? (admins[0] + '@c.us') : '';
}

async function safeSendMessage(to, content, opts) {
  const payload = (content === undefined || content === null) ? '' : content;
  return await safeSend(to, payload, opts || { sendSeen: false });
}

function normalizeWwebTargetChatId(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (/@(c\.us|lid|g\.us|s\.whatsapp\.net)$/i.test(raw)) return raw;
  const digits = onlyDigits(raw);
  if (digits) return digits + '@c.us';
  return raw;
}


function getMessageBodyText(message) {
  return String(message?.body || message?._data?.body || '').trim();
}


function getWwebIncomingMediaMaxBytes() {
  const raw =
    tenantConfig?.wweb_incoming_media_max_bytes ??
    tenantConfig?.wwebIncomingMediaMaxBytes ??
    process.env.WWEB_INCOMING_MEDIA_MAX_BYTES ??
    process.env.ASISTO_WWEB_INCOMING_MEDIA_MAX_BYTES ??
    (15 * 1024 * 1024);
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : (15 * 1024 * 1024);
}

function normalizeIncomingMessageType(message) {
  return String(message?.type || message?._data?.type || 'chat').trim().toLowerCase() || 'chat';
}

function getIncomingMediaFilename(message, media, messageType) {
  const fromWa =
    message?._data?.filename ||
   message?._data?.fileName ||
    message?._data?.title ||
    media?.filename ||
    '';
  if (fromWa) return String(fromWa);

  const mimeType = String(media?.mimetype || message?._data?.mimetype || '').split(';')[0].trim();
  const ext = mimeType ? (mime.extension(mimeType) || '') : '';
  const cleanExt = ext ? ('.' + ext) : '';

  if (messageType === 'image') return 'imagen' + cleanExt;
  if (messageType === 'document') return 'documento' + cleanExt;
  if (messageType === 'audio' || messageType === 'ptt' || messageType === 'voice') return 'audio' + (cleanExt || '.ogg');
  if (messageType === 'video') return 'video' + cleanExt;
  return 'archivo' + cleanExt;
}

function buildIncomingMediaText(messageType, caption, filename, mimeType, mediaAttached) {
  const cap = String(caption || '').trim();
  const file = filename ? (' Archivo: ' + filename + '.') : '';
  const mimeTxt = mimeType ? (' Tipo: ' + mimeType + '.') : '';

  if (messageType === 'audio' || messageType === 'ptt' || messageType === 'voice' || /^audio\//i.test(String(mimeType || ''))) {
    // Sin el archivo real no enviamos un texto ficticio a ChatGPT, porque termina
    // entrando al historial como si fuera lo que dijo el cliente.
    if (!mediaAttached) return cap;
    return cap || ('El cliente envió un audio por WhatsApp Web.' + file + mimeTxt);
  }

  if (messageType === 'image') {
    return cap || ('El cliente envió una imagen adjunta por WhatsApp Web. Si estabas esperando el comprobante de transferencia, tomalo como comprobante enviado.' + file + mimeTxt);
  }

  if (messageType === 'document') {
    return cap || ('El cliente envió un documento adjunto por WhatsApp Web. Si estabas esperando el comprobante de transferencia, tomalo como comprobante enviado.' + file + mimeTxt);
  }

  if (messageType === 'video') {
    return cap || ('El cliente envió un video adjunto por WhatsApp Web.' + file + mimeTxt);
 }

  if (messageType === 'sticker') {
    return cap || 'El cliente envió un sticker por WhatsApp Web.';
  }

  if (mediaAttached) {
    return cap || ('El cliente envió un archivo adjunto por WhatsApp Web.' + file + mimeTxt);
  }

  return cap;
}

function promiseWithTimeout(promise, timeoutMs, label) {
  const ms = Math.max(1000, Number(timeoutMs) || 12000);
  return Promise.race([
    Promise.resolve(promise),
    new Promise((_, reject) => {
      const timer = setTimeout(() => reject(new Error(String(label || 'operation') + '_timeout_' + ms + 'ms')), ms);
      if (timer && typeof timer.unref === 'function') timer.unref();
    })
  ]);
}





function writeIncomingAudioTrace(message, level = 'event') {
  const msg = String(message || '');
  try { console.log(msg); } catch {}
  try {
    fs.appendFileSync(
      path.join(__dirname, 'app_asisto_audio.log'),
      '[' + nowArgentinaISO() + '] ' + msg + '\n',
      'utf8'
    );
  } catch {}
  try { EscribirLog(msg, level === 'error' ? 'error' : 'event'); } catch {}
}


async function downloadIncomingMediaReliable(message, messageType) {
  if (!message) {
    return { media: null, error: 'message_missing', source: '', attempt: 0, stableId: '' };
  }

  const serialized = String(
    message?.id?._serialized ||
    message?._data?.id?._serialized ||
    ''
  ).trim();

  const rawId = String(message?.id?.id || message?._data?.id?.id || '').trim();
  const remote = String(
    message?.id?.remote ||
    message?._data?.id?.remote ||
    message?.from ||
    message?._data?.from ||
    ''
  ).trim();
  const stableId = serialized || rawId;
  const errors = [];

  // Camino normal. No se altera message.hasMedia: el wrapper puede venir viejo,
  // pero el mensaje vivo del navegador se resuelve en el bloque siguiente.
  if (message.hasMedia === true && typeof message.downloadMedia === 'function') {
     try {   
      const media = await promiseWithTimeout(message.downloadMedia(), 12000, 'message_downloadMedia');
      if (media?.data) {
        return { media, error: '', source: 'message.downloadMedia', attempt: 1, stableId };
      }
      errors.push('message.downloadMedia_empty');
    } catch (e) {
    errors.push('message.downloadMedia:' + String(e?.message || e));
    }
  } else {
  

    errors.push('message.hasMedia_false');
  }

  if (!client?.pupPage || typeof client.pupPage.evaluate !== 'function') {
    return { media: null, error: errors.concat('pupPage_not_available').join(' | '), source: '', attempt: 1, stableId };
  }

  const lookup = {
    serialized,
    rawId,
    remote,
    mimetype: String(message?._data?.mimetype || '').trim(),
    filename: String(message?._data?.filename || message?._data?.fileName || '').trim()
  };

  // Mismo recorrido que usa whatsapp-web.js actual:
  // resolveMediaBlob() o, si no existe en la versión instalada,
  // msg.downloadMedia() + WAWebMediaInMemoryBlobCache.
  for (const [index, delay] of [0, 500, 1500].entries()) {
    if (delay) await sleep(delay);

    let result;
    try {
      result = await promiseWithTimeout(
        client.pupPage.evaluate(async (info) => {
          const fail = (error, msg) => ({
            ok: false,
            error: String(error || 'media_not_available'),
            stage: String(msg?.mediaData?.mediaStage || ''),
            hasMediaData: !!msg?.mediaData,
            hasMediaObject: !!msg?.mediaObject,
            hasDirectPath: !!msg?.directPath,
            wwebVersion: String(window.Debug?.VERSION || '')
          });

          const idText = (value) => {
            if (!value) return '';
            if (typeof value === 'string') return value;
            return String(value._serialized || value.id || '');
          };

          const toBase64 = async (blobValue) => {
            let blob = blobValue;
            if (blob?.blob) blob = blob.blob;
            if (blob && typeof blob.forceToBlob === 'function') blob = blob.forceToBlob();
            if (!blob || typeof blob.arrayBuffer !== 'function') return '';
            const ab = await blob.arrayBuffer();
            if (typeof window.WWebJS?.arrayBufferToBase64Async === 'function') {
              return await window.WWebJS.arrayBufferToBase64Async(ab);
            }
            return await new Promise((resolve, reject) => {
              const reader = new FileReader();
              reader.onload = () => resolve(String(reader.result || '').split(',')[1] || '');
              reader.onerror = reject;
              reader.readAsDataURL(new Blob([ab]));
            });
          };

          let Msg;
          try { Msg = window.require('WAWebCollections').Msg; }
          catch (e) { return fail('WAWebCollections:' + String(e?.message || e)); }

          let msg = null;
          try {
            if (info.serialized) msg = Msg.get(info.serialized) || null;
            if (!msg && info.serialized && typeof Msg.getMessagesById === 'function') {
              msg = (await Msg.getMessagesById([info.serialized]))?.messages?.[0] || null;
            }
            if (!msg && info.rawId) {
              const list = typeof Msg.getModelsArray === 'function' ? Msg.getModelsArray() : (Msg._models || []);
              msg = (list || []).find((m) => {
                const sameId = String(m?.id?.id || '') === info.rawId;
                const mRemote = idText(m?.id?.remote);
                return sameId && (!info.remote || !mRemote || mRemote === info.remote);
              }) || null;
            }
          } catch (e) {
            return fail('message_lookup:' + String(e?.message || e));
           }

          if (!msg) return fail('message_not_found_in_store');

          // Versiones nuevas.
          if (typeof window.WWebJS?.resolveMediaBlob === 'function') {
            try {
              const resolved = await window.WWebJS.resolveMediaBlob(idText(msg.id) || info.serialized);
              if (resolved?.blob) {
                const data = await toBase64(resolved.blob);
                if (data) return {
                  ok: true,
                  source: 'resolveMediaBlob',
                  data,
                  mimetype: String(resolved.mimetype || msg.mimetype || info.mimetype || 'audio/ogg; codecs=opus'),
                  filename: String(resolved.filename || msg.filename || info.filename || '')
                };
              }
            } catch (e) {}
          }
 


          // Versiones donde resolveMediaBlob todavía no existe.
          try {
            await msg.downloadMedia({
              downloadEvenIfExpensive: true,
              rmrReason: 1,
              isUserInitiated: true
            });
          } catch (e) {}

          let blob = null;
          try {
            const filehash = msg.mediaObject?.filehash || msg.filehash;
            const cache = window.require('WAWebMediaInMemoryBlobCache').InMemoryMediaBlobCache;
            if (filehash && cache?.get) blob = cache.get(filehash) || null;
          } catch (e) {}
          if (!blob) blob = msg.mediaObject?.mediaBlob || msg.mediaData?.mediaBlob || null;

          const data = await toBase64(blob);
          if (!data) return fail('media_blob_not_resolved', msg);
   

          return {
            ok: true,
            source: 'mediaBlobCache',
            data,
            mimetype: String(msg.mimetype || info.mimetype || 'audio/ogg; codecs=opus'),
            filename: String(msg.filename || info.filename || '')
          };
        }, lookup),
        20000,
        'browser_resolve_media_blob'
      );
    } catch (e) {
      result = { ok: false, error: String(e?.message || e) };
    }

    if (result?.ok && result.data) {
      const media = {
        mimetype: String(result.mimetype || lookup.mimetype || 'audio/ogg; codecs=opus'),
        data: String(result.data),
        filename: String(result.filename || lookup.filename || '')
      };
      writeIncomingAudioTrace(
        '[BOT_AUDIO_DOWNLOAD] ok source=' + String(result.source || 'browser') +
        ' attempt=' + String(index + 1) +
        ' id=' + stableId +
        ' bytes=' + Buffer.byteLength(media.data, 'base64')
      );
      return { media, error: '', source: String(result.source || 'browser'), attempt: index + 1, stableId };
    }

    const detail = String(result?.error || 'media_not_available');
    errors.push('browser_' + String(index + 1) + ':' + detail);
    writeIncomingAudioTrace(
      '[BOT_AUDIO_DOWNLOAD] fail attempt=' + String(index + 1) +
      ' id=' + stableId +
      ' error=' + detail +
      ' stage=' + String(result?.stage || '') +
      ' mediaData=' + String(!!result?.hasMediaData) +
      ' mediaObject=' + String(!!result?.hasMediaObject) +
      ' directPath=' + String(!!result?.hasDirectPath) +
      ' wweb=' + String(result?.wwebVersion || ''),
      'error'
    );
  }

  return { media: null, error: errors.join(' | '), source: 'browser', attempt: 3, stableId };

}


async function buildIncomingBotPayload(message) {
  const messageType = normalizeIncomingMessageType(message);
  const caption = String(message?.caption || message?._data?.caption || '').trim();
  const body = String(message?.body || message?._data?.body || '').trim();

  if (messageType === 'chat') {
    return {
      mensaje: body,
      type: 'chat',
      hasMedia: false,
      media: null
    };
  }

  let media = null;
  let mediaError = '';

  let mediaDownloadSource = '';
  let mediaDownloadAttempt = 0;
  let mediaStableId = '';

  // El log se escribe antes de descargar para que un PTT nunca quede invisible mientras espera.
  if (['audio', 'ptt', 'voice'].includes(messageType)) {
    const audioStartLog = '[BOT_AUDIO] recibido from=' + String(message?.from || '') +
      ' id=' + String(message?.id?._serialized || message?._data?.id?._serialized || '') +
      ' hasMedia=' + String(message?.hasMedia === true);
    writeIncomingAudioTrace(audioStartLog, 'event');
  }


  // Lectura robusta del adjunto entrante de WhatsApp Web.
  const downloaded = await downloadIncomingMediaReliable(message, messageType);
  media = downloaded.media;
  mediaError = downloaded.error || '';
  mediaDownloadSource = downloaded.source || '';
  mediaDownloadAttempt = downloaded.attempt || 0;
  mediaStableId = downloaded.stableId || '';
  // Mismo fallback que el script que ya reenvía correctamente los adjuntos:
  // algunas versiones dejan el base64 directamente en message._data.body.
  if ((!media || !media.data) && typeof message?._data?.body === 'string') {
    const rawBody = String(message._data.body || '').replace(/\s+/g, '');
    const looksLikeBase64 =
      rawBody.length >= 128 &&
      rawBody.length % 4 === 0 &&
      /^[A-Za-z0-9+/]+={0,2}$/.test(rawBody);

    if (looksLikeBase64) {
      media = {
        mimetype: String(message?._data?.mimetype || 'application/octet-stream'),
        data: rawBody,
        filename: String(message?._data?.filename || message?._data?.fileName || '')
      };
    }
  }

  if ((!media || !media.data) && mediaError) {
    try { EscribirLog('[BOT_MEDIA] downloadMedia error type=' + messageType + ': ' + mediaError, 'error'); } catch {}
  }

  const mimeType = String(media?.mimetype || message?._data?.mimetype || '').trim();
  const filename = getIncomingMediaFilename(message, media, messageType);
  const mediaBytes = media?.data ? Buffer.byteLength(String(media.data), 'base64') : 0;
  const maxBytes = getWwebIncomingMediaMaxBytes();
  const includeBase64 = !!(media?.data && mediaBytes <= maxBytes);

  if (media?.data && !includeBase64) {
    try {
      EscribirLog('[BOT_MEDIA] adjunto no se envía al API por tamaño. type=' + messageType + ' bytes=' + mediaBytes + ' max=' + maxBytes + ' file=' + filename, 'error');
   } catch {}
  }

  const mediaDownloaded = !!(media && media.data);
  try {
    const mediaLog = '[BOT_MEDIA] type=' + messageType +
      ' hasMediaFlag=' + String(message?.hasMedia === true) +
      ' downloaded=' + String(mediaDownloaded) +
      ' source=' + String(mediaDownloadSource || '-') +
      ' attempt=' + String(mediaDownloadAttempt || 0) +
      ' id=' + String(mediaStableId || '-') +
      ' bytes=' + String(mediaBytes) +
      ' omitted=' + String(!!(media?.data && !includeBase64)) +
      (mediaError ? (' error=' + mediaError) : '');
    console.log(mediaLog);
    EscribirLog(mediaLog, 'event');
    if (!mediaDownloaded) EscribirLog(mediaLog, 'error');
  } catch {}

  const mensaje = buildIncomingMediaText(messageType, body || caption, filename, mimeType, mediaDownloaded);

  return {
    mensaje,
    type: messageType,
    hasMedia: mediaDownloaded,
    media: media ? {
      mimetype: mimeType,
      filename,
      data: includeBase64 ? String(media.data) : '',
      bytes: mediaBytes,
      omittedBySize: !!(media?.data && !includeBase64),
      error: mediaError
    } : null,
    mediaError
  };
}

function redactIncomingApiPayloadForLog(payload) {
  try {
    const copy = JSON.parse(JSON.stringify(payload || {}));
    if (copy.MediaBase64) copy.MediaBase64 = '[base64 ' + String(copy.MediaBytes || '') + ' bytes]';
    if (copy.MediaData) copy.MediaData = '[base64 ' + String(copy.MediaBytes || '') + ' bytes]';
    if (copy.media && copy.media.data) copy.media.data = '[base64 ' + String(copy.MediaBytes || copy.media.bytes || '') + ' bytes]';
    return copy;
  } catch {
    return payload;
  }
}



function isAdminDeliveryCommandBody(body) {
  return /^\/e(?:\s|$)/i.test(String(body || '').trim());
}

function isAdminDeliveryCommandMessage(message) {
  return isAdminDeliveryCommandBody(getMessageBodyText(message));
}

async function handleAdminDeliveryCommand(message, source = '') {
  try {
    const body = getMessageBodyText(message);
    if (!body || !isAdminDeliveryCommandBody(body)) return false;

    const isAdmin = await isAdminCommandSender(message);
    if (!isAdmin) return false;

    if (!isCompraEntregaSessionEnabled()) {
      try {
        const msg = '[admin-command] /e ignorado: sesión no habilitada para compras/entregas tenant=' +
          String(tenantId || '') + ' numero=' + String(numero || '') +
          ' from=' + String(message?.from || '');
        console.log(msg);
        EscribirLog(msg, 'event');
      } catch {}
      return true;
    }

    const parts = body.split(/\s+/).filter(Boolean);
    const cmd = String(parts[0] || '').trim().toLowerCase();
    const param = String(parts[1] || '').trim().toLowerCase();
    if (cmd !== '/e') return false;

    const replyTo = adminReplyTarget(message);
    if (!replyTo) return true;

    const odbcRuntime = getOdbcModule();
    if (!odbcRuntime) {
      await safeSendMessage(replyTo, 'ODBC no está disponible en este script. No puedo consultar pedidos.');
      return true;
    }

    console.log('[admin-command] OK source=' + source + ' from=' + message.from + ' to=' + message.to + ' fromMe=' + message.fromMe + ' body=' + body);
    EscribirLog('[admin-command] OK source=' + source + ' body=' + body, 'event');

    const cmdConnection = await odbcRuntime.connect('DSN=' + dsn + '; charset=UTF8');
    try {
      if (param === 'l' || !param) {
        const data2 = await cmdConnection.query("SELECT ven_remitos_cabecera.fecha,forma_de_pago.descripcion, ven_remitos_cabecera.total, es_datos_entregas.forma_pago, es_horarios.hora_desde, clientes.razon_social, ven_remitos_cabecera.nrotransaccion , es_datos_entregas.direccion_entrega  FROM ven_remitos_cabecera, es_datos_entregas,  es_horarios, forma_de_pago ,clientes WHERE (ven_remitos_cabecera.transaccion = es_datos_entregas.transaccion ) and  (ven_remitos_cabecera.transaccion = es_datos_entregas.transaccion )and  ( ven_remitos_cabecera.letra = es_datos_entregas.letra ) and( ven_remitos_cabecera.nrotransaccion = es_datos_entregas.nrotransaccion ) and  ( ven_remitos_cabecera.ptodeventa = es_datos_entregas.ptodeventa ) and  ( es_horarios.cod_horario = es_datos_entregas.cod_horario) and( forma_de_pago.codigo = es_datos_entregas.forma_pago )   and( ven_remitos_cabecera.cliente = clientes.codigo )  and (  es_horarios.fecha > DateAdd(day,-1,GetDate() )) order by es_horarios.hora_desde ;  ");

        if (!data2 || !data2.length) {
          await safeSendMessage(replyTo, 'No hay pedidos para listar hoy.');
          return true;
        }

        for (let i = 0; i <= data2.length - 1; i++) {
          await safeSendMessage(replyTo, data2[i].razon_social + '  ' + data2[i].nrotransaccion + ' ' + data2[i].hora_desde + ' / $' + data2[i].total + ' -- ' + data2[i].direccion_entrega);
          try { console.log(data2[i].fecha); } catch {}
        }
        return true;
      }

      if (param === '?') {
        await safeSendMessage(replyTo, '*------AYUDA------* \n */e* o */e l* -> listado de pedidos \n */e 0000XXXX* -> Envía Mensaje de Entrega*');
        return true;
      }

      const nro = String(param || '').replace(/'/g, "''");
      if (!nro) {
        await safeSendMessage(replyTo, 'Falta número de pedido. Usá /e l para listar o /e 0000XXXX para marcar entrega.');
        return true;
      }

      console.log("update es_datos_entregas set observaciones = 'e' where nrotransaccion = '" + nro + "'");
      await cmdConnection.query("update es_datos_entregas set observaciones = 'e' where nrotransaccion = '" + nro + "'");
      await safeSendMessage(replyTo, 'Mensaje Entrega Actualizado');
      startCompraEntregaLoopIfEnabled('admin_delivery_command');
      return true;
    } finally {
      try { await cmdConnection.close(); } catch {}
    }
  } catch (e) {
    console.log('[admin-command] error:', e?.message || e);
    try { EscribirLog('[admin-command] error: ' + String(e?.message || e), 'error'); } catch {}
    try {
      const replyTo = adminReplyTarget(message);
      if (replyTo) await safeSendMessage(replyTo, 'Error ejecutando comando: ' + String(e?.message || e));
    } catch {}
    return true;
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


function attachClientHandlers() {

async function processIncomingAsistoMessage(message, source) {
   const rawIncomingLog = '[INCOMING_RAW] source=' + String(source || 'message') +
    ' from=' + String(message?.from || '') +
    ' to=' + String(message?.to || '') +
    ' type=' + String(message?.type || message?._data?.type || '') +
    ' id=' + String(message?.id?._serialized || message?._data?.id?._serialized || '');
  try { console.log(rawIncomingLog); } catch {}
  try { EscribirLog(rawIncomingLog, 'event'); } catch {}

  if (!shouldProcessIncomingMessage(message, source)) return;


  try { console.log('[INCOMING] source=' + String(source || 'message') + ' from=' + String(message?.from || '') + ' type=' + String(message?.type || '')); } catch {}
  try { EscribirLog('[INCOMING] source=' + String(source || 'message') + ' from=' + String(message?.from || '') + ' type=' + String(message?.type || ''), 'event'); } catch {}


  try { await refreshTenantConfigFromDbPerMessage(); } catch {}
  try { RecuperarJsonConfMensajes(); } catch {}

  if (isAdminDeliveryCommandMessage(message)) {
   // Comando interno de entrega: NUNCA debe llegar al API principal ProcesarMensajePost.
    // Si no es admin, también se corta acá para que /e no se procese como chat del bot.
    const handled = await handleAdminDeliveryCommand(message, source || 'message');
    if (!handled) {
      try { console.log('[admin-command] /e ignorado: remitente no autorizado. No se llama al API principal. from=' + String(message?.from || '')); } catch {}
      try { EscribirLog('[admin-command] /e ignorado: remitente no autorizado. No se llama al API principal. from=' + String(message?.from || ''), 'event'); } catch {}
    }
    return;
  }

  // PAUSA DE SESIÓN: antes sólo detenía ConsultaApiMensajes (salientes).
  // También debe cortar los mensajes entrantes para que no lleguen a API/ChatGPT
  // ni generen respuestas mientras el panel muestra la sesión pausada.
  try { await pollActionsOnce(); } catch {}
  let incomingPaused = false;
  try {
    incomingPaused =
      lastPolicyBlocked === true ||
      String(localWsPanelState || '').toLowerCase() === 'paused' ||
      await isWwebMessagesBlockedSafe();
  } catch {
    incomingPaused =
      lastPolicyBlocked === true ||
      String(localWsPanelState || '').toLowerCase() === 'paused';
  }

  if (incomingPaused) {
    const pauseLog = '[PAUSE] mensaje entrante ignorado tenant=' + String(tenantId || '') +
      ' from=' + String(message?.from || '') +
      ' source=' + String(source || 'message');
    try { console.log(pauseLog); } catch {}
    try { EscribirLog(pauseLog, 'event'); } catch {}
    return;
  }

  // El filtro se evalúa antes de confirmaciones, descarga de audios o cualquier
  // llamada a la lógica API/ChatGPT. Un cliente no habilitado se ignora en silencio.
  const rawAccessFrom = String(message?.from || message?._data?.from || '').trim();
  if (rawAccessFrom === 'status@broadcast') return;

  let resolvedAccessFrom = '';


  const clientAccess = await isIncomingClientAllowedLocal(resolvedAccessFrom || rawAccessFrom);
  if (!clientAccess.allowed) {
    const accessLog = '[CLIENT_ACCESS] ignorado tenant=' + String(tenantId || '') +
      ' source=' + String(source || 'message') +
      ' from=' + rawAccessFrom +
      ' normalized=' + String(clientAccess.normalized || '') +
      ' reason=' + String(clientAccess.reason || 'not_listed');
    try { console.log(accessLog); } catch {}
    try { EscribirLog(accessLog, 'event'); } catch {}
    return;
  }
  try { message.__asistoResolvedClientPhone = resolvedAccessFrom || ''; } catch {}


  if (await registrarRespuestaConfirmacionApiMensajes(message)) {
    return;
  }
  if (await registrarRespuestaNoValidaConfirmacionApiMensajes(message)) {
    
    return;
  }

  if (habilitar_bot !== true) {
    try { console.log('[BOT] deshabilitado: no se llama al API principal. from=' + String(message?.from || '')); } catch {}
    try { EscribirLog('[BOT] deshabilitado: no se llama al API principal. from=' + String(message?.from || ''), 'event'); } catch {}
    return;
  }

//if (message.from=='5493462514448@c.us'   ){

  var indice_telefono = indexOf2d(message.from);

 if(indice_telefono == -1){

  var valor_i=0;
 }else{
 var valor_i = jsonGlobal[indice_telefono][1];
 }
 
EscribirLog(message.from +' '+message.to+' '+message.type+' '+message.body ,"event");


  console.log("mensaje "+message.from);
 
  


  
    
  if( valor_i==0) {
    
    RecuperarJsonConfMensajes();
   
    var segundos = Math.random() * (devolver_seg_hasta() - devolver_seg_desde()) + devolver_seg_desde();

   
    var telefonoTo = String(
     message?.to ||
     message?._data?.to ||
     client?.info?.me?.user ||
     telefono_qr ||
     numero ||
     ''
   );
  // var telefonoFrom = message.from;

      const remoteFrom = String(message.from || '').trim();
      var telefonoFrom = String(message?.__asistoResolvedClientPhone || '').trim() ||
        await resolvePhoneFromIncomingMessage(message);
      if (remoteFrom && telefonoFrom && telefonoFrom !== stripWhatsappSuffix(remoteFrom)) {
        console.log('[LID] remitente resuelto: ' + remoteFrom + ' -> ' + telefonoFrom);
        try { EscribirLog('[LID] remitente resuelto: ' + remoteFrom + ' -> ' + telefonoFrom, 'event'); } catch {}
      } else if (looksLikeLid(remoteFrom)) {
        console.log('[LID] no se pudo resolver teléfono real para ' + remoteFrom);
        try { EscribirLog('[LID] no se pudo resolver teléfono real para ' + remoteFrom, 'error'); } catch {}
      }
    //var telefonoFrom = '5493425472992@c.us' 
   // var telefonoTo = '5493424293943@c.us'

    telefonoTo = stripWhatsappSuffix(telefonoTo);

   // telefonoFrom = telefonoFrom.replace('@c.us','');
   
    var resp = null;
 

    if(telefonoFrom == 'status@broadcast'){
      console.log("mensaje de estado");
      return
    }




    
    const incomingBotPayload = await buildIncomingBotPayload(message);
    

    const incomingType = normalizeIncomingMessageType(message);
    const incomingIsAudio = ['audio', 'ptt', 'voice'].includes(incomingType) ||
      /^audio\//i.test(String(incomingBotPayload?.media?.mimetype || message?._data?.mimetype || ''));

    // Nunca dejar un audio sin respuesta ni sin diagnóstico. Si WhatsApp Web no
    // entregó el binario, no se manda un texto ficticio a ChatGPT.
    if (incomingIsAudio && !String(incomingBotPayload?.media?.data || '').trim()) {
      const mediaError = String(
        incomingBotPayload?.mediaError ||
        incomingBotPayload?.media?.error ||
        'audio_sin_base64'
      ).trim();
      const audioFailLog = '[BOT_AUDIO] descarga fallida from=' + String(message?.from || '') +
        ' id=' + String(message?.id?._serialized || message?._data?.id?._serialized || '') +
        ' error=' + mediaError;
        writeIncomingAudioTrace(audioFailLog, 'error');
      try {
        await safeSendMessage(
          message.from,
          'No pude descargar ese audio. Reenviámelo una vez más o escribime el mensaje por acá.'
        );
      } catch (e) {
        try { EscribirLog('[BOT_AUDIO] tampoco se pudo enviar aviso: ' + String(e?.message || e), 'error'); } catch {}
      }
      return;
    }

    const incomingHasText = !!String(incomingBotPayload?.mensaje || '').trim();
    const incomingHasMediaData = !!String(incomingBotPayload?.media?.data || '').trim();
    if (!incomingBotPayload || (!incomingHasText && !incomingHasMediaData)) {
      const skipLog = '[BOT] mensaje sin texto/media procesable type=' + String(message?.type || '') +
        ' mediaError=' + String(incomingBotPayload?.mediaError || incomingBotPayload?.media?.error || '');
      console.log(skipLog);
      try { EscribirLog(skipLog, 'error'); } catch {}
      return;
    }

    if(!telefonoTo){
      const toLog = '[BOT] telefono destino propio vacío. message.to=' + String(message?.to || '') +
        ' telefono_qr=' + String(telefono_qr || '') + ' numero=' + String(numero || '');
      console.log(toLog);
      try { EscribirLog(toLog, 'error'); } catch {}
      return;
    }

    if(message.from == ''|| message.from == null){
      console.log("message.from VACIO");
      return
    }

    if(!telefonoFrom){
      console.log("telefonoFrom VACIO");
      try { EscribirLog("telefonoFrom VACIO para remote " + String(message.from || ""), "error"); } catch {}
      return
    }
    const telefonoFromApi = validPhoneCandidateForRaw(remoteFrom, telefonoFrom);
    if (!telefonoFromApi) {
      console.log('[LID] bloqueado API: candidato no es teléfono real ' + remoteFrom + ' -> ' + telefonoFrom);
      try { EscribirLog('[LID] bloqueado API: candidato no es teléfono real ' + remoteFrom + ' -> ' + telefonoFrom, 'error'); } catch {}
      return
    }
telefonoFrom = telefonoFromApi;
    try {
      await logMessageStat('in', telefonoFrom, {
        body: incomingBotPayload.mensaje || '',
        type: incomingBotPayload.type || message.type || 'chat',
        hasMedia: !!incomingBotPayload.hasMedia
      });
    } catch {}
    console.log("mensaje");
   
      //////////////////////////////////////////////////////////
      // MENSAJE DE ESPERO POR FAVOR
      ////////////////////////////////////////////////////////
      if (msg_inicio == ''|| msg_inicio == null){
      }
      else{
        safeSend(message.from,msg_inicio );
      }

      await io.emit('message', 'Mensaje: '+telefonoFrom+': '+ incomingBotPayload.mensaje );

      var jsonTexto = {
        Tel_Origen: telefonoFrom ?? "",
        Tel_Destino: telefonoTo ?? "",
        Mensaje: incomingBotPayload.mensaje ?? "",
        Respuesta: "",
        TipoMensaje: incomingBotPayload.type || message.type || 'chat',
        HasMedia: !!incomingBotPayload.hasMedia
      };

      if (incomingBotPayload.media) {
        jsonTexto.MediaMimeType = incomingBotPayload.media.mimetype || '';
        jsonTexto.MediaFilename = incomingBotPayload.media.filename || '';
        jsonTexto.MediaBytes = incomingBotPayload.media.bytes || 0;
        jsonTexto.MediaBase64 = incomingBotPayload.media.data || '';
        jsonTexto.MediaOmittedBySize = !!incomingBotPayload.media.omittedBySize;
        jsonTexto.MediaError = incomingBotPayload.media.error || incomingBotPayload.mediaError || '';
        jsonTexto.media = {
          mimetype: jsonTexto.MediaMimeType,
          filename: jsonTexto.MediaFilename,
          bytes: jsonTexto.MediaBytes
        };
      }

      // jsonTexto = {Tel_Origen:'5493462674128',Tel_Destino:'5493424293943', Mensaje:incomingBotPayload.mensaje,Respuesta:''};
 

     // let url =  api
      const botLogicMode = await getWwebBotLogicModeForPhone(telefonoTo);
      let url = getIncomingApiUrlForLogicMode(botLogicMode);

      console.log('[BOT] logic_mode=' + botLogicMode + ' url=' + url);
      try { EscribirLog('[BOT] logic_mode=' + botLogicMode + ' url=' + url, 'event'); } catch {}

      console.log(JSON.stringify(redactIncomingApiPayloadForLog(jsonTexto)));
      EscribirLog("Mensaje "+JSON.stringify(redactIncomingApiPayloadForLog(jsonTexto)),'event');
      
   try {
         // Esta llamada usa Axios, igual que el Control API que ya funciona en esta PC.
         // Se usa el dominio canónico sin redirecciones para que el POST llegue a Render
         // con el body intacto. No se reintenta automáticamente para evitar duplicar pedidos.
         const resp = await axios.post(url, jsonTexto, {
           timeout: 180000,
           maxRedirects: 0,
           maxContentLength: 20 * 1024 * 1024,
           maxBodyLength: 20 * 1024 * 1024,
           validateStatus: () => true,
           headers: { "Content-Type": "application/json; charset=UTF-8" }
         });

         const raw = typeof resp.data === 'string' ? resp.data : '';
         let json = resp.data;
         if (raw) {
           try { json = JSON.parse(raw); } catch { json = raw; }
         }

         console.log(json);

         if (resp.status < 200 || resp.status >= 300) {
           const detalle = typeof json === 'string' ? json : JSON.stringify(json);
           EscribirLog("Error 02 ApiWhatsapp - Response ERROR HTTP " + resp.status + " " + detalle, "error");
           
           if (msg_errores) await safeSend(message.from, msg_errores);
           return "error";
         }

         // Respaldo del filtro central del servidor. No se envía msg_fin ni
         // ninguna otra respuesta cuando el cliente quedó fuera del listado.
         if (json && !Array.isArray(json) && json.ignored === true) {
           try {
             EscribirLog('[CLIENT_ACCESS] servidor ignoró mensaje from=' + String(telefonoFrom || '') +
               ' reason=' + String(json.reason || ''), 'event');
           } catch {}
           return "ignored";
         }

        tam_json = 0; // 👈 evitá globals sin const/let; ideal: const tam_json = 0;
         recuperar_json(message.from, json);
         await procesar_mensaje(json, message);

         if (msg_fin) {
           await safeSend(message.from, msg_fin);
         }

         return "ok";

   } catch (err) {
     const detalleTecnico = [
       err?.message || String(err),
       err?.code ? ('code=' + err.code) : '',
       err?.response?.status ? ('http=' + err.response.status) : '',
       err?.config?.url ? ('url=' + err.config.url) : ''
     ].filter(Boolean).join(' ');
     const detalle = "Error 03 Chatbot Error " + detalleTecnico + " " + JSON.stringify(jsonTexto);
     console.log(detalle);
     EscribirLog(detalle, "error");
    
     if (msg_errores) await safeSend(message.from, msg_errores);
     return "error";
   }





////////////////////
    };
   
    var body = String(message.body || '');
    body = body.trim();
    body = body.toUpperCase();


    if(valor_i !== 0 && body == 'N' ){
      console.log("cancelar"&msg_can);
      //safeSend(message.from,'*Consulta Cancelada* ❌' );
            
      if(msg_can == '' || msg_can == undefined || msg_can == 0){
        
        
      }else{
        safeSend(message.from,msg_can );

      }
      bandera_msg=1;
      jsonGlobal[indice_telefono][2] = '';
      jsonGlobal[indice_telefono][1] = 0;
      jsonGlobal[indice_telefono][3] = '';

    
    };
    if(valor_i!==0 && ((body != 'N') && (body != 'S' ) )){
      console.log("no entiendo ->"+message.body);
      safeSend(message.from,'🤔 *No entiendo*, \nPor favor ingrese *S* o *N* para mostrar los siguientes resultados\n ' );

    };
    

    if(valor_i !== 0 && body == 'S'){
      console.log("continuar "+tam_json+' indice '+indice_telefono);
      procesar_mensaje(jsonGlobal[indice_telefono][2], message);

     }
//}  //
}

client.on('message_create', async message => {
  try {
    const createRawLog = '[MESSAGE_CREATE_RAW] fromMe=' + String(!!message?.fromMe) +
      ' from=' + String(message?.from || message?._data?.from || '') +
      ' to=' + String(message?.to || message?._data?.to || '') +
      ' type=' + String(message?.type || message?._data?.type || '') +
      ' id=' + String(message?.id?._serialized || message?._data?.id?._serialized || '');
    try { console.log(createRawLog); } catch {}
    try { EscribirLog(createRawLog, 'event'); } catch {}
    try {
      const b = String(message?.body || message?._data?.body || '').trim();
      if (api_mensajes_confirmacion_habilitada === true && b && respuestaConfirmaApiMensajes(b)) {
        logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] message_create raw fromMe=' + String(!!message?.fromMe) +
          ' from=' + String(message?.from || message?._data?.from || '') +
          ' to=' + String(message?.to || message?._data?.to || '') +
          ' remote=' + String(message?.id?.remote || message?._data?.id?.remote || '') +
          ' type=' + String(message?.type || message?._data?.type || '') +
          ' body=' );
      }
    } catch {}
    if (message && message.fromMe === true) {
      await logOutgoingFromMessageFallback(message);
      if (isAdminDeliveryCommandMessage(message)) {
        try {
          const msg = '[admin-command] /e saliente ignorado en sesión emisora tenant=' +
            String(tenantId || '') + ' numero=' + String(numero || '') +
            ' to=' + String(message?.to || '');
          console.log(msg);
          EscribirLog(msg, 'event');
        } catch {}
        return;
      }

      // En modo ChatGPT/Pedidos, avisar a Render sobre mensajes enviados por el
      // operador desde el propio teléfono/WhatsApp. Render deduplica los que
      // salieron del bot o del panel y procesa únicamente los manuales reales.
      await notifyWwebOperatorOutgoingMessage(message);

      // IMPORTANTE: si el operador prueba/autoriza desde el mismo WhatsApp Web,
      // el mensaje sale como fromMe=true. En algunas versiones message.to viene vacío;
      // por eso se toma el destino desde to/from/id.remote/_data.* y, si no aparece,
      // se permite fallback por única confirmación pendiente.
      try {
        const body = String(message?.body || message?._data?.body || '').trim();
        if (api_mensajes_confirmacion_habilitada !== true) {
          return;
        }
        if (body && respuestaConfirmaApiMensajes(body)) {
          const targetRaw = getOutgoingConfirmacionTargetRaw(message) || '__confirmacion_fromme_fallback__';
          logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] OK saliente detectado fromMe=true target=' + targetRaw +
            ' raw_from=' + String(message?.from || message?._data?.from || '') +
            ' raw_to=' + String(message?.to || message?._data?.to || '') +
            ' remote=' + String(message?.id?.remote || message?._data?.id?.remote || '') +
            ' body=' );
          const fakeIncomingConfirmacion = {
            from: targetRaw,
            to: message?.from || message?._data?.from || '',
            body,
            type: 'chat',
            fromMe: false,
            id: message?.id,
            _data: message?._data,
            _confirmacionSource: 'message_create_fromMe'
          };
          const okProcesado = await registrarRespuestaConfirmacionApiMensajes(fakeIncomingConfirmacion);
          logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] resultado OK saliente procesado=' + String(okProcesado));
        } else if (body && esRespuestaNoValidaConfirmacionApiMensajes(body)) {
          const targetRaw = getOutgoingConfirmacionTargetRaw(message) || '__confirmacion_fromme_fallback__';
          logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] respuesta no valida saliente detectada fromMe=true target=' + targetRaw +
            ' raw_from=' + String(message?.from || message?._data?.from || '') +
            ' raw_to=' + String(message?.to || message?._data?.to || '') +
            ' remote=' + String(message?.id?.remote || message?._data?.id?.remote || '') +
            ' body=' );
          const fakeNoValidaConfirmacion = {
            from: targetRaw,
            to: message?.from || message?._data?.from || '',
            body,
            type: 'chat',
            fromMe: false,
            id: message?.id,
            _data: message?._data,
            _confirmacionSource: 'message_create_fromMe_no_valida'
          };
          const noValidaProcesada = await registrarRespuestaNoValidaConfirmacionApiMensajes(fakeNoValidaConfirmacion);
          logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] resultado respuesta no valida saliente procesada=' + String(noValidaProcesada));
        }
      } catch (e) {
        try { EscribirLog('[API_MENSAJES_CONFIRMACION] error procesando OK saliente: ' + String(e?.message || e), 'error'); } catch {}
      }
      return;
    }
    // El evento message tiene prioridad. message_create queda como respaldo demorado
    // para instalaciones donde ciertos PTT solo aparecen por este evento.
    scheduleIncomingFromMessageCreate(message, processIncomingAsistoMessage);
    return;
  } catch (e) {
    try { console.log('[message_create] incoming error:', e?.message || e); } catch {}
    try { EscribirLog('[message_create] incoming error: ' + String(e?.message || e), 'error'); } catch {}
  }
});


client.on('message_ack', (message2, ack) => {
  // Mismo criterio que app_chatbot_super: actualizar es_mensajes con id_ws y estado_ws
  // usando el wsMsgId devuelto por WhatsApp para los mensajes salientes.
  try {
    if (!compraEntregaConnection) return;
    if (!message2 || !message2.id) return;
    if (message2.fromMe !== true) return;

    const wsId = message2.id.id;
    if (!wsId) return;

    const dbId = pendingAck.get(wsId) ?? id_msg;
    const dbIdNum = Number(dbId);
    if (!Number.isFinite(dbIdNum) || dbIdNum <= 0) {
      console.log('ACK sin dbId válido (skip). wsId=' + wsId + ' id_msg=' + id_msg);
      return;
    }

    console.log('ACK id_msg ' + dbIdNum);
    compraEntregaConnection.query("update es_mensajes set id_ws = '" + wsId + "' where id='" + dbIdNum + "'");
    compraEntregaConnection.query("update es_mensajes set estado_ws = '" + ack + "' where id_ws='" + wsId + "'");

    pendingAck.delete(wsId);
    if (dbId === id_msg) id_msg = 0;

    console.log('Mensaje ' + wsId);
    console.log('Estado ' + ack);
  } catch (e) {
    console.log('message_ack handler error:', e?.message || e);
  }
});


client.on('message', async message => {
  try {
    try {
      const b = String(message?.body || message?._data?.body || '').trim();
      if (b && respuestaConfirmaApiMensajes(b)) {
        logConfirmacionDebug('[API_MENSAJES_CONFIRMACION_DEBUG] message raw fromMe=' + String(!!message?.fromMe) +
          ' from=' + String(message?.from || message?._data?.from || '') +
          ' to=' + String(message?.to || message?._data?.to || '') +
          ' remote=' + String(message?.id?.remote || message?._data?.id?.remote || '') +
          ' type=' + String(message?.type || message?._data?.type || '') +
          ' body=' );
      }
    } catch {}
    await processIncomingAsistoMessage(message, 'message');
  } catch (e) {
    try { console.log('[message] incoming error:', e?.message || e); } catch {}
    try { EscribirLog('[message] incoming error: ' + String(e?.message || e), 'error'); } catch {}
  }
});







client.on('ready', async () => {
  clearAuthReadyWatchdog('ready');
  restartInFlight = false;
  console.log("listo ready....");
  telefono_qr = client.info.me.user
  console.log("TEL QR: "+client.info.me.user);
  
    
   await io.emit('message', 'Whatsapp Listo!');
   EscribirLog('Whatsapp Listo!',"event");
   // Para el panel: sesión activa
  if (String(localWsPanelState || '').toLowerCase() !== 'paused' && lastPolicyBlocked !== true) {
    updateLockStateSafe('online').catch(()=>{});
  }
  // Opcional: si querés conservar un "hito" ready en historial:
  // updateLockStateSafe('ready').catch(()=>{});

  //ConsultaApiMensajes();
  try { await refreshTenantConfigFromDbPerMessage(); } catch {}
  try { RecuperarJsonConfMensajes(); } catch {}
  try {
     console.log('[CONFIG] wweb_engine=' + getWwebEngine() + ' habilitar_bot=' + habilitar_bot + ' habilitar_consulta_mensajes=' + consulta_api_mensajes_habilitado + ' wweb_bot_logic_mode=' + wweb_bot_logic_mode + ' time_cad_ms=' + time_cad);
  } catch {}
  startConsultaApiMensajesIfEnabled('ready');
  startCompraEntregaLoopIfEnabled('ready');

});

client.on('qr', (qr) => {
  clearAuthReadyWatchdog('qr');
 // Al entrar en QR ya no hay sesión lista: limpiar nro anterior para no consultar API saliente con un número viejo.
  telefono_qr = "";
  console.log('QR RECEIVED', qr);
pushHistory('qr', { at: new Date().toISOString() }).catch(()=>{});
  // Guardar último QR para endpoint /status/qr
  lastQrRaw = qr;
  lastQrAt = nowArgentinaISO();

  updateLockStateSafe('qr').catch(()=>{});
  qrcode.toDataURL(qr, (err, url) => {
     if (err || !url) {
      try { console.log('QR toDataURL error:', err); } catch {}
      return;
    }

    // Guardar el QR en memoria (status/qr) y en Mongo (panel /admin/wweb)
    lastQrDataUrl = url;
    updateLockQrDataSafe(url, lastQrAt).catch(() => {});


    io.emit('qr', url);
    io.emit('message', 'Código QR Recibido...');
  });
});


client.on('authenticated', async () => {
  io.emit('authenticated', 'Whatsapp Autenticado!.');
  io.emit('message', 'Whatsapp Autenticado!');
  console.log('Autenticado');
  EscribirLog('Autenticado',"event");
  updateLockStateSafe('authenticated').catch(()=>{});
  armAuthReadyWatchdog('authenticated');

});



client.on('auth_failure', async function(session) {
  telefono_qr = "";
  io.emit('message', 'Auth failure');
 
  const authFailureReason = String(session || '');
  EscribirLog('Error 04 - Chatbot error Auth failure', authFailureReason, "error");
  updateLockStateSafe('auth_failure').catch(()=>{});
  clearAuthReadyWatchdog('auth_failure');

  // Baileys 401/loggedOut significa que WhatsApp invalidó la credencial local.
  // Reiniciar con la misma carpeta auth solo repite 401 indefinidamente. Dejamos
  // la sesión en auth_failure para que "Borrar autenticación" limpie ESA sesión
  // y genere un QR nuevo desde el panel.
  const baileysLoggedOut = isBaileysEngine() && /^baileys_logged_out:/i.test(authFailureReason);
  if (baileysLoggedOut) {
    clientStarted = false;
    authFailureHandling = false;
    const msg = '[AUTH_FAILURE] Baileys loggedOut; se detiene auto-restart y se espera Borrar autenticación desde el panel';
    try { console.log(msg); } catch {}
    try { EscribirLog(msg, 'error'); } catch {}
    return;
  }
 

    // Para otros auth_failure mantenemos el comportamiento histórico.
  if (isLocalAuthMode() && isOwner && !authFailureHandling) {
    authFailureHandling = true;
    setTimeout(async () => {
      try {
        await restartClientSession('auth_failure', 7000);
      } catch (e) {
        EscribirLog('auth_failure recovery error: ' + String(e?.message || e), 'error');
      } finally {
        authFailureHandling = false;
      }
    }, 2500);
  }
});

client.on('disconnected', async (reason) => {
  telefono_qr = "";
  try { compraEntregaQueryStopRequested = true; } catch {}
  try { if (compraEntregaConnection && typeof compraEntregaConnection.close === 'function') await compraEntregaConnection.close(); } catch {}
  try { compraEntregaConnection = null; } catch {}
  io.emit('message', 'Whatsapp Desconectado!');

  const disconnectedLog = '[DISCONNECTED] reason=' + String(reason || 'sin_detalle');
  try { console.log(disconnectedLog); } catch {}
  EscribirLog(disconnectedLog, 'event');
  updateLockStateSafe('disconnected').catch(()=>{});

  clearAuthReadyWatchdog('disconnected');

  try { if (client) await destroyClientHard(client); } catch(e) {}
  try { client = null; } catch {}
  resetClientRuntimeFlags('disconnected');
  // Si el corte fue provocado por Reiniciar/Borrar auth, no agendar otro reinicio automático.
  // Antes podía quedar doble initialize() y el panel permanecía en "iniciando".
  if (restartInFlight || clearAuthInFlight || fullProcessRestartInFlight) {
    try { EscribirLog('[DISCONNECTED] sin auto-restart por acción en curso: ' + String(reason || ''), 'event'); } catch {}
    return;
  }

  // Solo reintenta si esta PC sigue siendo owner del lock.
  if (isOwner) {
    setTimeout(() => {
      if (isOwner && !clientStarted && !restartInFlight && !clearAuthInFlight && !fullProcessRestartInFlight && !startingNow) {
        restartClientSession('disconnected', 7000).catch(() => {});
      }
    }, 2500);
  }
});


}





function recuperar_json(a_telefono, json){

  var indice =indexOf2d(a_telefono);


  let now = new Date();
 
  if(indice !== -1){
   // console.log("ESTA "+a_telefono);
   
    jsonGlobal[indice][0] = a_telefono;
   // jsonGlobal[a_telefono,2] = 0;
    jsonGlobal[indice][2] = json;
    jsonGlobal[indice][3] = now;
    //console.table(jsonGlobal);
 }else{

    //console.log("NO ESTA "  +a_telefono);
     
  jsonGlobal.push([a_telefono,0,json,now])
    
      
 }


}

function indexOf2d(itemtofind) {
  var valor = -1
  console.table(jsonGlobal);

  for (var i = 0; i < jsonGlobal.length; i++) {
    
    if(jsonGlobal[i][0]==itemtofind){
      console.log('array '+jsonGlobal[i][0]);
      return i
    } else{

      valor = -1
    }
  }

  return valor


  //console.log('indice_a '+[].concat.apply([], ([].concat.apply([], myArray))).indexOf(itemtofind));
  //console.log('indice_b '+myArray.indexOf(itemtofind));
  //console.log('indice_c '+myArray(0).indexOf(itemtofind));
  //return [].concat.apply([], ([].concat.apply([], myArray))).indexOf(itemtofind) !== -1;
  //return [].concat.apply([], ([].concat.apply([], myArray))).indexOf(itemtofind) ;
 
  }

/////////////////////////////////////////////////////////////////////////////////////
// FUNCION DONDE SE PROCESA EL JSON GLOBAL DE MSG Y SE ENVIA
////////////////////////////////////////////////////////////////////////////////

async function procesar_mensaje(json, message){
  
  RecuperarJsonConfMensajes();

  var indice =indexOf2d(message.from);
  let now = new Date();

  var segundos = Math.random() * (seg_hasta - seg_desde) + seg_desde;
  var l_from = message.from;
  var l_json =jsonGlobal[indice][2];
  var l_i = jsonGlobal[indice][1];
  var tam_json =0;
  
  jsonGlobal[indice][3] = now;
  

  console.table(jsonGlobal);
 
  for(var j in jsonGlobal[indice][2]){
    tam_json = tam_json + 1;
  }


  // cant_lim = 0 debe significar "sin límite". Si no, la primera respuesta
  // no se envía y aparece directamente "Continuar? S / N".
  var limite_lote = Number(cant_lim);
  if (!Number.isFinite(limite_lote) || limite_lote <= 0) {
    limite_lote = tam_json;
  }

  for( var i=jsonGlobal[indice][1]; i < tam_json; i++){
   
    if(l_json[i].cod_error){ 
      var mensaje = l_json[i].msj_error;
      EscribirLog('Error 05 - procesar_mensaje() devuelve cod_error API ',"error");
     
    }else{
      var mensaje =  l_json[i].Respuesta;
    }
      
      if (mensaje == '' || mensaje == null || mensaje == undefined ){
      }
      else{
    
        mensaje = mensaje.replaceAll("|","\n");
    
        console.log("mensaje "+message.from+" - "+mensaje);
        
        if(i<= limite_lote + jsonGlobal[indice][1] -1){
        
         await safeSend(message.from,mensaje );
         await sleep(segundos);
         await io.emit('message', 'Respuesta: '+message.from+': '+ mensaje );
         if(tam_json-1==i){
            bandera_msg=1;
            jsonGlobal[indice][1] = 0;
            jsonGlobal[indice][2] = '';
            jsonGlobal[indice][3] = '';
         }
      }else{
       // for (var j = 0; j < 20; j++){
          msg_lim = msg_lim.replaceAll("|","\n");
        //}
        var msg_loc = msg_lim;

       
        if(tam_json  <= i + cant_lim  ){
          msg_loc = msg_loc.replace('<recuento>', tam_json  - i );
       }else{
        msg_loc = msg_loc.replace('<recuento>', limite_lote+1);
       }
      
        msg_loc = msg_loc.replace('<recuento_lote>', tam_json - 2);
        msg_loc = msg_loc.replace('<recuento_pendiente>', tam_json  - i);
              
        if (msg_loc == '' || msg_loc == null || msg_loc == undefined ){
        }
        else{
          safeSend(message.from,msg_loc);
        }
       bandera_msg=0;
       jsonGlobal[indice][1]  = i;
       jsonGlobal[indice][3] = now;
       return;
      }
    }
  }


};

///////////////////////////////////////////////////////////////////////
// CONTROLA CADUCIDAD DE LOS MESNAJES
///////////////////////////////////////////////////////////////////////

async function controlar_hora_msg(){

  while(a < 1){
    const ttlMs = Number(time_cad);
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
      await sleep(5000);
      continue;
    }

     for(var i in jsonGlobal){
     
      if(jsonGlobal[i] && jsonGlobal[i][3] !== ''){
        var fecha = new Date();
        var fechaMsgDate = (jsonGlobal[i][3] instanceof Date) ? jsonGlobal[i][3] : new Date(jsonGlobal[i][3]);
        var fecha_msg = fechaMsgDate.getTime();
        if (!Number.isFinite(fecha_msg)) continue;
        var fecha_msg2 = fecha.getTime();
        var diferencia = fecha_msg2-fecha_msg;
         if(diferencia > ttlMs ){
          if(msg_cad == '' || msg_cad  == undefined || msg_cad == 0 ){
            
          } else {
            await safeSend(jsonGlobal[i][0],msg_cad );

          }
          console.log("tiempo expirado "+ jsonGlobal[i][0]+' '+diferencia+' '+ttlMs );
          // delete(jsonGlobal[i]);
          
          jsonGlobal[i][3] = '';
          jsonGlobal[i][2] = '';
          jsonGlobal[i][1] = 0;
          }
        }

        
        
    }
   
    await sleep(5000);
  }   
}

function startCaducidadMensajesWatcher(source = ''){
  try {
    if (mensajeCaducidadWatcherStarted) return;
    mensajeCaducidadWatcherStarted = true;
    const msg = 'Control caducidad mensajes iniciado'
      + (source ? ' source=' + source : '')
      + ' time_cad_ms=' + String(Number(time_cad) || 0);
    console.log(msg);
    EscribirLog(msg, 'event');
    controlar_hora_msg().catch((e) => {
      mensajeCaducidadWatcherStarted = false;
      console.log('controlar_hora_msg fatal:', e?.message || e);
      EscribirLog('controlar_hora_msg fatal: ' + String(e?.message || e), 'error');
    });
  } catch (e) {
    mensajeCaducidadWatcherStarted = false;
    console.log('startCaducidadMensajesWatcher error:', e?.message || e);
    EscribirLog('startCaducidadMensajesWatcher error: ' + String(e?.message || e), 'error');
  }
}

 
function RecuperarJsonConfMensajes(){
  // Mensajes/config vienen de MongoDB (tenantConfig). Mantiene configuracion_errores.json desde archivo.
  let jsonError = null;
  try { jsonError = JSON.parse(fs.readFileSync('configuracion_errores.json')); } catch {}
  try {
    if (jsonError && jsonError.configuracion) {
 
      msg_errores = jsonError.configuracion.msg_error;
    }
  } catch {}

  // Preferencia: tenantConfig (BD)
  if (tenantConfig && typeof tenantConfig === "object") {
    applyTenantConfig(tenantConfig);
    return;
  }

  // Fallback (legacy): si alguien todavía usa configuracion.json viejo con {configuracion:{...}}
  try {
    const raw = JSON.parse(fs.readFileSync('configuracion.json'));
    const conf = (raw && raw.configuracion && typeof raw.configuracion === "object") ? raw.configuracion : raw;
    if (conf && typeof conf === "object") applyTenantConfig(conf);
  } catch {}
}



////////////////////////////////////////////////////////////////////////////////////////////
//  FUNCION PARA MANTENER EL JSON GLOBAL CON LOS TELEFONOS Y MENSAJES QUE VAN INGRESANDO - FUNCION
//   NECESARIA PARA PODER LIMITAR LA CANTIDAD DE MENSAJES CONTINUOS A ENVIAR
////////////////////////////////////////////////////////////////////////////////////////////

function recuperar_json(a_telefono, json){

  var indice =indexOf2d(a_telefono);


  let now = new Date();
 
  if(indice !== -1){
   // console.log("ESTA "+a_telefono);
   
    jsonGlobal[indice][0] = a_telefono;
   // jsonGlobal[a_telefono,2] = 0;
    jsonGlobal[indice][2] = json;
    jsonGlobal[indice][3] = now;
    //console.table(jsonGlobal);
 }else{

    //console.log("NO ESTA "  +a_telefono);
     
  jsonGlobal.push([a_telefono,0,json,now])
    
      
 }


}
function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}



function isDetachedFrameError(err) {
  const msg = String(err?.message || err || "");
  return msg.toLowerCase().includes("detached frame") || msg.toLowerCase().includes("frame was detached") || msg.toLowerCase().includes("navigating frame was detached");
}

function isExecutionContextError(err) {
  const msg = String(err?.message || err || "");
 return msg.includes("Execution context was destroyed") ||
         msg.includes("Cannot find context") ||
         msg.includes("Target closed") ||
         msg.includes("Protocol error");
}

function getWwebChromiumPid(c) {
  if (!c || c.__transport === 'baileys') return 0;
  try {
    const pid = Number(c?.pupBrowser?.process?.()?.pid || 0);
    return Number.isInteger(pid) && pid > 0 ? pid : 0;
  } catch {
    return 0;
  }
}

async function forceTerminateProcessTree(pid, reason = '') {
  const n = Number(pid);
  if (!Number.isInteger(n) || n <= 0) return false;
  if (!isPidAlive(n)) return true;

  const prefix = `[WWEB_CLEANUP] force kill pid=${n} reason=${String(reason || '')}`;
  try { console.log(prefix); } catch {}
  try { EscribirLog(prefix, 'event'); } catch {}

  try {
    if (process.platform === 'win32') {
      // /T mata solo el árbol del Chromium lanzado por Puppeteer para ESTA sesión.
      await runCommand('taskkill', ['/PID', String(n), '/T', '/F'], { cwd: __dirname, timeout: 5000 });
    } else {
      try { process.kill(n, 'SIGTERM'); } catch {}
      await sleep(300);
      if (isPidAlive(n)) {
        try { process.kill(n, 'SIGKILL'); } catch {}
      }
    }
  } catch (e) {
    // Si ya terminó entre la comprobación y taskkill, no es un error funcional.
    if (isPidAlive(n)) {
      try { EscribirLog('[WWEB_CLEANUP] no se pudo terminar pid=' + n + ': ' + String(e?.message || e), 'error'); } catch {}
      return false;
    }
  }
  return !isPidAlive(n);
}

async function closeWhatsappClientForProcessExit(c, reason = 'process_exit', timeoutMs = 2200) {
  if (!c) return { ok: true, skipped: true, engine: '' };

  const engine = String(c.__transport || (isBaileysEngine() ? 'baileys' : 'wwebjs')).trim().toLowerCase();
  const browserPid = engine === 'wwebjs' ? getWwebChromiumPid(c) : 0;
  const timeout = Math.max(500, Number(timeoutMs) || 2200);
  const timeoutMarker = Symbol('client_close_timeout');

  const closePromise = (async () => {
    if (engine === 'baileys') {
      try { await c.destroy?.(); } catch {}
      return 'closed';
    }

    // No usamos destroyClientHard acá porque incluye esperas largas pensadas para
    // reintentos normales. En salida de proceso queremos liberar Chromium rápido.
    try { await c.destroy?.(); } catch {}
    try { await c.pupPage?.close?.(); } catch {}
    try { await c.pupBrowser?.close?.(); } catch {}
    return 'closed';
  })();

  let result = null;
  try {
    result = await Promise.race([
      closePromise,
      new Promise((resolve) => setTimeout(() => resolve(timeoutMarker), timeout))
    ]);
  } catch {}

  const timedOut = result === timeoutMarker;
  if (timedOut) {
    try { EscribirLog(`[WWEB_CLEANUP] timeout engine=${engine} reason=${String(reason || '')} timeoutMs=${timeout}`, 'event'); } catch {}
  }

  let forced = false;
  if (browserPid && isPidAlive(browserPid)) {
    forced = true;
    await forceTerminateProcessTree(browserPid, reason);
  }

  try {
    EscribirLog(`[WWEB_CLEANUP] fin engine=${engine} reason=${String(reason || '')} browserPid=${browserPid || 0} timedOut=${timedOut} forced=${forced}`, 'event');
  } catch {}

  return { ok: true, engine, browserPid, timedOut, forced };
}


async function destroyClientHard(c) {
  if (!c) return;
  if (c.__transport === 'baileys') {
    try { await c.destroy(); } catch {}
    await sleep(250);
    return;
  }
  // whatsapp-web.js expone (según versión) pupBrowser/pupPage en el client.
  try { await c.destroy(); } catch {}
  try { await c.pupPage?.close?.(); } catch {}
  try { await c.pupBrowser?.close?.(); } catch {}
  await sleep(2500);
}

async function recreateClientForRetry(reason) {
  try { console.log(`Recreando client por: ${reason}`); } catch {}
  try { EscribirLog(`Recreando client por: ${reason}`, "event"); } catch {}

  const wasBaileys = client?.__transport === 'baileys';
  try { await destroyClientHard(client); } catch {}
  try { clientStarted = false; } catch {}
  try { client = null; } catch {}

  // whatsapp-web.js necesita dar tiempo a Chromium para cerrar; Baileys no.
  await sleep(wasBaileys ? 300 : 2500);

  // Re-crea el client:
  // Si venimos por execution_context / detached_frame, NO tocar el storage local ni forzar restore,
  // porque esos errores suelen ser del navegador, no de la sesión.
  await createClientIfNeeded();
  return client;
}

async function initializeWithRetry(clientInstance, maxRetries = 5) {
  // IMPORTANTE: ante ciertos errores (detached frame / execution context) conviene
  // recrear TODO el client y reintentar. Re-usar el mismo objeto suele quedar roto.
  let c = clientInstance;

  for (let i = 1; i <= maxRetries; i++) {
    try {
      try {
        const initClientId = `asisto_${tenantId}_${numero}`;
        if (c?.__transport === 'baileys' || isBaileysEngine()) {
          console.log(`[INIT] attempt=${i} engine=baileys authDir=${getBaileysAuthSessionDir(initClientId)}`);
        } else {
          console.log(`[INIT] attempt=${i} engine=wwebjs dataPath=${getAuthBasePath()} sessionDir=${getLocalAuthSessionDir(initClientId)}`);
        }
      } catch {}
      await c.initialize();
      return true;
    } catch (e) {
       const detached = isDetachedFrameError(e);
      const ctx = isExecutionContextError(e);
      if (!detached && !ctx) throw e;

      const msg = String(e?.message || e || "");
      console.log(`initialize retry ${i}/${maxRetries} (${detached ? "detached frame" : "execution context"}) -> ${msg}`);
      try { EscribirLog(`initialize retry ${i}/${maxRetries}: ${msg}`, "event"); } catch {}

      // Backoff progresivo
      await sleep(1500 * i);

      // Re-create completo (evita quedarse con frames viejos)
      c = await recreateClientForRetry(detached ? "detached_frame" : "execution_context");

    }
  }
  throw new Error("initialize_failed_after_retries");
}
function detectMimeType(b64) {
  for (var s in signatures) {
    if (b64.indexOf(s) === 0) {
    return signatures[s];
  }
}}

function devolver_puerto(){

return port;
}


function devolver_seg_tele(){

return seg_tele;
}

function devolver_seg_desde(){

return seg_desde;
}

function devolver_seg_hasta(){

return seg_hasta;
}


function devolver_seg_desde2(){

return seg_desde2;
}

function devolver_seg_hasta2(){

return seg_hasta2;
}

function devolver_headless(){

return headless;
}

function RecuperarJsonConf(){
  // configuracion.json (bootstrap) SOLO: tenantId, mongo_uri, mongo_db
  // El resto se carga desde Mongo (tenantConfig) por loadTenantConfigFromDb()
  try {
    const boot = readBootstrapFromFile();
    if (!tenantId && boot.tenantId) tenantId = String(boot.tenantId).trim();
 
  // Normalizar tenantId para evitar locks duplicados por mayúsculas/espacios
  tenantId = String(tenantId || '').trim();
  if (tenantId) tenantId = tenantId.toUpperCase();
    if (!mongo_uri && (boot.mongo_uri || boot.mongoUri)) mongo_uri = String(boot.mongo_uri || boot.mongoUri).trim();
    if (!mongo_db && (boot.mongo_db || boot.mongoDb || boot.dbName)) mongo_db = String(boot.mongo_db || boot.mongoDb || boot.dbName).trim();
    if (!mongo_db) mongo_db = "Cluster0";
    configureControlApiFromValues(boot);
    controlApi.configure({ tenantId, numero });
  } catch {}

  // Si ya hay config de BD/API, aplicarla (no rompe si es null)
  try { if (tenantConfig) applyTenantConfig(tenantConfig); } catch {}
}



// ISO-like: 2025-09-04T13:45:22 (hora de Argentina)
function nowArgentinaISO() {
  const s = new Intl.DateTimeFormat('sv-SE', {
    timeZone: AR_TZ,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).format(new Date()); // "YYYY-MM-DD HH:mm:ss"
  return s.replace(' ', 'T'); // "YYYY-MM-DDTHH:mm:ss"
}

// (Opcional) con offset: 2025-09-04T13:45:22-03:00
function nowArgentinaISOWithOffset() {
  const d = new Date();
  const base = new Intl.DateTimeFormat('sv-SE', {
    timeZone: AR_TZ,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).format(d).replace(' ', 'T');

  // Si tu versión de Node soporta 'longOffset', obtenemos "-03:00"
  const tzName = new Intl.DateTimeFormat('en', {
    timeZone: AR_TZ,
    timeZoneName: 'longOffset' // devuelve "GMT-03:00" en Node moderno
  }).formatToParts(d).find(p => p.type === 'timeZoneName')?.value;

  const offset = tzName ? tzName.replace('GMT', '') : '-03:00';
  return base + offset;
}

function EscribirLog(mensaje, tipo) {
  const timestamp = nowArgentinaISO(); // o nowArgentinaISOWithOffset()
  const logMessage = `[${timestamp}] ${mensaje}\n`;
  const file = (tipo === 'event') ? logFilePath_event : logFilePath_error;

  fs.appendFile(file, logMessage, (err) => {
    if (err) {
      console.error('Error al escribir en el archivo de log:', err);
    }
  });
}
