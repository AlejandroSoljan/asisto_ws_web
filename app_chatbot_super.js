/*script:Script_super*/
/*version:4.00.13*/

//var funciones = require('./funciones');
//const { Client, MessageMedia, LocalAuth , Buttons } = require('whatsapp-web.js');
const { Client, LocalAuth  ,Buttons,List,MessageMedia}  = require('whatsapp-web.js');

const express = require('express');
const { body, validationResult } = require('express-validator');
const socketIO = require('socket.io');
const qrcode = require('qrcode');
const http = require('http');
const fs = require('fs');
var odbc = require("odbc");
//const { phoneNumberFormatter } = require('./helpers/formatter');
const fileUpload = require('express-fileupload');
const axios = require('axios');
const mime = require('mime-types');
const { ClientInfo } = require('whatsapp-web.js/src/structures');
const utf8 = require('utf8');
const { OdbcError } = require('odbc');
const { json } = require('express');

const mongoose = require('mongoose');
const os = require('os');
const crypto = require('crypto');
const path = require('path');
const { spawn } = require('child_process');

// --- LocalAuth backup to Mongo (GridFS) ---
let AdmZip = null;
try { AdmZip = require('adm-zip'); } catch (e) { /* opcional: npm i adm-zip */ }
let GridFSBucket = null;
try { ({ GridFSBucket } = require('mongodb')); }
catch (e) {
  // Fallback: mongoose ya trae el driver de mongodb
  try { GridFSBucket = mongoose?.mongo?.GridFSBucket; } catch {}
}


var bandera_query = 0;
var port = 8000
var headless = true;
var seg_desde = 80000;
var seg_hasta = 10000;
var seg_msg = 5000;
var version = "1.0";
var script = "__";
var telefono_qr = "0";
var telefono_local = "0";
var tel_array = [];
var ver_whatsapp = "0";
var dsn = "msm_manager";
var api = "http://managermsm.ddns.net:2002/ApiChat/api/Api_Chat_Cab/ProcesarMensajePost";

var seg_tele = 3000;
var msg_inicio = "";
var msg_fin = "";
var msg_lim = 'Continuar? S / N';
var cant_lim = 0;
var time_cad = 0;
var msg_cad = "";
var msg_can = "";

const AR_TZ = 'America/Argentina/Cordoba';
let tenantConfig = null;
let lastQrRaw = null;
let lastQrDataUrl = null;
let lastQrAt = null;
let localWsPanelState = 'idle';

// =========================
// Auto-update desde repositorio (opcional)
// =========================
let auto_update_enabled = String(process.env.AUTO_UPDATE_ENABLED || '').trim().toLowerCase() === 'true';
let auto_update_repo_path = String(process.env.AUTO_UPDATE_REPO_PATH || process.cwd()).trim() || process.cwd();
let auto_update_remote = String(process.env.AUTO_UPDATE_REMOTE || 'origin').trim() || 'origin';
let auto_update_branch = String(process.env.AUTO_UPDATE_BRANCH || '').trim();
let auto_update_check_every_ms = Number(process.env.AUTO_UPDATE_CHECK_EVERY_MS || 10 * 60_000);
let auto_update_startup_delay_ms = Number(process.env.AUTO_UPDATE_STARTUP_DELAY_MS || 120_000);
let auto_update_restart_on_apply = String(process.env.AUTO_UPDATE_RESTART_ON_APPLY || 'true').trim().toLowerCase() !== 'false';
let auto_update_require_clean = String(process.env.AUTO_UPDATE_REQUIRE_CLEAN || 'true').trim().toLowerCase() !== 'false';
let auto_update_run_npm_install = String(process.env.AUTO_UPDATE_RUN_NPM_INSTALL || 'true').trim().toLowerCase() !== 'false';
let auto_update_post_update_cmd = String(process.env.AUTO_UPDATE_POST_UPDATE_CMD || '').trim();
let autoUpdateTimer = null;
let autoUpdateRunning = false;
let autoUpdateRestarting = false;


// =========================
// WhatsApp session en Mongo (Lock/Lease + Backup LocalAuth)
// =========================
let tenantId = process.env.TENANT_ID || "";
let numero = process.env.NUMERO || "";                 // solo dígitos, ej: 54911...
let mongo_uri = process.env.MONGO_URI || "";
let mongo_db = process.env.MONGO_DB || "";
let status_token = process.env.STATUS_TOKEN || "";     // opcional (no se usa acá, pero se conserva por compatibilidad)
let lease_ms = Number(process.env.LEASE_MS || 30000);
let heartbeat_ms = Number(process.env.HEARTBEAT_MS || 10000);
let backup_every_ms = Number(process.env.BACKUP_EVERY_MS || 300000);
let auth_base_path = process.env.SUPER_AUTH_PATH || ""; // override dataPath de LocalAuth (opcional)

//var l_cliente = "";
const app = express();
const server = http.createServer(app);
const io = socketIO(server);
// id del registro es_mensajes que esperamos actualizar cuando llegue el ACK
// (inicializar en 0 para evitar 'undefined' en SQL)
var id_msg = 0;
// Mapa robusto: wsMsgId -> id (DB). Evita carreras cuando salen varios envíos seguidos.
const pendingAck = new Map();
var connection ;


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


//console.log("dsn "+dsn+"; encoding=UTF-8");
//const connection = odbc.connect('DSN='+dsn+'; charset=UTF8');
//connection = odbc.connect("DSN='msm_manager'; charset=UTF8");
// console.log("conectado a Manager..."+dsn);
   


// Bootstrap movido al arranque controlado.

let client = null;

// =========================
// WhatsApp session en Mongo (Lock/Lease + Backup LocalAuth)
// - Evita pedir QR en reinicios (restaura LocalAuth desde Mongo)
// - Evita 2 PCs conectadas a la vez (lock/lease en Mongo)
// =========================
const instanceId = process.env.INSTANCE_ID || `${os.hostname()}-${process.pid}-${crypto.randomBytes(4).toString("hex")}`;
let lockId = "";                 // `${tenantId}:${numero}`
let isOwner = false;             // true si esta PC tiene el lock
let clientStarted = false;       // true si initialize() fue llamado
let startingNow = false;         // evita inicializaciones concurrentes
let mongoReady = false;
let mongoConnectingPromise = null;
let LockModel = null;
let ActionModel = null;
let PolicyModel = null;
let HistoryModel = null;
let MessageLogModel = null;
let heartbeatTimer = null;
let pollTimer = null;
let actionTimer = null;
let actionBusy = false;

function getAuthBasePath() {
  // priority: config auth_base_path -> env -> default (whatsapp-web.js)
  if (auth_base_path && String(auth_base_path).trim()) return String(auth_base_path).trim();
  const envp = process.env.SUPER_AUTH_PATH || process.env.ASISTO_AUTH_PATH;
  if (envp && String(envp).trim()) return String(envp).trim();
  // whatsapp-web.js por defecto usa "./.wwebjs_auth"
  return path.join(process.cwd(), ".wwebjs_auth");
}

function getLocalAuthSessionDir(clientId) {
  // whatsapp-web.js LocalAuth crea: <dataPath>/session-<clientId>
  return path.join(getAuthBasePath(), `session-${clientId}`);
}

async function cleanupOrphanGridFSChunks(bucketName = "wa_localauth") {
  // Este script usa mongoose, no mongoClient.
  await ensureMongo();
  const db = mongoose?.connection?.db;
  if (!db) {
    sessionLog("GridFS cleanup: mongo db not ready");
    return;
  }
  const filesColl = db.collection(`${bucketName}.files`);
  const chunksColl = db.collection(`${bucketName}.chunks`);

  const ids = await filesColl.distinct("_id");
  const filter = ids.length ? { files_id: { $nin: ids } } : {}; // si no hay files, borra todo

  const res = await chunksColl.deleteMany(filter);
  sessionLog(`GridFS cleanup: orphan chunks deleted=${res.deletedCount}`);
}

function dirLooksPopulated(p) {
  try {
    if (!fs.existsSync(p)) return false;
    const items = fs.readdirSync(p);
    return Array.isArray(items) && items.length > 0;
  } catch { return false; }
}

// Criterio "real" de sesión: evita guardar/restaurar carpetas incompletas.
// (Mismo criterio que en app_asisto_ws.js)
function localAuthDirHasKeyData(sessionDir) {
  try {
    if (!sessionDir || !fs.existsSync(sessionDir)) return false;
    const required = [
      path.join(sessionDir, "Default", "Local Storage", "leveldb"),
      path.join(sessionDir, "Default", "IndexedDB"),
      path.join(sessionDir, "Default", "Cookies"),
      path.join(sessionDir, "Default", "Preferences"),
    ];
    for (const p of required) {
      if (!fs.existsSync(p)) continue;
      try {
        const st = fs.statSync(p);
        if (st.isFile() && st.size > 0) return true;
        if (st.isDirectory()) {
          const items = fs.readdirSync(p);
          if (items && items.length > 0) return true;
        }
      } catch {}
    }
    const items = fs.readdirSync(sessionDir).filter(x => x && !String(x).startsWith("."));
    return items.length >= 3;
  } catch {
    return false;
  }
}

function sessionLog(msg) {
  try { console.log(msg); } catch {}
  try { EscribirLog(msg, "event"); } catch {}
}

function nowArgentinaISO() {
  try {
    return new Date().toLocaleString('sv-SE', { timeZone: AR_TZ }).replace(' ', 'T');
  } catch {
    return new Date().toISOString();
  }
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

function readBootstrapFromFile() {
  const candidates = [
    process.env.SUPER_CONFIG_PATH,
    process.env.ASISTO_CONFIG_PATH,
    'C:/Chatbot_pb/configuracion.json',
    path.join(process.cwd(), 'configuracion.json')
  ].filter(Boolean);

  for (const p of candidates) {
    try {
      if (!fs.existsSync(p)) continue;
      const raw = JSON.parse(fs.readFileSync(p, 'utf8'));
      const obj = (raw && raw.configuracion && typeof raw.configuracion === 'object') ? raw.configuracion : raw;
      if (raw && typeof raw === 'object') {
        return { ...raw, ...(obj && typeof obj === 'object' ? obj : {}) };
      }
    } catch {}
  }
  return {};
}

function readVersionFromFile() {
  const candidates = [
    process.env.SUPER_VERSION_PATH,
    'C:/Chatbot_pb/version.json',
    path.join(process.cwd(), 'version.json')
  ].filter(Boolean);

  for (const p of candidates) {
    try {
      if (!fs.existsSync(p)) continue;
      const raw = JSON.parse(fs.readFileSync(p, 'utf8'));
      if (raw && typeof raw === 'object') return raw;
    } catch {}
  }
  return {};
}

function normalizeAutoUpdateConfig(conf) {
  if (!conf || typeof conf !== 'object') return {};
  const nested = (conf.auto_update && typeof conf.auto_update === 'object')
    ? conf.auto_update
    : ((conf.autoUpdate && typeof conf.autoUpdate === 'object') ? conf.autoUpdate : null);
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
    if (v) auto_update_repo_path = v;
  }
  if (au.auto_update_remote !== undefined || au.remote !== undefined) {
    const v = String(au.remote || au.auto_update_remote || '').trim();
    if (v) auto_update_remote = v;
  }
  if (au.auto_update_branch !== undefined || au.branch !== undefined) {
    const v = String(au.branch || au.auto_update_branch || '').trim();
    if (v) auto_update_branch = v;
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
  auto_update_repo_path = auto_update_repo_path || process.cwd();
  auto_update_remote = auto_update_remote || 'origin';
}

function applyTenantConfig(conf) {
  if (!conf || typeof conf !== 'object') return;

  const hasValue = (v) => v !== undefined && v !== null && !(typeof v === 'string' && v.trim() === '');
  const asNumber = (v, current) => {
    if (!hasValue(v)) return current;
    const n = Number(v);
    return Number.isFinite(n) ? n : current;
  };
  const asString = (v, current = '') => {
    if (!hasValue(v)) return current;
    return String(v).trim();
  };

  if (hasValue(conf.tenantId)) tenantId = asString(conf.tenantId, tenantId).toUpperCase();
  if (hasValue(conf.numero) || hasValue(conf.NUMERO)) numero = asString(conf.numero || conf.NUMERO, numero);
  if (hasValue(conf.mongo_uri) || hasValue(conf.mongoUri)) mongo_uri = asString(conf.mongo_uri || conf.mongoUri, mongo_uri);
  if (hasValue(conf.mongo_db) || hasValue(conf.mongoDb) || hasValue(conf.dbName)) mongo_db = asString(conf.mongo_db || conf.mongoDb || conf.dbName, mongo_db);
  if (conf.status_token !== undefined) status_token = asString(conf.status_token, status_token);

  if (hasValue(conf.puerto)) port = asNumber(conf.puerto, port);
  if (conf.headless !== undefined) headless = parseBoolLike(conf.headless, !!headless);
  seg_desde = asNumber(conf.seg_desde, seg_desde);
  seg_hasta = asNumber(conf.seg_hasta, seg_hasta);
  seg_msg = asNumber(conf.seg_msg, seg_msg);
  seg_tele = asNumber(conf.seg_tele, seg_tele);
  if (conf.dsn !== undefined) dsn = asString(conf.dsn, dsn);
  if (conf.api !== undefined) api = asString(conf.api, api);
  if (conf.msg_inicio !== undefined) msg_inicio = String(conf.msg_inicio ?? '');
  if (conf.msg_fin !== undefined) msg_fin = String(conf.msg_fin ?? '');
  if (conf.msg_lim !== undefined) msg_lim = String(conf.msg_lim ?? '');
  cant_lim = asNumber(conf.cant_lim, cant_lim);
  time_cad = asNumber(conf.time_cad, time_cad);
  if (conf.msg_cad !== undefined) msg_cad = String(conf.msg_cad ?? '');
  if (conf.msg_can !== undefined) msg_can = String(conf.msg_can ?? '');

  lease_ms = asNumber(conf.lease_ms, lease_ms);
  heartbeat_ms = asNumber(conf.heartbeat_ms, heartbeat_ms);
  backup_every_ms = asNumber(conf.backup_every_ms, backup_every_ms);
  if (conf.auth_base_path !== undefined || conf.auth_path !== undefined) {
    auth_base_path = asString(conf.auth_base_path || conf.auth_path, auth_base_path);
  }

  applyAutoUpdateConfig(conf);
}

async function loadTenantConfigFromDbMinimal() {
  try {
    if (!tenantId || !mongo_uri) return null;
    const ok = await ensureMongo();
    if (!ok || !mongoose?.connection?.db) return null;

    const collName = String(process.env.ASISTO_CONFIG_COLLECTION || 'tenant_config').trim() || 'tenant_config';
    const coll = mongoose.connection.db.collection(collName);

    let doc = await coll.findOne({ _id: tenantId });
    if (!doc) doc = await coll.findOne({ tenantId });
    if (!doc) return null;

    const conf = (doc && doc.configuracion && typeof doc.configuracion === 'object') ? doc.configuracion : doc;
    tenantConfig = conf;
    applyTenantConfig(conf);
    return conf;
  } catch (e) {
    try { EscribirLog('loadTenantConfigFromDbMinimal error: ' + String(e?.message || e), 'error'); } catch {}
    return null;
  }
}

async function refreshTenantConfigFromDbPerMessage() {
  try {
    if (!tenantId || !mongo_uri) return tenantConfig;
    const conf = await loadTenantConfigFromDbMinimal();
    if (conf && typeof conf === 'object') {
      tenantConfig = conf;
      applyTenantConfig(conf);
      return conf;
    }
  } catch {}

  try {
    if (tenantConfig && typeof tenantConfig === 'object') {
      applyTenantConfig(tenantConfig);
      return tenantConfig;
    }
  } catch {}

  return null;
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
    for (const p of (parts || [])) if (p && p.type) map[p.type] = p.value;
    const y = map.year || '0000';
    const m = map.month || '00';
    const d = map.day || '00';
    const hh = map.hour || '00';
    const mm = map.minute || '00';
    const ss = map.second || '00';
    return { dayKey: `${y}-${m}-${d}`, atLocal: `${y}-${m}-${d}T${hh}:${mm}:${ss}` };
  } catch {
    const dt = date || new Date();
    const iso = dt.toISOString();
    return { dayKey: iso.slice(0, 10), atLocal: iso.slice(0, 19) };
  }
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
    const cleanContact = String(contact || '').replace(/@c\.us$/i, '').trim();
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

function runCommand(bin, args = [], opts = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(resolveCmdBin(bin), args, {
      cwd: opts.cwd || process.cwd(),
      shell: !!opts.shell,
      env: { ...process.env, ...(opts.env || {}) },
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe']
    });

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

    if (child.stdout) child.stdout.on('data', (d) => { stdout += d.toString(); });
    if (child.stderr) child.stderr.on('data', (d) => { stderr += d.toString(); });
    child.on('error', (err) => {
      if (finished) return;
      finished = true;
      if (timeoutId) clearTimeout(timeoutId);
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
  const out = await runCommand('git', ['rev-parse', '--abbrev-ref', 'HEAD'], { cwd: repoPath, timeout: 20000 });
  return String(out.stdout || '').trim() || 'main';
}

async function autoUpdateCheckAndApply(reason = 'interval') {
  if (!auto_update_enabled || autoUpdateRunning || autoUpdateRestarting) return;
  autoUpdateRunning = true;

  try {
    const repoPath = path.resolve(auto_update_repo_path || process.cwd());
    if (!fs.existsSync(repoPath) || !fs.existsSync(path.join(repoPath, '.git'))) {
      autoUpdateLog(`[AUTO_UPDATE] skip (${reason}): repo_path inválido -> ${repoPath}`, 'event');
      return;
    }

    const branch = await autoUpdateGetBranch(repoPath);
    const remote = auto_update_remote || 'origin';

    if (auto_update_require_clean) {
      const statusOut = await runCommand('git', ['status', '--porcelain'], { cwd: repoPath, timeout: 20000 });
      if (String(statusOut.stdout || '').trim()) {
        autoUpdateLog(`[AUTO_UPDATE] skip (${reason}): working tree con cambios locales`, 'event');
        return;
      }
    }

    const headOut = await runCommand('git', ['rev-parse', 'HEAD'], { cwd: repoPath, timeout: 15000 });
    const localHead = String(headOut.stdout || '').trim();
    await runCommand('git', ['fetch', remote, branch, '--prune'], { cwd: repoPath, timeout: 120000 });
    const remoteRef = `${remote}/${branch}`;
    const remoteHeadOut = await runCommand('git', ['rev-parse', remoteRef], { cwd: repoPath, timeout: 15000 });
    const remoteHead = String(remoteHeadOut.stdout || '').trim();

    if (!localHead || !remoteHead || remoteHead === localHead) {
      autoUpdateLog(`[AUTO_UPDATE] ok (${reason}): sin cambios (${String(localHead || '').slice(0,7)})`, 'event');
      return;
    }

    autoUpdateLog(`[AUTO_UPDATE] update (${reason}): ${localHead.slice(0,7)} -> ${remoteHead.slice(0,7)}`, 'event');

    const changedOut = await runCommand('git', ['diff', '--name-only', `${localHead}..${remoteRef}`], { cwd: repoPath, timeout: 30000 });
    const changedFiles = String(changedOut.stdout || '').split(/\r?\n/).map(s => s.trim()).filter(Boolean);

    await runCommand('git', ['reset', '--hard', remoteRef], { cwd: repoPath, timeout: 120000 });

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
      setTimeout(() => { gracefulShutdown('AUTO_UPDATE'); }, 1200);
    }
  } catch (e) {
    autoUpdateLog(`[AUTO_UPDATE] error (${reason}): ${e?.message || e}`, 'error');
  } finally {
    autoUpdateRunning = false;
  }
}

function startAutoUpdateScheduler() {
  if (!auto_update_enabled) {
    autoUpdateLog('[AUTO_UPDATE] desactivado', 'event');
    return;
  }
  if (autoUpdateTimer) return;

  const repoPath = path.resolve(auto_update_repo_path || process.cwd());
  autoUpdateLog(`[AUTO_UPDATE] activado repo=${repoPath} remote=${auto_update_remote} branch=${auto_update_branch || '(auto)'} every=${auto_update_check_every_ms}ms startupDelay=${auto_update_startup_delay_ms}ms`, 'event');

  setTimeout(() => {
    autoUpdateCheckAndApply('startup').catch(() => {});
  }, Math.max(0, Number(auto_update_startup_delay_ms) || 0));

  autoUpdateTimer = setInterval(() => {
    autoUpdateCheckAndApply('interval').catch(() => {});
  }, Math.max(60000, Number(auto_update_check_every_ms) || 600000));
}

async function getLockDocSafe() {
  try {
    if (await ensureMongo() && LockModel && lockId) {
      const doc = await LockModel.findById(lockId).lean();
      if (doc) return doc;
    }
  } catch {}

  return {
    _id: lockId || `${tenantId}:${numero}`,
    tenantId,
    numero,
    holderId: instanceId,
    host: os.hostname(),
    pid: process.pid,
    state: localWsPanelState,
    startedAt: null,
    lastSeenAt: new Date(),
    lastQrAt,
    lastQrDataUrl
  };
}

function requireStatusToken(req, res, next) {
  if (!status_token) return next();
  const t = String(req.query?.token || req.headers['x-status-token'] || '');
  if (t && t === String(status_token)) return next();
  return res.status(401).json({ ok: false, error: 'unauthorized' });
}


async function ensureMongo() {
  try {
    if (mongoReady && mongoose?.connection?.readyState === 1 && mongoose?.connection?.db) {
      initMongoModelsIfNeeded();
      return true;
    }
    if (mongoConnectingPromise) {
      const ok = await mongoConnectingPromise;
      if (ok) initMongoModelsIfNeeded();
      return ok;
    }
    if (!mongo_uri) return false;

    mongoConnectingPromise = (async () => {
      try {
        await mongoose.connect(mongo_uri, {
          dbName: (mongo_db || tenantId || "super"),
          autoIndex: true,
          serverSelectionTimeoutMS: 15000
        });

        if (!mongoose.connection.db) {
          await new Promise((resolve, reject) => {
            const t = setTimeout(() => reject(new Error("mongo_db_not_ready")), 15000);
            mongoose.connection.once("connected", () => { clearTimeout(t); resolve(); });
          });
        }

        mongoReady = true;
        initMongoModelsIfNeeded();
        return true;
      } catch (e) {
        try { console.log("Mongo connect error:", e?.message || e); } catch {}
        try { await mongoose.disconnect(); } catch {}
        mongoReady = false;
        return false;
      } finally {
        mongoConnectingPromise = null;
      }
    })();

    const ok = await mongoConnectingPromise;
    if (ok) initMongoModelsIfNeeded();
    return ok;
  } catch (e) {
    mongoReady = false;
    mongoConnectingPromise = null;
    return false;
  }
}

function initMongoModelsIfNeeded() {
  try {
    if (!mongoose?.connection?.db) return;

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

    if (!PolicyModel) {
      const PolicySchema = new mongoose.Schema(
        {
          _id: { type: String },
          tenantId: { type: String, index: true },
          numero: { type: String, index: true },
          disabled: { type: Boolean, default: false }
        },
        { collection: "wa_wweb_policies" }
      );
      PolicyModel = mongoose.models.WaWwebPolicy || mongoose.model("WaWwebPolicy", PolicySchema);
    }

    if (!ActionModel) {
      const ActionSchema = new mongoose.Schema(
        {
          lockId: { type: String, index: true },
          action: { type: String, index: true },
          reason: { type: String },
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
  } catch {}
}

// --- LocalAuth backup to Mongo (GridFS) ---
 // Evita uploads concurrentes que generan múltiples GridFS files/chunks
 let savingLocalAuth = false;
  // Evita multiplicar timers/intervals si 'ready' se dispara más de una vez
  let localAuthBackupSchedulerStarted = false;
  let localAuthBackupInterval = null;

function zipDirectoryToBuffer(dirPath) {
  if (!AdmZip) {
    const msg = "Falta dependencia adm-zip. Instalá: npm i adm-zip";
    try { console.log(msg); } catch {}
    throw new Error("missing_dependency_adm_zip");
  }
  const zip = new AdmZip();

  // Backup MINIMAL (para que no sea gigante y para evitar EBUSY en Windows).
  // Incluye lo necesario para que LocalAuth restaure la sesión.
  const INCLUDE_MIN = [
    "Local State",
    "Default/Local Storage",
    //"Default/IndexedDB",
    "Default/Cookies",
    "Default/Cookies-journal",
    "Default/Preferences",
    "Default/Secure Preferences",
  ];

  const INCLUDE_FULL = [
    ...INCLUDE_MIN,
    "Default/Web Storage",
    "Default/Session Storage",
    "Default/Network",
    "Default/TransportSecurity",
  ];

  // Compat: usar el mismo flag que Asisto. Si querés separarlo: SUPER_AUTH_BACKUP_FULL=1.
  const useFull = String(process.env.SUPER_AUTH_BACKUP_FULL || process.env.ASISTO_AUTH_BACKUP_FULL || "").trim() === "1";
  const INCLUDE = useFull ? INCLUDE_FULL : INCLUDE_MIN;

  const normalizeRel = (p) => String(p || "").split("\\").join("/").replace(/^\/+/, "");

  function addFile(absFile, zipDir) {
    try {
      zip.addLocalFile(absFile, zipDir || "");
      return;
    } catch (e) {
      const msg = String(e?.message || e || "");
      const busy = msg.includes("EBUSY") || msg.toLowerCase().includes("busy") || msg.toLowerCase().includes("locked");
      if (busy) {
        // En Windows algunos archivos quedan bloqueados para escritura pero se pueden LEER.
        // Probamos agregarlos al zip desde buffer; si falla, los omitimos.
        try {
          const data = fs.readFileSync(absFile);
          const base = path.basename(absFile);
          const zpath = (zipDir && String(zipDir).length) ? `${zipDir}/${base}` : base;
          zip.addFile(zpath, data);
          return;
        } catch {
          return;
        }
      }
      throw e;
    }
  }

  function walk(absDir, relZipBase) {
    let entries = [];
    try {
      entries = fs.readdirSync(absDir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const ent of entries) {
      const abs = path.join(absDir, ent.name);
      const nextRel = relZipBase ? `${relZipBase}/${ent.name}` : ent.name;
      if (ent.isDirectory()) {
        walk(abs, nextRel);
      } else if (ent.isFile()) {
        addFile(abs, relZipBase || "");
      }
    }
  }

  for (const rel of INCLUDE) {
    const relNorm = normalizeRel(rel);
    const abs = path.join(dirPath, ...relNorm.split("/"));
    if (!fs.existsSync(abs)) continue;

    try {
      const st = fs.statSync(abs);
      if (st.isDirectory()) {
        walk(abs, relNorm);
      } else if (st.isFile()) {
        const zipDir = relNorm.includes("/") ? relNorm.split("/").slice(0, -1).join("/") : "";
        addFile(abs, zipDir);
      }
    } catch {
      continue;
    }
  }

  return zip.toBuffer();
}

function extractZipBufferToDir(buf, destDir) {
  if (!AdmZip) throw new Error("missing_dependency_adm_zip");
  fs.mkdirSync(destDir, { recursive: true });
  const zip = new AdmZip(buf);
  zip.extractAllTo(destDir, true);
}

function getBucket() {
  if (!mongoose?.connection?.db) throw new Error("mongo_db_not_ready");
  if (!GridFSBucket) throw new Error("mongodb_gridfsbucket_not_available");
  return new GridFSBucket(mongoose.connection.db, { bucketName: "wa_localauth" });
}

function bucketDeleteAsync(bucket, fileId) {
  return new Promise((resolve, reject) => {
    try {
      bucket.delete(fileId, (err) => {
        if (err) return reject(err);
        resolve();
      });
    } catch (e) {
      reject(e);
    }
  });
}

async function deleteExistingFilename(bucket, filename) {
  // OJO: GridFSBucket.delete() usa callback (no Promise) en varias versiones del driver.
  // Si hacemos `await bucket.delete(...)` puede NO borrar y la colección wa_localauth.chunks crece sin límite.
  const existing = await bucket.find({ filename }).toArray();
  for (const f of (existing || [])) {
    try { await bucketDeleteAsync(bucket, f._id); } catch {}
  }
}

async function pruneOldLocalAuthFiles(bucket, filename, keep = 1) {
  // Mantiene solo los últimos N uploads del mismo filename (por si hubo carreras / duplicados)
  const files = await bucket.find({ filename }).sort({ uploadDate: -1 }).toArray();
  const extra = (files || []).slice(Math.max(0, keep));
  for (const f of extra) {
    try { await bucketDeleteAsync(bucket, f._id); } catch {}
  }
}

 
function escapeRegex(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function deleteTempBackups(bucket, tenantId, numero) {
  // borra residuos de uploads temporales (si quedaron por cortes)
  try {
    const bucketName = bucket?.s?.options?.bucketName || "wa_localauth";
    const filesColl = bucket.s.db.collection(`${bucketName}.files`);
    const rx = `^LocalAuth-.*_${escapeRegex(tenantId)}_${escapeRegex(numero)}\\.zip\\.tmp-`;
    const cursor = filesColl.find({ filename: { $regex: rx } }, { projection: { _id: 1, filename: 1 } });
    const ids = [];
    await cursor.forEach(d => ids.push(d._id));
    for (const id of ids) {
      try { await bucketDeleteAsync(bucket, id); } catch {}
    }
    if (ids.length) sessionLog(`LocalAuth: TEMP cleanup deleted=${ids.length}`);
  } catch {}
}

async function cleanupLocalAuthGlobalKeepOnlyCurrent(bucket, keepFilename) {
  // Deja SOLO 1 backup para este tenantId/numero:
  // - keepFilename (el actual)
  // - borra cualquier otro file de este tenantId/numero (incluye clientId viejos)
  // - borra temporales .tmp-*
  // - borra duplicados del keepFilename (deja el más nuevo)
  try {
    if (!tenantId || !numero) return;
    const bucketName = bucket?.s?.options?.bucketName || "wa_localauth";
    const filesColl = bucket.s.db.collection(`${bucketName}.files`);

    // 1) temporales
    await deleteTempBackups(bucket, tenantId, numero);

    // 2) borrar TODO lo que sea de este tenant/numero y NO sea el keepFilename
    //    - primero por metadata (si existe)
    //    - fallback por regex de numero en filename (por si backups viejos no tenían metadata)
    const numeroRx = `_${escapeRegex(numero)}\\.zip`;
    const q = {
      $or: [
        { "metadata.tenantId": tenantId, "metadata.numero": numero },
        { filename: { $regex: numeroRx } }
      ]
    };
    const cursor = filesColl.find(q, { projection: { _id: 1, filename: 1, uploadDate: 1 } });
   const toDelete = [];
    const keepDups = [];
    await cursor.forEach(d => {
      if (!d || !d._id) return;
      if (d.filename === keepFilename) keepDups.push(d);
      else toDelete.push(d._id);
    });
    for (const id of toDelete) {
      try { await bucketDeleteAsync(bucket, id); } catch {}
    }

    // 3) si hay duplicados del keepFilename, dejar solo el más nuevo
    if (keepDups.length > 1) {
      keepDups.sort((a, b) => {
        const da = a.uploadDate ? new Date(a.uploadDate).getTime() : 0;
        const db = b.uploadDate ? new Date(b.uploadDate).getTime() : 0;
        return db - da;
      });
      const extra = keepDups.slice(1);
      for (const d of extra) {
        try { await bucketDeleteAsync(bucket, d._id); } catch {}
      }
      sessionLog(`LocalAuth: GLOBAL prune keep=1 deleted=${toDelete.length + extra.length}`);
    } else if (toDelete.length) {
      sessionLog(`LocalAuth: GLOBAL prune keep=1 deleted=${toDelete.length}`);
    }

    // 4) chunks huérfanos (si la función existe en este script)
    try {
      if (typeof cleanupOrphanGridFSChunks === "function") {
        await cleanupOrphanGridFSChunks("wa_localauth");
      }
    } catch {}
  } catch {}
}

async function saveLocalAuthToMongo(label = "") {
  if (!tenantId || !numero) return;
  if (!await ensureMongo()) return;

  const clientId = `super_${tenantId}_${numero}`;
  const sessionDir = getLocalAuthSessionDir(clientId);

  // Evita backups de sesiones incompletas (ej: carpeta existe pero sin data clave)
  if (!localAuthDirHasKeyData(sessionDir)) {
    try { EscribirLog(`LocalAuth: BACKUP skip (sin data clave) (${sessionDir})`, "event"); } catch {}
    return;
  }

  const filename = `LocalAuth-${clientId}.zip`;
  const bucket = getBucket();

  const buf = zipDirectoryToBuffer(sessionDir);
  const bytes = buf?.length || 0;

   // Evitar que se ejecuten dos backups al mismo tiempo (crea duplicados)
   if (savingLocalAuth) {
     sessionLog(`LocalAuth: BACKUP skip (ya hay uno en curso) label=${label}`);
     return;
   }
   savingLocalAuth = true;

   try {
    // MODO ESTRICTO: antes de subir, borra TODOS los backups previos de ese filename.
    // Así garantizás que nunca queden backups viejos aunque falle el prune.
    await deleteExistingFilename(bucket, filename);

     await new Promise((resolve, reject) => {
       const up = bucket.openUploadStream(filename, {
         metadata: {
           tenantId, numero, clientId, label,
           host: os.hostname(),
           pid: process.pid,
           savedAt: new Date()
         }
       });
       up.on("error", reject);
       up.on("finish", resolve);
       up.end(buf);
     });

    // Redundancia: por si hubo carreras raras, dejamos 1.
    await pruneOldLocalAuthFiles(bucket, filename, 1);

     // Limpieza fuerte post-backup: temporales + chunks huérfanos
     await cleanupLocalAuthAfterBackup(bucket, filename);
  sessionLog(`LocalAuth: BACKUP OK (${label}) filename=${filename}`);

  // Limpieza GLOBAL: dejar SOLO 1 backup para este tenantId/numero + borrar temporales + huérfanos
  await cleanupLocalAuthGlobalKeepOnlyCurrent(bucket, filename);

     
     try { EscribirLog(`REMOTE SESSION SAVED (LocalAuth->Mongo) bytes=${bytes} label=${label}`, "event"); } catch {}
   } catch (e) {
     const msg = `LocalAuth: BACKUP FAIL (${label}) ${e?.message || e}`;
     try { console.log(msg); } catch {}
     try { EscribirLog(msg, "error"); } catch {}
   } finally {
     savingLocalAuth = false;
   }

  
}

async function restoreLocalAuthFromMongoIfNeeded() {
  if (!tenantId || !numero) return false;
  if (!await ensureMongo()) return false;

  const clientId = `super_${tenantId}_${numero}`;
  const sessionDir = getLocalAuthSessionDir(clientId);

  // Si la sesión local tiene datos reales, no restauramos.
  if (localAuthDirHasKeyData(sessionDir)) {
    try { EscribirLog(`LocalAuth: sesión local OK (${sessionDir}). No restaura.`, "event"); } catch {}
    return true;
  }

  // Si existe pero está incompleta, limpiamos para restaurar desde Mongo.
  if (dirLooksPopulated(sessionDir) && !localAuthDirHasKeyData(sessionDir)) {
    sessionLog(`LocalAuth: carpeta existe pero INCOMPLETA -> limpio y restauro (${sessionDir})`);
    try { fs.rmSync(sessionDir, { recursive: true, force: true }); } catch {}
  }

  const filename = `LocalAuth-${clientId}.zip`;
  const bucket = getBucket();

  const files = await bucket.find({ filename }).sort({ uploadDate: -1 }).limit(1).toArray();
  if (!files || files.length === 0) {
    try { EscribirLog(`LocalAuth: no hay backup en Mongo (${filename}). Se pedirá QR.`, "event"); } catch {}
    return false;
  }

  const chunks = [];
  await new Promise((resolve, reject) => {
    const dl = bucket.openDownloadStream(files[0]._id);
    dl.on("data", (d) => chunks.push(d));
    dl.on("error", reject);
    dl.on("end", resolve);
  });

  const buf = Buffer.concat(chunks);
  const bytes = buf?.length || 0;
  try { fs.rmSync(sessionDir, { recursive: true, force: true }); } catch {}
  extractZipBufferToDir(buf, sessionDir);

  if (!localAuthDirHasKeyData(sessionDir)) {
    sessionLog(`LocalAuth: RESTORE terminó pero sesión sigue incompleta (${sessionDir}). Se pedirá QR.`);
    return false;
  }
  sessionLog(`LocalAuth: RESTORE OK (${sessionDir}).`);
  try { EscribirLog(`LocalAuth: restore desde Mongo OK bytes=${bytes} -> ${sessionDir}`, "event"); } catch {}
  return true;
}

function scheduleLocalAuthBackups() {
    // Si 'ready' vuelve a ejecutarse (reconnect), NO vuelvas a crear timers/intervalos
  if (localAuthBackupSchedulerStarted) {
    sessionLog("LocalAuth backup schedule: ya estaba iniciado (skip)");
    return;
  }
  localAuthBackupSchedulerStarted = true;

  if (!AdmZip) {
    console.log("ERROR: falta dependencia adm-zip. Instalá: npm i adm-zip");
    try { EscribirLog("ERROR: falta dependencia adm-zip. Instalá: npm i adm-zip", "error"); } catch {}
  }
  const safeSave = (label) => saveLocalAuthToMongo(label).catch((e) => {
    const msg = `saveLocalAuthToMongo failed (${label}): ${e?.message || e}`;
    try { console.log(msg); } catch {}
    try { EscribirLog(msg, "error"); } catch {}
  });

  // backups diferidos (evita capturar sesión incompleta)
  setTimeout(() => safeSave("ready+15s"), 15_000);
  setTimeout(() => safeSave("ready+60s"), 60_000);
  setTimeout(() => safeSave("ready+120s"), 120_000);

  const every = Math.max(60_000, Number(backup_every_ms) || 300_000);

  if (localAuthBackupInterval) {
    try { clearInterval(localAuthBackupInterval); } catch {}
    localAuthBackupInterval = null;
  }
  localAuthBackupInterval = setInterval(() => safeSave("interval"), every);

  try { EscribirLog(`LocalAuth backup schedule: every=${every}ms`, "event"); } catch {}
}

function isDetachedFrameError(err) {
  const msg = String(err?.message || err || "");
  return msg.includes("detached Frame") || msg.includes("Attempted to use detached Frame");
}

async function initializeWithRetry(clientInstance, maxRetries = 5) {
  for (let i = 1; i <= maxRetries; i++) {
    try {
      await clientInstance.initialize();
      return true;
    } catch (e) {
      if (!isDetachedFrameError(e) || i === maxRetries) throw e;
      try { console.log(`initialize retry ${i}/${maxRetries} (detached frame)`); } catch {}
      await sleep(1500 * i);
    }
  }
  return false;
}

async function createClientIfNeeded() {
  if (client) return client;

  const ok = await ensureMongo();
  if (!ok) throw new Error("mongo_not_ready");
  if (!tenantId || !numero) throw new Error("tenant_or_numero_missing");

  const clientId = `super_${tenantId}_${numero}`;

  // Restaura sesión (si no existe carpeta local)
  try { await restoreLocalAuthFromMongoIfNeeded(); } catch {}

  client = new Client({
    restartOnAuthFail: true,
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
    authStrategy: new LocalAuth({
      clientId,
      dataPath: getAuthBasePath()
    })
  });

  attachClientHandlers();
  return client;
}

// --- Lock / lease ---
async function updateLockStateSafe(state) {
  try {
    localWsPanelState = String(state || localWsPanelState || 'idle');
    if (!isOwner) return;
    if (!await ensureMongo()) return;
    if (!lockId) return;
    await LockModel.updateOne(
      { _id: lockId, holderId: instanceId },
      {
        $set: {
          tenantId,
          tenantid: tenantId,
          numero,
          holderId: instanceId,
          host: os.hostname(),
          pid: process.pid,
          state: localWsPanelState,
          lastSeenAt: new Date()
        }
      }
    );
  } catch {}
}


async function getPolicySafe() {
  try {
    if (!await ensureMongo()) return null;
    if (!PolicyModel) return null;
    const tid = String(tenantId || '');
    const num = String(numero || '');
    const p = await PolicyModel.findOne({
      numero: num,
      $or: [
        { tenantId: tid },
        { tenantid: tid }
      ]
    }).lean();
    return p || null;
  } catch {
    return null;
  }
}

async function updateLockQrDataSafe(qrDataUrl, qrAtIso) {
  try {
    if (qrDataUrl) lastQrDataUrl = String(qrDataUrl);
    if (qrAtIso) lastQrAt = String(qrAtIso);
    localWsPanelState = 'qr';

    if (!isOwner) return;
    if (!await ensureMongo()) return;
    if (!lockId) return;

    await LockModel.updateOne(
      { _id: lockId, holderId: instanceId },
      {
        $set: {
          tenantId,
          tenantid: tenantId,
          numero,
          holderId: instanceId,
          host: os.hostname(),
          pid: process.pid,
          state: 'qr',
          lastSeenAt: new Date(),
          lastQrAt: String(qrAtIso || ''),
          lastQrDataUrl: String(qrDataUrl || '')
        }
      }
    );
  } catch {}
}

async function tryAcquireLock() {
  if (!tenantId || !numero || !mongo_uri) {
    console.log("ERROR: Falta tenantId/numero/mongo_uri en configuracion.json. No se inicia WhatsApp.");
    return false;
  }
  lockId = `${tenantId}:${numero}`;

  const okMongo = await ensureMongo();
  if (!okMongo) {
    console.log("ERROR: No se pudo conectar a Mongo. No se inicia WhatsApp.");
    return false;
  }

  const now = new Date();
  const stale = new Date(now.getTime() - (Number(lease_ms) || 30000));

  const existing = await LockModel.findById(lockId).lean();
  if (existing) {
    const last = existing.lastSeenAt ? new Date(existing.lastSeenAt) : null;
    const isStale = !last || last < stale;
    const isMine = existing.holderId === instanceId;

    if (!isMine && !isStale) {
      isOwner = false;
      return false;
    }

    const doc = await LockModel.findOneAndUpdate(
      {
        _id: lockId,
        $or: [
          { holderId: instanceId },
          { lastSeenAt: { $lt: stale } },
          { lastSeenAt: { $exists: false } }
        ]
      },
      {
        $set: {
          tenantId,
          numero,
          holderId: instanceId,
          host: os.hostname(),
          pid: process.pid,
          state: "standby",
          startedAt: now,
          lastSeenAt: now
        }
      },
      { upsert: false, new: true }
    ).lean();

    isOwner = !!(doc && doc.holderId === instanceId);
    return isOwner;
  }

  const doc = await LockModel.findOneAndUpdate(
    { _id: lockId },
    {
      $setOnInsert: { createdAt: now },
      $set: {
        tenantId,
        tenantid: tenantId,
        numero,
        holderId: instanceId,
        host: os.hostname(),
        pid: process.pid,
        state: "standby",
        startedAt: now,
        lastSeenAt: now
      }
    },
    { upsert: true, new: true }
  ).lean();

  isOwner = !!(doc && doc.holderId === instanceId);
  return isOwner;
}

function startHeartbeat() {
  if (heartbeatTimer) return;
  heartbeatTimer = setInterval(async () => {
    try {
      if (!isOwner) return;
      if (!await ensureMongo()) return;
      if (!lockId) return;

      const pol = await getPolicySafe();
      if (pol && pol.disabled === true) {
        await updateLockStateSafe('disabled').catch(() => {});
        if (clientStarted) {
          try { await client.destroy(); } catch {}
          clientStarted = false;
          try { client = null; } catch {}
        }
        return;
      }

      const r = await LockModel.updateOne(
        { _id: lockId, holderId: instanceId },
        { $set: { state: localWsPanelState || 'online', lastSeenAt: new Date() } }
      );

      if (!r || r.matchedCount === 0) {
        isOwner = false;
        if (clientStarted) {
          try { await client.destroy(); } catch {}
          clientStarted = false;
        }
      } else if (!clientStarted && !startingNow && localWsPanelState === 'disabled') {
        await startClientInitialize().catch(() => {});
      }
    } catch {}
  }, heartbeat_ms || 10000);
}

async function startClientInitialize() {
  if (clientStarted) return;
  if (!isOwner) return;
  if (startingNow) return;
  startingNow = true;

  try {
    await createClientIfNeeded();
  } catch (e) {
    clientStarted = false;
    console.log("No se pudo crear cliente WhatsApp:", e?.message || e);
    return;
  }

  try {
    const pol = await getPolicySafe();
    if (pol && pol.disabled === true) {
      await updateLockStateSafe("disabled").catch(() => {});
      startingNow = false;
      return;
    }
  } catch {}

  console.log("LOCK OK -> inicializando WhatsApp...");
  updateLockStateSafe("starting").catch(() => {});

  try {
    await initializeWithRetry(client, 5);
    clientStarted = true;
  } catch (e) {
    clientStarted = false;
    console.log("Error al inicializar WhatsApp:", e?.message || e);
    const msg = String(e?.message || e || "");
    if (msg.includes("browser is already running")) {
      console.log("TIP: Se detectó un Chrome ya corriendo para este userDataDir. Evitar doble instancia.");
    }
  } finally {
    startingNow = false;
  }
}

async function bootstrapWithLock() {
  const ok = await tryAcquireLock();
  if (ok) {
    isOwner = true;
    try { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } } catch {}
    startHeartbeat();
    startActionPoller();
    await startClientInitialize();
    return;
  }

  console.log(`STANDBY: sesión activa en otra PC (${lockId}). No se inicializa WhatsApp acá.`);

  if (!pollTimer) {
    pollTimer = setInterval(async () => {
      try {
        const ok2 = await tryAcquireLock();
        if (ok2) {
          try { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } } catch {}
          console.log("LOCK TOMADO (otra PC cayó) -> iniciando...");
          startHeartbeat();
          startActionPoller();
          await startClientInitialize();
        }
      } catch {}
    }, 8000);
  }
}

async function handleActionDoc(doc) {
  const action = String(doc?.action || '').toLowerCase();
  const reason = String(doc?.reason || '');

  try {
    if (action === 'restart') {
      EscribirLog('Accion RESTART recibida: ' + reason, 'event');
      await updateLockStateSafe('restarting').catch(() => {});

      try { if (client) await client.destroy(); } catch {}
      try { client = null; } catch {}
      clientStarted = false;

      if (isOwner && !startingNow) {
        await startClientInitialize();
      }
      return 'restarted';
    }

    if (action === 'release') {
      EscribirLog('Accion RELEASE recibida: ' + reason, 'event');
      try { if (client) await client.destroy(); } catch {}
      try { client = null; } catch {}
      clientStarted = false;
      localWsPanelState = 'offline';
      try { await updateLockStateSafe('offline'); } catch {}
      isOwner = false;
      return 'released';
    }

    if (action === 'resetauth') {
      EscribirLog('Accion RESET AUTH recibida: ' + reason, 'event');
      try { if (client && typeof client.logout === 'function') await client.logout(); } catch {}
      try { if (client) await client.destroy(); } catch {}
      try { client = null; } catch {}
      clientStarted = false;
      localWsPanelState = 'offline';
      try { await updateLockStateSafe('offline'); } catch {}
      isOwner = false;
      return 'reset_auth_requested';
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

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
//                 API PARA ESCUCHA DE MENSAJES ENTRANTES
//
//
//
//
//
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


function attachClientHandlers(){
  client.on('message_create',async message => {

    var telefonoTo = message.to;
     var telefonoFrom = message.from;

     telefonoTo = telefonoTo.replace('@c.us','');
     telefonoFrom = telefonoFrom.replace('@c.us','');

     var resp = null;


     if(telefonoFrom == 'status@broadcast'){
       console.log("mensaje de estado");
       return
     }
    /* if(message.type !== 'chat'){
       console.log("mensaje <> texto");
       return
     }*/
    // console.log(message);
    /* console.log('/////////////////////////////////////////////////////////////////////////////');
     console.log('from '+telefonoFrom+'   to '+telefonoTo+'  qr '+ telefono_qr);
     console.log('id '+message.id._serialized+ '  '+ message.id.id);
     console.log('id_msg '+id_msg);
     //const data7 = await connection.query("update gen_mensajes set id_ws = '"+message.id.id+"' where id='"+id_msg+"'");
   // id_msg = 0;*/

  });

  client.on('message_ack', (message2, ack) => {
    // Este handler puede dispararse en situaciones donde todavía no hay
    // un id_msg válido (o incluso antes de abrir ODBC). Si mandamos 'undefined'
    // a SQL Anywhere, explota: "Cannot convert 'undefined' to a numeric".
    try {
      if (!connection) return;
      if (!message2 || !message2.id) return;
      // Nos interesan sólo ACKs de mensajes salientes
      if (message2.fromMe !== true) return;

      const wsId = message2.id.id;
      if (!wsId) return;

      // Preferimos el mapa (robusto ante concurrencia). Fallback a id_msg.
      const dbId = pendingAck.get(wsId) ?? id_msg;
      const dbIdNum = Number(dbId);
      if (!Number.isFinite(dbIdNum) || dbIdNum <= 0) {
        console.log('ACK sin dbId válido (skip). wsId=' + wsId + ' id_msg=' + id_msg);
        return;
      }

      console.log('ACK id_msg ' + dbIdNum);

      connection.query("update es_mensajes set id_ws = '" + wsId + "' where id='" + dbIdNum + "'");
      connection.query("update es_mensajes set estado_ws = '" + ack + "' where id_ws='" + wsId + "'");

      pendingAck.delete(wsId);
      // sólo reseteamos el global si lo acabamos de usar como fallback
      if (dbId === id_msg) id_msg = 0;

      console.log('Mensaje ' + wsId);
      console.log('Estado ' + ack);
    } catch (e) {
      console.log('message_ack handler error:', e?.message || e);
    }
  });




  client.on('message', async message => {

    try { await refreshTenantConfigFromDbPerMessage(); } catch {}
    try { RecuperarJsonConfMensajes(); } catch {}

    //console.log('listo');
   var telefonoTo = message.to;
    var telefonoFrom = message.from;

     telefonoTo = telefonoTo.replace('@c.us','');
     telefonoFrom = telefonoFrom.replace('@c.us','');

     console.log('Mensaje ' + JSON.stringify(message.id));

     console.log('Mensaje: ' +message.id._serialized);
     telefonoTo = '543462674128@c.us';
     var resp = null;


     if(telefonoFrom == 'status@broadcast'){
      //console.log("mensaje de estado");
      return
     }

     try {
       await logMessageStat('in', telefonoFrom, { body: message.body || '', type: message.type || 'chat', hasMedia: !!message.hasMedia });
     } catch {}

     if (message.from !='5493462674128@c.us'  ){
      let contact = await client.getContactById(message.from);
      //let identificacion= JSON.stringify(contact.pushname )
      //console.log(JSON.stringify(contact));
      let name;
      if(contact.isBusiness==true){

         name = contact.name;

      } else{


         name = contact.name;

      }
     // console.log(JSON.stringify(message));


    if(message.type=="chat"){
     // console.log("chat");
       await safeSendMessage('5493462674128@c.us', contact.id.user+': '+name+': '+ message.body );
     }


    if(message.type!="chat"){
      //console.log("no chat");
        //////////////////
      var signatures = {
        JVBERi0: "application/pdf",
        R0lGODdh: "image/gif",
        R0lGODlh: "image/gif",
        iVBORw0KGgo: "image/png",
        "/9j/": "image/jpg"
        };

        function detectMimeType(b64) {
    for (var s in signatures) {
      if (b64.indexOf(s) === 0) {
      return signatures[s];
    }
  }
  }
  if(message.type!="ptt"){
  var media = await new MessageMedia(detectMimeType(message._data.body), message._data.body, message.body);
  }else{
    await safeSendMessage('5493462674128@c.us', contact.id.user+': '+name+': AUDIO' );
    //var media = await new MessageMedia("audio/ogg; codecs=opus", message.deprecatedMms3Url, '_');
  }

  await safeSendMessage('5493462674128@c.us', media, { caption: contact.id.user+': '+name });
  }



     }


    //var telefono_api = validarTelefono(telefonoFrom);

     if (message.from=='5493462674128@c.us'  ){

        var mensaje = message.body;

        var l_fun = mensaje.slice(0,2);
        var l_par = mensaje.slice(3,11);

        l_fun = l_fun.trim();
        l_par = l_par.trim();
        console.log(l_fun);
        console.log(l_par);

        if(l_fun == '/e'){

            const connection = await odbc.connect('DSN='+dsn);


            if( l_par == 'l' )      {

              const data2 = await connection.query("SELECT ven_remitos_cabecera.fecha,forma_de_pago.descripcion, ven_remitos_cabecera.total, es_datos_entregas.forma_pago, es_horarios.hora_desde, clientes.razon_social, ven_remitos_cabecera.nrotransaccion , es_datos_entregas.direccion_entrega  FROM ven_remitos_cabecera, es_datos_entregas,  es_horarios, forma_de_pago ,clientes WHERE (ven_remitos_cabecera.transaccion = es_datos_entregas.transaccion ) and  (ven_remitos_cabecera.transaccion = es_datos_entregas.transaccion )and  ( ven_remitos_cabecera.letra = es_datos_entregas.letra ) and( ven_remitos_cabecera.nrotransaccion = es_datos_entregas.nrotransaccion ) and  ( ven_remitos_cabecera.ptodeventa = es_datos_entregas.ptodeventa ) and  ( es_horarios.cod_horario = es_datos_entregas.cod_horario) and( forma_de_pago.codigo = es_datos_entregas.forma_pago )   and( ven_remitos_cabecera.cliente = clientes.codigo )  and (  es_horarios.fecha > DateAdd(day,-1,GetDate() )) order by es_horarios.hora_desde ;  "
              );
              var tam2 = data2.length;

              let now = new Date();

              for(i=0;i<=tam2 -1;i++) {
                await safeSendMessage(message.from, data2[i].razon_social+'  ' +data2[i].nrotransaccion+' ' +data2[i].hora_desde+' / $' +data2[i].total+' -- '+data2[i].direccion_entrega );
                console.log(data2[i].fecha);
              }


            }
            else{

              if(l_par =='?'){
                await safeSendMessage(message.from,'*------AYUDA------* \n */e l* -> listado de pedidos \n */e 0000XXXX* -> Envía Mensaje de Entrega*' );

              }else{
                console.log("update es_datos_entregas set observaciones = 'e' where nrotransaccion = '"+l_par +"'");
                const data3= await connection.query("update es_datos_entregas set observaciones = 'e' where nrotransaccion = '"+l_par +"'");

                await safeSendMessage(message.from,'Mensaje Entrega Actualizado' );
              }  

              connection.close;

            console.log(l_fun);
            console.log(l_par);
          }

        }
       // 

     }


       // await io.emit('message', 'Mensaje: '+message.from+': '+ message.body );
       // client.sendMessage(telefonoTo,'Mensaje: '+message.from+': '+ message.body );


     /*console.log(JSON.stringify(jsonTexto));

     try{


           resp = await fetch(url, {
             method: "POST",
             body: JSON.stringify(jsonTexto),
             headers: {"Content-type": "application/json; charset=UTF-8"}
           })

           .catch(err => console.log("err "+err))

           if (resp.ok  ) {


             var json = await resp.json()
             //var json = await resp.text()
               console.log("json "+JSON.stringify(json));

               for(var i in json){
                 if(json[i].cod_error){ 
                   var mensaje = json[i].msj_error;
                   }else{
                   var mensaje = json[i].respuesta;
                   }
                   for (var i = 0; i < 20; i++){
                     mensaje = mensaje.replace("|","\n");
                   }
               await client.sendMessage(message.from,mensaje );

               await io.emit('message', 'Respuesta: '+message.from+': '+ mensaje );
               }
                  }
           else
           {
             console.log("ApiWhatsapp - Response ERROR")
             return 'error'
           }
       }
       catch (err) {
         console.log(err)
         return 'error'
       }

   */
  });


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //
  //                 API PARA RECUPERAR MENSAJES A ENVIAR
  //
  //
  //
  //
  //
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////




  async function queryAccess() {

      bandera_query=1;
      var a= 0;
      var telefono = '549'+telefono_qr+'@c.us';

      console.log("Telefono Habilitado:"+telefono);

      const l_cliente = JSON.stringify(client.info);
      console.log("cliente:"+client.info.me.user+'@c.us');
      telefono_local = client.info.me.user+'@c.us';

      if(telefono != telefono_local){
        console.log(telefono_local+' '+telefono);
        console.log("TELEFONO NO AUTORIZADO A UTILIZAR WSCHATBOT!!!")
        io.emit('message', "TELEFONO NO AUTORIZADO A UTILIZAR WSCHATBOT!!!");
        return
      }

      // await client.sendMessage(telefono,'*👋 bienvenido a WSCHATBOT');

     // console.log("dsn "+dsn+"; encoding=ISO8859-1");
     console.log("dsn "+dsn+"; encoding=UTF-8");
       connection = await odbc.connect('DSN='+dsn+'; charset=UTF8');

      console.log("conectado a Manager..."+dsn);
      console.log("esperando...");

      await sleep(1000);


      while(a < 1){

        RecuperarJsonConfMensajes();
       await  enviar_mensajes_compra();

       await enviar_mensajes_entrega();

       await enviar_mensajes_info();

       await sleep(seg_msg);
      }


      ////////////////////////////////////////FUNCION ENVIAR MENSAJE COMPRA REALIZADA /////////////////////////////////////////////////////////////////////////////////////////////////////////////
      async function enviar_mensajes_compra(){

        const data2 = await connection.query("select  codigo, clientes.razon_social, clientes.telefono, direccion_entrega, hora_desde, hora_hasta, total from clientes, es_datos_entregas, ven_remitos_cabecera, es_horarios where es_datos_entregas.cod_horario = es_horarios.cod_horario and ven_remitos_cabecera.transaccion = es_datos_entregas.transaccion and ven_remitos_cabecera.letra = es_datos_entregas.letra and ven_remitos_cabecera.nrotransaccion = es_datos_entregas.nrotransaccion and  ven_remitos_cabecera.ptodeventa = es_datos_entregas.ptodeventa and  ven_remitos_cabecera.cliente = clientes.codigo and     es_datos_entregas.observaciones = 'obs'");
        var tam2 = data2.length;

        //console.log("compra: "+tam2);

        let now = new Date();

           // Normaliza a JID de WhatsApp tipo: 549XXXXXXXXXX@c.us
        function toChatId(rawPhone) {
          const raw = String(rawPhone ?? "");
          let digits = raw.replace(/\D/g, ""); // solo numeros
          if (!digits) return null;

          // Si ya viene con 549..., usarlo tal cual
          if (digits.startsWith("549")) return `${digits}@c.us`;

          // Si viene con 54... (sin 9), convertir a 549...
          if (digits.startsWith("54")) return `549${digits.slice(2)}@c.us`;

          // Caso comun: viene sin prefijo pais/9
         return `549${digits}@c.us`;
        }

        for (let i = 0; i <= tam2 - 1; i++) {

          const jid = toChatId(data2[i].telefono);
          if (!jid) {
            console.log("COMPRA: tel invalido ->", data2[i].telefono, data2[i].razon_social);
            io.emit('message', 'COMPRA: tel invalido -> ' + data2[i].razon_social);
            continue;
          }

          console.log('COMPRA: '+data2[i].codigo+' '+ data2[i].razon_social+' '+jid);
          io.emit('message',  'COMPRA: '+data2[i].codigo+' '+ data2[i].razon_social+' '+jid );

          await sleep(1000);

          // Validar que el número exista en WhatsApp antes de enviar
          const isReg = await client.isRegisteredUser(jid).catch(() => false);
          if (!isReg) {
            console.log("COMPRA: numero NO registrado en WhatsApp ->", jid, data2[i].razon_social);
            io.emit('message', 'COMPRA: numero NO registrado -> ' + data2[i].razon_social + ' ' + jid);
            continue;
          }

          try {
             await client.sendMessage(jid,'*👋 Hola '+data2[i].razon_social +'*\nGracias por su compra...\n🛒 Tu súper Online en Venado Tuerto\n\nwww.supermercadodigital.com.ar\n\n_Mensaje automático enviado por Asisto Bot_\n_https://www.instagram.com/asistobot/_', { sendSeen: false });
            await client.sendMessage('5493462674128@c.us','*COMPRA: '+data2[i].razon_social +'* \n '+data2[i].hora_desde+'\n'+'$ '+data2[i].total, { sendSeen: false });
            await client.sendMessage('5493462541989@c.us','*COMPRA: '+data2[i].razon_social +'* \n '+data2[i].hora_desde+'\n'+'$ '+data2[i].total, { sendSeen: false });
             const data3 = await connection.query("update es_datos_entregas set observaciones = '*'");
          } catch (e) {
            console.log("COMPRA: sendMessage ERROR ->", jid, e?.message || e);
            io.emit('message', 'COMPRA: sendMessage ERROR -> ' + data2[i].razon_social + ' ' + jid);
            // no cortamos el loop
          }
          //
            //console.log("TELEFONO BLOQUEADO: "+data2[i].telefono );
            ///io.emit('message',"TELEFONO BLOQUEADO: "+data2[i].telefono);
       // const data3 = await connection.query("update gen_mensajes set estado = 'B' where id='"+data[i].id+"'");
         // }
      /////////////////////////////////////////////////////////////////////////////
        }



      }
      //////////////////FUNCION ENVIAR MENSAJE ENTREGA PEDIDO//////////////////////////////////

      async function enviar_mensajes_entrega(){

        const data1 = await connection.query("select  codigo, clientes.razon_social, clientes.telefono, direccion_entrega, hora_desde, hora_hasta from clientes, es_datos_entregas, ven_remitos_cabecera, es_horarios where es_datos_entregas.cod_horario = es_horarios.cod_horario and ven_remitos_cabecera.transaccion = es_datos_entregas.transaccion and ven_remitos_cabecera.letra = es_datos_entregas.letra and ven_remitos_cabecera.nrotransaccion = es_datos_entregas.nrotransaccion and  ven_remitos_cabecera.ptodeventa = es_datos_entregas.ptodeventa and  ven_remitos_cabecera.cliente = clientes.codigo and     es_datos_entregas.observaciones = 'e'");
        var tam1 = data1.length;
        //console.log("entrega: "+tam1);

        if(tam1 == 0){
         //   console.log("Sin Registro")


        }

        for(j=0;j<=tam1 -1;j++){
          const jid = toChatId(data1[j].telefono);
          if (!jid) continue;

          console.log("telefono "+jid);
          var telefono_api = validarTelefono(jid.replace("@c.us",""));
          console.log("telefono_api "+telefono_api);
          //if(telefono_api == true){
              console.log('ENTREGA: '+data1[j].codigo+' '+ data1[j].razon_social+' '+jid);
            io.emit('message', 'ENTREGA: '+data1[j].codigo+' '+ data1[j].razon_social+' '+jid);
       await sleep(5000);
            const desde = data1[j].hora_desde;
            hora_d = desde.substr(10,6);

            const hasta = data1[j].hora_hasta;
            hora_h = hasta.substr(10,6);

            const isReg = await client.isRegisteredUser(jid).catch(() => false);
            if (!isReg) {
              console.log("ENTREGA: numero NO registrado ->", jid);
              continue;
            }

            await safeSendMessage('5493462674128@c.us','Mensaje Entrega enviado a: '+data1[j].razon_social+' '+hora_d+' a '+ hora_h+' en la direccion '+data1[j].direccion_entrega );
            await safeSendMessage(jid,'*👋 Hola '+data1[j].razon_social +'*\nTu pedido está en camino...\nserá entregado de '+ hora_d+' a '+ hora_h+' en la direccion '+data1[j].direccion_entrega +' \n🛒 Tu súper Online en Venado Tuerto\n\nwww.supermercadodigital.com.ar\n\n_Mensaje automático enviado por Asisto Bot_\n_https://www.instagram.com/asistobot/_');
          const data = await connection.query("update es_datos_entregas set observaciones = '*'");
         // }
         //   else
         // {
         //   console.log("TELEFONO BLOQUEADO: "+data1[i].telefono );
         //   io.emit('message',"TELEFONO BLOQUEADO: "+data1[i].telefono);

        //  }

        }


      }
      /////////////////////FUNCION ENVIAR MENSAJE MARKETING E INFO //////////////////////////////////

      async function enviar_mensajes_info(){

        const data = await connection.query("select first * from es_mensajes where estado <> 'S' and tipo = 'WS' and origen =" +telefono_qr +' order by prioridad asc');

        var tam = data.length;

        var today = new Date();

        var hora = today.toLocaleTimeString('en-US');
        //console.log(hora);

        for(i=0;i<tam;i++){

          const data_img = await connection.query("select first * from gen_imagenes where cod_imagen =" +data[i].cod_imagen);

         var tam_img = data_img.length;



            var segundos = Math.random() * (seg_hasta - seg_desde) + seg_desde;

            var arrayTelefono = data[i].destino;

            arrayTelefono = arrayTelefono.split(";");


            var tam2 = arrayTelefono.length;

            for(j=0;j<tam2;j++){

                // Normalizo y armo JID SIEMPRE (así no queda 'telefono' undefined en el else)
              const jid = toChatId(arrayTelefono[j]);
              if (!jid) continue;

              var telefono_api = validarTelefono(jid.replace("@c.us",""));
 
    

              if(telefono_api == true){

                

                //var msg_utf = new String(data[i].cuerpo, "ISO-8859-1");
                var msg_utf = new String(data[i].cuerpo, "UTF-8");
 console.log('MENSAJE: '+data[i].asunto+' '+ msg_utf+' '+jid);
               
 io.emit('message','MENSAJE: '+data[i].asunto+' '+  msg_utf+' '+jid+' '+segundos);
                

                if(tam_img > 0) {

                  console.log('img '+data_img[0].path);
                  var signatures = {
                    JVBERi0: "application/pdf",
                    R0lGODdh: "image/gif",
                    R0lGODlh: "image/gif",
                    iVBORw0KGgo: "image/png",
                    "/9j/": "image/jpg"
                  };

                  function detectMimeType(b64) {
                    for (var s in signatures) {
                      if (b64.indexOf(s) === 0) {
                      return signatures[s];
                    }
                  }
                }


                const fileData = fs.readFileSync(data_img[0].path, {
                  encoding: 'base64',
                })

                console.log('tipo de dato: '+detectMimeType(fileData));

                var media = await new MessageMedia(detectMimeType(fileData), fileData, data_img[0].nombre);

                const isReg = await client.isRegisteredUser(jid).catch(() => false);
                if (!isReg) {

                  console.log("numero no registrado");
                  const data6 = await connection.query("update es_mensajes set motivo_no_envio = 'numero no registrado' where id='"+data[i].id+"'");
                }
                else{
                  // Guardamos el vínculo wsMsgId -> id (DB) para que el ACK no dependa de una variable global.
                  const sent = await safeSendMessage(jid, media, { caption: msg_utf });
                  if (sent?.id?.id) pendingAck.set(sent.id.id, data[i].id);
                  // Fallback por compatibilidad (si el send no devuelve id)
                  if (!id_msg) id_msg = data[i].id;
                }
              // await client.sendMessage(telefono, msg_utf);
              // await sendMessage(media, {caption: 'this is my caption'};
              }
              else
              {
                const isReg2 = await client.isRegisteredUser(jid).catch(() => false);
                if (!isReg2) {

                  console.log("numero no registrado");
                  const data6 = await connection.query("update es_mensajes set motivo_no_envio = 'numero no registrado' where id='"+data[i].id+"'");
                }
                else{
                const sent2 = await safeSendMessage(jid, msg_utf);
                if (sent2?.id?.id) pendingAck.set(sent2.id.id, data[i].id);
                if (!id_msg) id_msg = data[i].id;
                }
              }
            }
            else
            {
              console.log("TELEFONO BLOQUEADO: "+arrayTelefono[j] );
              io.emit('message',"TELEFONO BLOQUEADO: "+arrayTelefono[j]);
            }

            const data3 = await connection.query("update es_mensajes set estado = 'S' where id='"+data[i].id+"'");

            let l_fecha = new Date();

            var numberOfMlSeconds = l_fecha.getTime();
            var addMlSeconds = 180 * 60000;

            l_fecha = new Date(numberOfMlSeconds - addMlSeconds);
            l_fecha = l_fecha.toISOString();

            l_fecha = l_fecha.replace('T',' ');
            l_fecha = l_fecha.replace('Z','');

            const data4 = await connection.query("update es_mensajes set fecha_envio = '"+l_fecha+"' where id='"+data[i].id+"'");
            // No pisar id_msg si todavía estamos esperando ACK de un envío anterior.
            if (!id_msg) id_msg = data[i].id;
            //console.log("serialized"+message.id._serialized);

            //const data5 = await connection.query("update es_mensajes set id_ws = '"+message.id._serialized+"' where id='"+data[i].id+"'");

            console.log("segundos espera. "+segundos)
            await sleep(segundos);
        }


      }






      }

  }






  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //           
  //
  //
  //
  //
  //    
  //
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  client.on('qr', (qr) => {
    console.log('QR RECEIVED', qr);
    lastQrRaw = qr;
    lastQrAt = nowArgentinaISO();
    updateLockStateSafe('qr').catch(() => {});
    qrcode.toDataURL(qr, (err, url) => {
      if (!err && url) {
        lastQrDataUrl = url;
        updateLockQrDataSafe(url, lastQrAt).catch(() => {});
      }
    });
  });

  client.on('ready', async () => {
    console.log("ready");
    updateLockStateSafe('online').catch(() => {});
    await ensureMongo();
    await cleanupOrphanGridFSChunks("wa_localauth");

    // Programar backups de sesión (LocalAuth -> Mongo)
    try { scheduleLocalAuthBackups(); } catch (e) { /* ignore */ }
    //socket.emit('ready', 'Whatsapp Listo!');
    //socket.emit('message', 'Whatsapp listo!');
   // RecuperarJsonConf();
   //if(bandera_query==0){
    queryAccess();//}
  });

  client.on('authenticated', () => {
    console.log('Autenticado');
    updateLockStateSafe('authenticated').catch(() => {});
    try { setTimeout(() => saveLocalAuthToMongo('authenticated+10s').catch(()=>{}), 10_000); } catch (e) { /* ignore */ }
  });

  client.on('auth_failure', function(session) {
    updateLockStateSafe('auth_failure').catch(() => {});
  });

  client.on('disconnected', function(reason) {
    updateLockStateSafe('disconnected').catch(() => {});
  });




}


app.get('/status', requireStatusToken, async (req, res) => {
  const lock = await getLockDocSafe();
  let waState = null;
  try { if (client) waState = await client.getState(); } catch {}
  return res.json({
    ok: true,
    now: nowArgentinaISO(),
    tenantId,
    numero,
    instanceId,
    lockId,
    isOwner,
    clientStarted,
    waState,
    telefono_qr,
    lock
  });
});

app.get('/status/lock', requireStatusToken, async (req, res) => {
  const lock = await getLockDocSafe();
  return res.json({ ok: true, lockId, lock });
});

app.get('/status/qr', requireStatusToken, async (req, res) => {
  return res.json({
    ok: true,
    now: nowArgentinaISO(),
    tenantId,
    numero,
    instanceId,
    lockId,
    isOwner,
    clientStarted,
    lastQrAt,
    lastQrRaw,
    lastQrDataUrl
  });
});

// Arranque controlado: bootstrap local + tenant_config + auto update + lock
(async function startSuperBot() {
  try {
    RecuperarJsonConf();

    try {
      const boot = readBootstrapFromFile();
      applyAutoUpdateConfig(boot);
    } catch (e) {
      try { EscribirLog('applyAutoUpdateConfig bootstrap error: ' + String(e?.message || e), 'error'); } catch {}
    }

    await loadTenantConfigFromDbMinimal();

    server.listen(port, function() {
      console.log('App running on *: ' + port);
    });

    startAutoUpdateScheduler();

    bootstrapWithLock().catch(e => {
      console.log('bootstrapWithLock error:', e?.message || e);
    });
  } catch (e) {
    console.log('FATAL bootstrap:', e?.message || e);
    try { EscribirLog('FATAL bootstrap: ' + String(e?.message || e), 'error'); } catch {}
    try {
      server.listen(port, function() {
        console.log('App running on *: ' + port);
      });
    } catch {}
  }
})();



async function gracefulShutdown(signal) {
  try { sessionLog(`[SHUTDOWN] ${signal} -> cerrando WhatsApp...`); } catch {}
  try { if (autoUpdateTimer) { clearInterval(autoUpdateTimer); autoUpdateTimer = null; } } catch {}
  try { if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; } } catch {}
  try { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } } catch {}
  try { if (actionTimer) { clearInterval(actionTimer); actionTimer = null; } } catch {}
  try { if (client) { try { await client.destroy(); } catch {} } } catch {}
  try { localWsPanelState = 'offline'; } catch {}
  try { await updateLockStateSafe('offline'); } catch {}
  try { isOwner = false; } catch {}
  process.exit(0);
}

process.on('SIGINT', () => { gracefulShutdown('SIGINT'); });
process.on('SIGTERM', () => { gracefulShutdown('SIGTERM'); });
process.on('SIGBREAK', () => { gracefulShutdown('SIGBREAK'); });


// Socket IO
io.on('connection', function(socket) {
  socket.emit('message', 'Conectando...');

 /* client.on('qr', (qr) => {
    console.log('QR RECEIVED', qr);
    qrcode.toDataURL(qr, (err, url) => {
      socket.emit('qr', url);
      socket.emit('message', 'Código QR Recibido...');
    });
  });*/

  /*client.on('ready', () => {
    socket.emit('ready', 'Whatsapp Listo!');
    socket.emit('message', 'Whatsapp listo!');
   // RecuperarJsonConf();
   //if(bandera_query==0){
  //  queryAccess();}
  });*/

 /* client.on('authenticated', () => {
    socket.emit('authenticated', 'Whatsapp Autenticado!');
    socket.emit('message', 'Whatsapp Autenticado!');
    console.log('Autenticado');
  });*/

  /*client.on('auth_failure', function(session) {
    socket.emit('message', 'Auth failure, restarting...');
  });*/

 /* client.on('disconnected', (reason) => {
    socket.emit('message', 'Whatsapp Desconectado!');
    client.destroy();
    client.initialize();
  });*/
});


const checkRegisteredNumber = async function(number) {
  const isRegistered = await client.isRegisteredUser(number);
  return isRegistered;
}



////////////////////funciones///////////////////////////////////////////



function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}


// Envío robusto: evita crash por bug sendSeen/markedUnread y no corta loops
async function safeSendMessage(to, content, options = {}) {
  try {
    const merged = { sendSeen: false, ...options };
    const sent = await client.sendMessage(to, content, merged);
    try {
      const logPayload = (content && typeof content === 'object')
        ? { body: merged.caption || '', type: content.mimetype ? 'media' : (content.type || 'text'), mimetype: content.mimetype || '', filename: content.filename || '', data: content.data ? '[data]' : '' }
        : { body: String(content || ''), type: 'text', hasMedia: false };
      await logMessageStat('out', to, logPayload);
    } catch {}
    return sent;
  } catch (e) {
    try { console.log("safeSendMessage ERROR ->", to, e?.message || e); } catch {}
    return null;
  }
}

// Normaliza a JID de WhatsApp tipo: 549XXXXXXXXXX@c.us (reusable)
function toChatId(rawPhone) {
  const raw = String(rawPhone ?? "");
  let digits = raw.replace(/\D/g, "");
  if (!digits) return null;
  if (digits.startsWith("549")) return `${digits}@c.us`;
  if (digits.startsWith("54")) return `549${digits.slice(2)}@c.us`;
  return `549${digits}@c.us`;
}




function RecuperarJsonConf(){

  const jsonConf = readBootstrapFromFile();
  console.log("configuracion.json " + JSON.stringify(jsonConf));

  applyTenantConfig(jsonConf);

  const jsonPackage = readVersionFromFile();
  if (jsonPackage && typeof jsonPackage === 'object') {
    version = jsonPackage.version || version;
    script = jsonPackage.script || script;
    telefono_qr = jsonPackage.telefono || telefono_qr;
  }

  if (!tenantId) tenantId = String(script || tenantId || 'super').trim().toUpperCase();
  if (!numero) numero = String(telefono_qr || numero || '').trim();
  if (!mongo_db) mongo_db = tenantId || mongo_db || 'super';

  try {
    console.log("puerto: " + port);
    console.log("headless: " + headless);
    console.log("seg_desde: " + seg_desde);
    console.log("seg_hasta: " + seg_hasta);
    console.log("dsn: " + dsn);
    console.log("seg_msg: " + seg_msg);
    console.log("api: " + api);
    console.log("msg_inicio: " + msg_inicio);
    console.log("msg_fin: " + msg_fin);
    console.log("tenantId: " + tenantId);
    console.log("numero: " + numero);
  } catch {}
}



function RecuperarJsonConfMensajes(){

  try {
    const jsonConf = readBootstrapFromFile();
    applyTenantConfig(jsonConf);
  } catch {}

  try {
    if (tenantConfig && typeof tenantConfig === 'object') {
      applyTenantConfig(tenantConfig);
    }
  } catch {}
}


function RecuperarTelefonos(){
  try {
  const jsonTel =  JSON.parse(fs.readFileSync('C:/Chatbot_pb/telefonos.json'));

  return jsonTel
} catch (error) {
  jsonTel = '[{"telefono":"999999999","nombre":"-","permitir":"N"}]';
  console.log("Sin bloqueo de telefonos...");
  return jsonTel
  
}


}

function validarTelefono( tel){
  var telefono = RecuperarTelefonos();

  var permitir = telefono[0].permitir;
  console.log("permitir "+permitir);
  
  if (permitir==undefined){
    return true
  }

  if(permitir=='N'){

    var tam = telefono.length;
    for(var i =0;i< tam;i++){
      console.log("busca en N "+'549'+telefono[i].telefono+' '+tel)
      if('549'+telefono[i].telefono == tel){
        console.log("existe en N "+'549'+telefono[i].telefono+' '+tel)
        return false

      }

    }
   return true

  }

  if(permitir=='S'){
    var tam = telefono.length;
    for(var i =0;i< tam;i++){
      console.log("busca en S "+'549'+telefono[i].telefono+' '+tel)
      if('549'+telefono[i].telefono == tel){
        console.log("existe en S "+'549'+telefono[i].telefono+' '+tel)
        return true

    }

  }
  return false
}
}


