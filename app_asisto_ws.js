/*script:app_asisto*/
/*version:1.06.02   11/07/2025*/

// =========================
// FIXES 2026-01:
// - Evitar takeover por state=offline si NO está stale (solo por lease_ms)
// - Evitar que el prune borre el backup mientras otra PC lo está restaurando (pin)
// - Backup seguro: upload temp -> rename -> prune
// =========================


//const chatbot = require("./funciones_asisto.js")
const { Client, MessageMedia, LocalAuth, RemoteAuth } = require('whatsapp-web.js');
const mongoose = require('mongoose');
const { MongoStore } = require('wwebjs-mongo');
const os = require('os');
const crypto = require('crypto');
const express = require('express');
const { body, validationResult } = require('express-validator');
const socketIO = require('socket.io');
const qrcode = require('qrcode');
const http = require('http');
//var odbc = require("odbc");
const fetch = require('node-fetch');
const fileUpload = require('express-fileupload');
const axios = require('axios');
const mime = require('mime-types');
const { ClientInfo } = require('whatsapp-web.js/src/structures');
const utf8 = require('utf8');
//const { OdbcError } = require('odbc');
const nodemailer = require('nodemailer');
const { eventNames } = require('process');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
let mongoConnectingPromise = null;

// Momento en el que ESTA instancia tomó el lock (para ignorar acciones viejas en wa_wweb_actions)
let lockAcquiredAt = null;
// --- LocalAuth backup to Mongo (GridFS) ---
let AdmZip = null;
try { AdmZip = require('adm-zip'); } catch (e) { /* optional until you install: npm i adm-zip */ }
let GridFSBucket = null;
try { ({ GridFSBucket } = require('mongodb')); }
catch (e) {
  // Fallback: mongoose ya trae el driver de mongodb
  try { GridFSBucket = mongoose?.mongo?.GridFSBucket; } catch {}
}

// Mientras una PC RESTAURA, "pin" para que el prune no borre ese fileId.
// Debe ser > timeout de restore.
const LOCALAUTH_RESTORE_PIN_MS = Number(process.env.LOCALAUTH_RESTORE_PIN_MS || 5 * 60_000);
// Si querés logs más verbosos de restore/backup
const LOCALAUTH_VERBOSE = String(process.env.LOCALAUTH_VERBOSE || "0").trim() === "1";

 // --- LocalAuth: anti-crecimiento en GridFS ---
 // Evita backups concurrentes (duplican files/chunks)
 let savingLocalAuth = false;
 // Evita multiplicar timers/intervals si 'ready' se dispara más de una vez (reconnect)
 let localAuthBackupSchedulerStarted = false;
 let localAuthBackupInterval = null;

  // Evita que en recreaciones (Target closed / execution_context) se intente RESTORE otra vez
  // y termine tocando la carpeta local innecesariamente.
  let localAuthRestoreAttempted = false;
  // Evita manejar múltiples auth_failure en cascada
  let authFailureHandling = false;
const AR_TZ = 'America/Argentina/Cordoba';



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

// =========================
// Config por tenant (MongoDB)
// configuracion.json: SOLO { tenantId, mongo_uri, mongo_db }
// El resto (puerto, numero, seg_desde, etc.) viene de la colección tenant_config.
// =========================
let tenantConfig = null; // config cargada desde Mongo

function readBootstrapFromFile() {
  try {
    const p = path.join(process.cwd(), "configuracion.json");
    if (!fs.existsSync(p)) return {};
    const raw = JSON.parse(fs.readFileSync(p, "utf8"));
    const obj = (raw && raw.configuracion && typeof raw.configuracion === "object") ? raw.configuracion : raw;
    return obj && typeof obj === "object" ? obj : {};
  } catch {
    return {};
  }
}

function applyTenantConfig(conf) {
  if (!conf || typeof conf !== "object") return;

  // Core
  if (conf.puerto !== undefined && conf.puerto !== null && conf.puerto !== "") {
    port = Number(conf.puerto) || port;
  }
  if (conf.headless !== undefined) {
    headless = conf.headless;
    if (typeof headless === "string") headless = headless.toLowerCase().trim() === "true";
    else headless = !!headless;
  }
  if (!numero && (conf.numero || conf.NUMERO)) numero = String(conf.numero || conf.NUMERO).trim();
  if (conf.status_token !== undefined) status_token = String(conf.status_token || status_token || "").trim();

  // Lock/lease
  if (conf.lease_ms !== undefined) lease_ms = Number(conf.lease_ms) || lease_ms;
  if (conf.heartbeat_ms !== undefined) heartbeat_ms = Number(conf.heartbeat_ms) || heartbeat_ms;
  if (conf.backup_every_ms !== undefined) backup_every_ms = Number(conf.backup_every_ms) || backup_every_ms;
  if (conf.auth_base_path !== undefined || conf.auth_path !== undefined) {
    auth_base_path = String(conf.auth_base_path || conf.auth_path || auth_base_path || "").trim();
  }
  // En Windows el backup (zip) puede bloquear el event loop varios segundos.
  // Si lease_ms es muy bajo, otra PC toma el lock aunque esta siga viva.
  if (!Number.isFinite(lease_ms) || lease_ms < MIN_LEASE_MS) lease_ms = MIN_LEASE_MS;


  if (conf.auth_mode !== undefined && conf.auth_mode !== null && String(conf.auth_mode).trim() !== '') {
    auth_mode = String(conf.auth_mode).trim().toLowerCase();
  }

  // Mensajes / límites
  if (conf.seg_desde !== undefined) seg_desde = conf.seg_desde;
  if (conf.seg_hasta !== undefined) seg_hasta = conf.seg_hasta;
  if (conf.dsn !== undefined) dsn = conf.dsn;
  if (conf.seg_msg !== undefined) seg_msg = conf.seg_msg;
  if (conf.seg_tele !== undefined) seg_tele = conf.seg_tele;
  if (conf.api !== undefined) api = conf.api;
  if (conf.msg_inicio !== undefined) msg_inicio = conf.msg_inicio;
  if (conf.msg_fin !== undefined) msg_fin = conf.msg_fin;
  if (conf.cant_lim !== undefined) cant_lim = conf.cant_lim;
  if (conf.msg_lim !== undefined) msg_lim = conf.msg_lim;
  if (conf.time_cad !== undefined) time_cad = conf.time_cad;
  if (conf.msg_cad !== undefined) msg_cad = conf.msg_cad;
  if (conf.msg_can !== undefined) msg_can = conf.msg_can;
  if (conf.nom_emp !== undefined) nom_chatbot = conf.nom_emp;
  if (conf.nom_chatbot !== undefined) nom_chatbot = conf.nom_chatbot;

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

  if (!tenantId || !mongo_uri) throw new Error("Falta tenantId/mongo_uri en configuracion.json");

  const ok = await ensureMongo();
  if (!ok || !mongoose?.connection?.db) throw new Error("No se pudo conectar a Mongo para cargar configuración");

  const collName = String(process.env.ASISTO_CONFIG_COLLECTION || "tenant_config").trim() || "tenant_config";
  const coll = mongoose.connection.db.collection(collName);

  let doc = await coll.findOne({ _id: tenantId });
  if (!doc) doc = await coll.findOne({ tenantId: tenantId });
  if (!doc) throw new Error(`No existe configuración en BD para tenantId=${tenantId} (${collName})`);

  const conf = (doc && doc.configuracion && typeof doc.configuracion === "object") ? doc.configuracion : doc;
  tenantConfig = conf;
  applyTenantConfig(conf);

  try {
    console.log(`[CONFIG] tenantId=${tenantId} numero=${numero || ""} puerto=${port} headless=${headless} seg_desde=${seg_desde}`);
  } catch {}
  return true;
}

function localAuthDirHasKeyData(sessionDir) {
  try {
    if (!sessionDir || !fs.existsSync(sessionDir)) return false;
    // ⚠️ Importante: en WhatsApp Web (Chromium profile), que exista la carpeta no alcanza.
    // En reinicios, si faltan "Service Worker" / "CacheStorage" suele volver a pedir QR.
    // Entonces hacemos un probe más estricto, pero SIN leer contenido sensible.

    const probes = [
      // nivel root
      { p: path.join(sessionDir, "Local State"), kind: "file", minBytes: 200 },
      // nivel Default
      { p: path.join(sessionDir, "Default", "IndexedDB"), kind: "dir", minItems: 1 },
      { p: path.join(sessionDir, "Default", "Local Storage", "leveldb"), kind: "dir", minItems: 1 },
      { p: path.join(sessionDir, "Default", "Cookies"), kind: "file", minBytes: 1 },
     { p: path.join(sessionDir, "Default", "Service Worker"), kind: "dir", minItems: 1 },
      { p: path.join(sessionDir, "Default", "CacheStorage"), kind: "dir", minItems: 1 },
      { p: path.join(sessionDir, "Default", "Network"), kind: "dir", minItems: 1 },
      { p: path.join(sessionDir, "Default", "Preferences"), kind: "file", minBytes: 50 },
    ];

    let okCount = 0;
    for (const pr of probes) {
     try {
        if (!fs.existsSync(pr.p)) continue;
        const st = fs.statSync(pr.p);
        if (pr.kind === "file") {
          if (st.isFile() && st.size >= (pr.minBytes || 1)) okCount++;
        } else {
          if (st.isDirectory()) {
            const items = fs.readdirSync(pr.p);
            if ((items?.length || 0) >= (pr.minItems || 1)) okCount++;
          }
        }
      } catch {}
   }

    // Regla práctica: si tenemos al menos 4 señales fuertes, asumimos que la sesión es restaurable.
    if (okCount >= 4) return true;

    // Fallback ultra-permisivo: carpeta grande con estructura mínima.
    // (evita falsos negativos en OS que no guardan CacheStorage, etc.)
    try {
     const items = fs.readdirSync(sessionDir).filter(x => x && !String(x).startsWith("."));
      if (items.length < 3) return false;
      // Si la carpeta pesa < 20MB, suele ser perfil "incompleto" recién creado.
      const approxSize = (dirPath) => {
        let sum = 0;
        try {
          const stack = [dirPath];
          while (stack.length) {
            const cur = stack.pop();
            let lst = [];
            try { lst = fs.readdirSync(cur); } catch { continue; }
            for (const name of lst) {
              const abs = path.join(cur, name);
              let st2;
              try { st2 = fs.statSync(abs); } catch { continue; }
              if (st2.isDirectory()) stack.push(abs);
              else sum += (st2.size || 0);
              if (sum > 25 * 1024 * 1024) return sum; // corte temprano
            }
          }
        } catch {}
        return sum;
      };
      const sz = approxSize(sessionDir);
      return sz >= 20 * 1024 * 1024;
    } catch {
      return false;
    }
  } catch {
    return false;
  }
}

function sessionLog(msg) {
  try { console.log(msg); } catch {}
  try { EscribirLog(msg, "event"); } catch {}
}

const RESTORE_TIMEOUT_MS = Number(process.env.RESTORE_TIMEOUT_MS || 240_000);
const RESTORE_LOG_EVERY_BYTES = Number(process.env.RESTORE_LOG_EVERY_BYTES || (2 * 1024 * 1024)); // 2MB
const RESTORE_RETRY_ON_TIMEOUT = Number(process.env.RESTORE_RETRY_ON_TIMEOUT || 1); // 1 reintento

// Lease/heartbeat configurables (ms)
const MIN_LEASE_MS = Number(process.env.MIN_LEASE_MS || 180000);
let lease_ms = Number(process.env.LEASE_MS || MIN_LEASE_MS);
let heartbeat_ms = Number(process.env.HEARTBEAT_MS || 5000);
let backup_every_ms = Number(process.env.BACKUP_EVERY_MS || 300000); // LocalAuth backup period
let auth_base_path = process.env.ASISTO_AUTH_PATH || "";            // LocalAuth dataPath override
let auth_mode = String(process.env.ASISTO_AUTH_MODE || '').trim().toLowerCase(); // 'remote' | 'local' (default: local)

// =========================
// Auto-update desde repositorio (opcional, NO rompe comportamiento actual)
// Requiere que la carpeta local sea un checkout git y que exista 'git' en la PC.
// Por seguridad, viene DESACTIVADO por defecto y solo se habilita por config/env.
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

// opcional para proteger /status

function isRemoteAuthMode() {
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

    child.stdout && child.stdout.on('data', (d) => { stdout += d.toString(); });
    child.stderr && child.stderr.on('data', (d) => { stderr += d.toString(); });
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
  const out = await runCommand('git', ['rev-parse', '--abbrev-ref', 'HEAD'], { cwd: repoPath, timeout: 20_000 });
  return String(out.stdout || '').trim() || 'main';
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

    const branch = await autoUpdateGetBranch(repoPath);
    const remote = auto_update_remote || 'origin';

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

    await runCommand('git', ['fetch', remote, branch, '--prune'], { cwd: repoPath, timeout: 120_000 });

    const remoteRef = `${remote}/${branch}`;
    const remoteHeadOut = await runCommand('git', ['rev-parse', remoteRef], { cwd: repoPath, timeout: 15_000 });
    const remoteHead = String(remoteHeadOut.stdout || '').trim();
    if (!remoteHead) throw new Error('git_remote_head_empty');

    if (remoteHead === localHead) {
      autoUpdateLog(`[AUTO_UPDATE] ok (${reason}): sin cambios (${localHead.slice(0, 7)})`, 'event');
      return;
    }

    autoUpdateLog(`[AUTO_UPDATE] update (${reason}): ${localHead.slice(0, 7)} -> ${remoteHead.slice(0, 7)}`, 'event');

    const changedOut = await runCommand('git', ['diff', '--name-only', `${localHead}..${remoteRef}`], { cwd: repoPath, timeout: 30_000 });
    const changedFiles = String(changedOut.stdout || '').split(/\r?\n/).map(s => s.trim()).filter(Boolean);

    await runCommand('git', ['reset', '--hard', remoteRef], { cwd: repoPath, timeout: 120_000 });

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
// Cache liviano: si la política marca disabled=true, no inicializamos WhatsApp.
let lastPolicyDisabled = null;
let mongoReady = false;
let LockModel = null;
let ActionModel = null;
let PolicyModel = null;      // wa_wweb_policies
let HistoryModel = null;     // wa_wweb_history
let heartbeatTimer = null;
let pollTimer = null;
var a = 0;
//var port = 8002
var headless = true;
var seg_desde = 80000;
var seg_hasta = 10000;
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
//var api2 = "http://managermsm.ddns.net:2002/v200/api/Api_Mensajes/Consulta?key=1";
var api2 = "http://managermsm.ddns.net:2002/v200/api/Api_Mensajes/Consulta_no_enviados"//key={{key}}&nro_tel_from={{nro_tel_from}}";
var api3 = "http://managermsm.ddns.net:2002/v200/api/Api_Mensajes/Actualiza_mensaje"//?key={{key}}&nro_tel_from={{nro_tel_from}}"//key={{key}}&nro_tel_from={{nro_tel_from}}";
var key = 'FMM0325*';
var msg_inicio = "";
var msg_fin = "";
var cant_lim = 0;
var msg_lim = 'Continuar? S / N';
var time_cad = 0;
var email_err = "";
var msg_cad = "";
var msg_can = "";
var bandera_msg = 1;
var jsonGlobal = [];   //1-json, 2 -i , 3-tel, 4-hora
var json;
var i_global = 0;
var msg_body;
var smtp;
var email_usuario;
var email_pas;
var email_puerto;
var email_saliente;
var msg_errores;
var nom_chatbot;

var Id_msj_dest ;
var Id_msj_renglon;

var signatures = {
  JVBERi0: "application/pdf",
  R0lGODdh: "image/gif",
  R0lGODlh: "image/gif",
  iVBORw0KGgo: "image/png",
  "/9j/": "image/jpg"
};



const logFilePath_event = path.join(__dirname, 'app_asisto_event.log');
const logFilePath_error = path.join(__dirname, 'app_asisto_error.log');

EscribirLog("inicio Script","event");


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

async function cleanupOrphanGridFSChunks(bucketName = "wa_localauth") {
  await ensureMongo();

  const db = mongoose.connection.db; // ✅ acá está la DB real
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

function requireStatusToken(req, res, next) {
  if (!status_token) return next();
  const t = String(req.query?.token || req.headers["x-status-token"] || "");
  if (t && t === String(status_token)) return next();
  return res.status(401).json({ ok: false, error: "unauthorized" });
}

async function ensureMongo() {
  try {
    // Ya conectado
    if (mongoReady && mongoose?.connection?.readyState === 1 && mongoose?.connection?.db) {
      // asegurar modelos
      initMongoModelsIfNeeded();
      return true;
    }

    // Promise global para serializar conexión (evita ReferenceError aunque falte una variable global)
    if (globalThis.__asistoMongoConnectingPromise) {
      const ok = await globalThis.__asistoMongoConnectingPromise;
      if (ok) initMongoModelsIfNeeded();
      return ok;
    }

    if (!mongo_uri) return false;

    globalThis.__asistoMongoConnectingPromise = (async () => {
      try {
        await mongoose.connect(mongo_uri, {
          dbName: (mongo_db || tenantId || "asisto"),
          autoIndex: true,
          serverSelectionTimeoutMS: 15000
        });

        // Asegurar que db exista (algunas veces tarda un tick)
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
          console.log(`Mongo conectado. dbName=${dbName} host=${host}`);
          EscribirLog(`Mongo conectado. dbName=${dbName} host=${host}`, "event");
        } catch {}

        initMongoModelsIfNeeded();
        return true;
      } catch (e) {
        try { console.log("Mongo connect error:", e?.message || e); } catch {}
        try { EscribirLog("Mongo connect error: " + String(e?.message || e), "error"); } catch {}
        try { await mongoose.disconnect(); } catch {}
        mongoReady = false;
        return false;
      } finally {
        globalThis.__asistoMongoConnectingPromise = null;
      }
    })();

    const ok = await globalThis.__asistoMongoConnectingPromise;
    if (ok) initMongoModelsIfNeeded();
    return ok;
  } catch (e) {
    try { console.log("ensureMongo error:", e?.message || e); } catch {}
    try { EscribirLog("ensureMongo error: " + String(e?.message || e), "error"); } catch {}
    mongoReady = false;
    globalThis.__asistoMongoConnectingPromise = null;
    return false;
  }
}

// Inicializa modelos una sola vez (lock/policies/history/actions)
function initMongoModelsIfNeeded() {
  try {
    if (!mongoose?.connection?.db) return;

    if (!PolicyModel) {
      const PolicySchema = new mongoose.Schema(
        {
          _id: { type: String },
          // Compat: algunos deployments antiguos guardaban tenantid.
          tenantid: { type: String },
          // Canonical: lo usamos para filtrar desde el panel.
          tenantId: { type: String, index: true },
          numero: { type: String, index: true },
          // Nuevo: si disabled=true, el script no inicializa WhatsApp (queda "habilitado/bloqueado" desde el panel)
          disabled: { type: Boolean, default: false },
          mode: { type: String, default: "any" },          // any | pinned
          pinnedHost: { type: String, default: "" },       // hostname permitido (si mode=pinned)
          blockedHosts: { type: [String], default: [] },   // hostnames bloqueados
          updatedAt: { type: Date },
          updatedBy: { type: String }
        },
        { collection: "wa_wweb_policies" }
      );
      PolicyModel = mongoose.models.WaWwebPolicy || mongoose.model("WaWwebPolicy", PolicySchema);
    }

    if (!HistoryModel) {
      const HistorySchema = new mongoose.Schema(
        {
          lockId: { type: String, index: true },
          event: { type: String, index: true },            // startup|standby|lock_acquired|qr|ready|policy_blocked|policy_pinned|release|...
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
          action: { type: String, index: true },           // release | restart | logout
          reason: { type: String },
          requestedBy: { type: String },
          requestedAt: { type: Date, default: Date.now, index: true },
          consumedAt: { type: Date }
        },
        { collection: "wa_wweb_actions" }
      );
      ActionModel = mongoose.models.WaWwebAction || mongoose.model("WaWwebAction", ActionSchema);
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
    // Necesitamos bootstrap mínimo antes
    if (!tenantId || !mongo_uri) return null;

    const ok = await ensureMongo();
    if (!ok || !mongoose?.connection?.db) return null;

    const collName = String(process.env.ASISTO_CONFIG_COLLECTION || "tenant_config").trim() || "tenant_config";
    const coll = mongoose.connection.db.collection(collName);

    // Soporta doc con _id=tenantId o con campo tenantId
    let doc = await coll.findOne({ _id: tenantId });
    if (!doc) doc = await coll.findOne({ tenantId: tenantId });
    if (!doc) {
      try { console.log(`[CONFIG] No existe config en BD para tenantId=${tenantId} (colección ${collName})`); } catch {}
      return null;
    }

    const conf = (doc && doc.configuracion && typeof doc.configuracion === "object") ? doc.configuracion : doc;

    // Aplicar SOLO si vienen valores definidos (no pisar con vacíos)
    if (!numero && conf.numero) numero = String(conf.numero).trim();

    if (conf.puerto !== undefined && conf.puerto !== null && conf.puerto !== "") {
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


    if (!status_token && conf.status_token) status_token = String(conf.status_token).trim();

    applyAutoUpdateConfig(conf);

    try { console.log(`[CONFIG] tenantId=${tenantId} numero=${numero} puerto=${port} headless=${headless} auth_mode=${auth_mode || 'local'} lease_ms=${lease_ms} heartbeat_ms=${heartbeat_ms}`); } catch {}
    return conf;
  } catch (e) {
    try { console.log("loadTenantConfigFromDbMinimal error:", e?.message || e); } catch {}
    try { EscribirLog("loadTenantConfigFromDbMinimal error: " + String(e?.message || e), "error"); } catch {}
    return null;
  }
}



async function pushHistory(event, detail) {
  try {
    if (!HistoryModel) return;
    if (!lockId) return;
    await HistoryModel.create({
      lockId,
      event: String(event || ""),
      host: os.hostname(),
      pid: process.pid,
      detail: detail || null,
      at: new Date()
    });
  } catch {}
}

async function getPolicySafe() {
  try {
    if (!PolicyModel) return null;
    // El panel guarda políticas por {tenantId, numero}. Mantener fallback por _id por compat.
    if (tenantId && numero) {
      const p = await PolicyModel.findOne({ tenantId: String(tenantId), numero: String(numero) }).lean();
      if (p) return p;
    }
    if (lockId) {
      const p2 = await PolicyModel.findById(lockId).lean();
      if (p2) return p2;
    }
    return null;
  } catch {
    return null;
  }
}

function hostName() {
  return os.hostname();
}

async function getLockDocSafe() {
  try {
    if (!await ensureMongo()) return null;
    if (!lockId) return null;
    return await LockModel.findById(lockId).lean();
  } catch {
    return null;
  }
}

app.get("/status", requireStatusToken, async (req, res) => {
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

app.get("/status/lock", requireStatusToken, async (req, res) => {
  const lock = await getLockDocSafe();
  return res.json({ ok: true, lockId, lock });
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
    lastQrAt,
    lastQrRaw,
    lastQrDataUrl
  });
});

app.post("/control/release", requireStatusToken, async (req, res) => {
  // Libera el lock y apaga el cliente en esta PC (standby manual).
  try {
    if (!isOwner) return res.status(409).json({ ok: false, error: "not_owner" });

    // best-effort: apagar WA
    try { if (clientStarted) await client.destroy(); } catch {}
    clientStarted = false;

    await forceReleaseLock();
    isOwner = false;

    return res.json({ ok: true, released: true });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});



    
(async function startAsistoWs() {
  // Bootstrap: configuracion.json (tenantId/mongo_uri/mongo_db) + tenant_config (resto)
  try {
    RecuperarJsonConf();

    // Cargar resto de configuración desde Mongo (numero/puerto/headless/etc.)
    await loadTenantConfigFromDbMinimal();

    server.listen(port, function() {
      console.log('App running on *: ' + port);
      EscribirLog('App running on *: ' + port,"event");
    });

    startAutoUpdateScheduler();

    bootstrapWithLock().catch(e => {
      console.log('bootstrapWithLock error:', e?.message || e);
      EscribirLog('bootstrapWithLock error: ' + String(e?.message || e), 'error');
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

// =========================
// LocalAuth <-> Mongo (GridFS) helpers
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

function dirLooksPopulated(p) {
  try {
    if (!fs.existsSync(p)) return false;
    const items = fs.readdirSync(p);
 return Array.isArray(items) && items.length > 0;
  } catch {
    return false;
  }
}



function zipDirectoryToBuffer(dirPath) {
  if (!AdmZip) {
    const msg = "Falta dependencia adm-zip. Instalá: npm i adm-zip";
    try { console.log(msg); } catch {}
    throw new Error("missing_dependency_adm_zip");
  }
  const zip = new AdmZip();

  // Backup MINIMAL (para que no sea gigante y para evitar EBUSY en Windows):
  // Incluye solo lo necesario para que LocalAuth restaure la sesión.
  // Todo lo de cache se excluye.
  // ⚠️ En la práctica, WhatsApp Web/Chromium suele necesitar más que IndexedDB/LocalStorage/Cookies
  // para sobrevivir reinicios sin pedir QR (Service Worker / CacheStorage / Session Storage, etc.).
  const INCLUDE_MIN = [
    "Local State",
    "Default/Local Storage",
    "Default/IndexedDB",
    "Default/Cookies",
    "Default/Cookies-journal",
    "Default/Preferences",
    "Default/Secure Preferences",
    // Recomendados para persistencia estable:
    "Default/Service Worker",
    "Default/CacheStorage",
    "Default/Session Storage",
    "Default/Web Storage",
    "Default/Code Cache",
    "Default/Network",
    "Default/TransportSecurity"
  ];

  const INCLUDE_FULL = [
    ...INCLUDE_MIN,
    "Default/Web Storage",
    "Default/Session Storage",
    "Default/Network",
     "Default/TransportSecurity"
   ];
 const INCLUDE = (String(process.env.ASISTO_AUTH_BACKUP_FULL || "").trim() === "1") ? INCLUDE_FULL : INCLUDE_MIN;
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
  if (!AdmZip) {
    const msg = "Falta dependencia adm-zip. Instalá: npm i adm-zip";
    try { console.log(msg); } catch {}
    throw new Error("missing_dependency_adm_zip");
  }
  fs.mkdirSync(destDir, { recursive: true });
  const zip = new AdmZip(buf);
  zip.extractAllTo(destDir, true);
}

function getBucket() {
  if (!mongoose?.connection?.db) throw new Error("mongo_db_not_ready");
  if (!GridFSBucket) throw new Error("mongodb_gridfsbucket_not_available");
  // bucket name: wa_localauth (creates wa_localauth.files / wa_localauth.chunks)
  return new GridFSBucket(mongoose.connection.db, { bucketName: "wa_localauth" });
}




function isGridfsFileNotFoundError(err) {
   
  if (!err) return false;
  const msg = String(err.message || err).toLowerCase();
  // mongodb driver suele tirar: "File not found for id <id>"
  if (msg.includes('file not found for id')) return true;
  // algunas variantes
  if (msg.includes('filenotfound')) return true;
  if (String(err.code || '').toLowerCase() == 'filenotfound') return true;
  return false;
}


function bucketDeleteAsync(bucket, fileId) {
  return new Promise((resolve, reject) => {
     // Compatibilidad: mongodb driver v3/v4 (callback) y v5/v6 (Promise)
    try {
      const maybePromise = bucket.delete(fileId);
      if (maybePromise && typeof maybePromise.then === 'function') {
        maybePromise.then(resolve).catch(reject);
        return;
      }
      // fallback callback-style
      bucket.delete(fileId, (err) => {
        if (err) return reject(err);
        resolve();
      });
    } catch (e) {
      // si falló el modo Promise, intentamos callback-style
      try {
        bucket.delete(fileId, (err) => {
          if (err) return reject(err);
          resolve();
        });
      } catch (e2) {
        reject(e2);
      }
    }
  }).catch((err) => {
    // Si el file ya no existe (race), no queremos tirar abajo el proceso.
    if (isGridfsFileNotFoundError(err)) return;
    throw err;
  });
}

async function pinGridFsFile(bucket, fileId, ms) {
  try {
    const until = new Date(Date.now() + (Number(ms) || LOCALAUTH_RESTORE_PIN_MS));
    await bucket.s.db.collection(`${bucket.s.options.bucketName}.files`).updateOne(
      { _id: fileId },
      { $set: { "metadata.pinnedUntil": until } }
    );
    if (LOCALAUTH_VERBOSE) console.log(`LocalAuth: PIN set fileId=${fileId} pinnedUntil=${until.toISOString()}`);
  } catch (e) {
    // no romper restore si falla el pin
    if (LOCALAUTH_VERBOSE) console.log(`LocalAuth: PIN warn fileId=${fileId} err=${e?.message || e}`);
  }
}

async function unpinGridFsFile(bucket, fileId) {
  try {
    await bucket.s.db.collection(`${bucket.s.options.bucketName}.files`).updateOne(
      { _id: fileId },
      { $unset: { "metadata.pinnedUntil": "" } }
    );
    if (LOCALAUTH_VERBOSE) console.log(`LocalAuth: PIN cleared fileId=${fileId}`);
  } catch {}
}

async function deleteTempBackups(bucket, canonicalFilename) {
  // borra residuos de uploads temporales (si quedaron por cortes)
  try {
    const filesColl = bucket.s.db.collection(`${bucket.s.options.bucketName}.files`);
    // canonicalFilename puede tener caracteres especiales -> escapar para regex
    const rx = `^${escapeRegex(String(canonicalFilename))}\\.tmp-`;
    const cursor = filesColl.find({ filename: { $regex: rx } }, { projection: { _id: 1 } });
    const ids = [];
    await cursor.forEach(d => ids.push(d._id));
    for (const id of ids) {
      // bucket.delete() no siempre es Promise; usar wrapper seguro
      try { await bucketDeleteAsync(bucket, id); } catch {}

    }
    if (ids.length && LOCALAUTH_VERBOSE) console.log(`LocalAuth: TEMP cleanup deleted=${ids.length}`);
  } catch {}
}



async function deleteExistingFilename(bucket, filename) {
   // OJO: GridFSBucket.delete() usa callback (no Promise) en varias versiones del driver.
  // Si hacemos `await bucket.delete(...)` NO borra y la colección wa_localauth.chunks crece sin límite.

  const existing = await bucket.find({ filename }).toArray();
  for (const f of (existing || [])) {
     try {
      await bucketDeleteAsync(bucket, f._id);
    } catch (e) {
      if (!isGridfsFileNotFoundError(e)) {
        try { console.warn('GridFS deleteExistingFilename error:', e?.message || e); } catch {}
      }
    }
  }
}


async function deleteByFilenamePrefix(bucketName, filenamePrefix) {
  await ensureMongo();
  const db = mongoose.connection.db;
  if (!db) return;
  const filesColl = db.collection(`${bucketName}.files`);
  const rows = await filesColl.find({ filename: { $regex: `^${escapeRegex(filenamePrefix)}` } }, { projection: { _id: 1, filename: 1 } }).toArray();
  if (!rows || !rows.length) return;
  const bucket = new GridFSBucket(db, { bucketName });
  for (const r of rows) {
    try { await bucketDeleteAsync(bucket, r._id); } catch {}
  }
}

function escapeRegex(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function pruneOldLocalAuthFiles(bucket, filename, keep ) {
  try {
    const keepN = Math.max(1, Number(keep) || 1);
    const now = new Date();
    const files = await bucket.find({ filename }).sort({ uploadDate: -1 }).toArray();
    // ⚠️ NO borrar archivos "pinned" (otra PC puede estar restaurando)
   const safe = [];
    for (const f of files) {
      const pu = f?.metadata?.pinnedUntil ? new Date(f.metadata.pinnedUntil) : null;
      const isPinned = pu && pu > now;
      if (isPinned) {
        if (LOCALAUTH_VERBOSE) console.log(`LocalAuth: PRUNE skip pinned fileId=${f._id} pinnedUntil=${pu.toISOString()}`);
        continue;
      }
      safe.push(f);
    }

    // Mantener el más nuevo (keep=1) entre los NO-pinned
    const toDelete = safe.slice(keepN);
    for (const f of toDelete) {
      // bucket.delete() no siempre es Promise; usar wrapper seguro
      try { await bucketDeleteAsync(bucket, f._id); } catch {}

    }
    if (toDelete.length && LOCALAUTH_VERBOSE) console.log(`LocalAuth: PRUNE deleted=${toDelete.length} keep=${keepN}`);
  } catch {}
}


async function restoreRemoteLocalAuthToDisk(clientId, sessionDir, timeoutMs = 90_000) {
  const filename = `LocalAuth-${clientId}.zip`;
  const bucket = getBucket();
  const filesColl = bucket.s.db.collection(`${bucket.s.options.bucketName}.files`);

  // Buscar el más nuevo
  const latest = await filesColl.find({ filename }).sort({ uploadDate: -1 }).limit(1).next();
  if (!latest) {
    console.log(`LocalAuth: RESTORE skip (no remote) filename=${filename}`);
    return false;
  }

  console.log(`LocalAuth: RESTORE start filename=${filename} fileId=${latest._id} uploadDate=${latest.uploadDate} timeoutMs=${timeoutMs}`);

  // PIN para evitar que el prune de otra PC lo borre mientras se descarga
  await pinGridFsFile(bucket, latest._id, LOCALAUTH_RESTORE_PIN_MS);

  try {
    const buf = await downloadGridFsFileToBuffer(bucket, latest._id, timeoutMs);
    extractZipBufferToDir(buf, sessionDir);
    console.log(`LocalAuth: RESTORE OK fileId=${latest._id} bytes=${buf.length}`);
    return true;
  } finally {
    // liberamos pin (igual el prune ya respeta pinnedUntil)
    await unpinGridFsFile(bucket, latest._id);
  }
}



async function promoteTempToCanonical(bucketName, tempFileId, canonicalFilename) {
  await ensureMongo();
  const db = mongoose.connection.db;
  if (!db) throw new Error("mongo_db_not_ready");
  const filesColl = db.collection(`${bucketName}.files`);
  // Renombramos el staging al nombre canónico (NO mueve chunks, solo actualiza metadata)
  await filesColl.updateOne({ _id: tempFileId }, { $set: { filename: canonicalFilename } });
}


async function saveLocalAuthToMongo(label = "") {
  if (!tenantId || !numero) return;
  if (!await ensureMongo()) return;

  const clientId = `asisto_${tenantId}_${numero}`;
  const sessionDir = getLocalAuthSessionDir(clientId);

    if (!localAuthDirHasKeyData(sessionDir)) {
    EscribirLog(`LocalAuth: BACKUP skip (sin data clave) (${sessionDir})`, "event");
     return;
   }

  
  

  const filename = `LocalAuth-${clientId}.zip`;
  const bucket = getBucket();

  // Zip -> upload
  const buf = zipDirectoryToBuffer(sessionDir);
  const bytes = buf?.length || 0;

   // Evitar backups concurrentes (duplican GridFS files/chunks)
   if (savingLocalAuth) {
     sessionLog(`LocalAuth: BACKUP skip (ya hay uno en curso) label=${label}`);
     return;
   }
   savingLocalAuth = true;

   try {
  
     // FAILOVER + 1 SOLO BACKUP:
    // 1) subimos a "staging filename" (temporal)
    // 2) promovemos staging -> filename canónico
    // 3) borramos duplicados y staging viejos
    const tmpName = `${filename}.tmp-${instanceId}-${Date.now()}`;
    sessionLog(`LocalAuth: BACKUP start label=${label} bytes=${bytes} tmp=${tmpName}`);
    let uploadedFileId = null;
    await new Promise((resolve, reject) => {
      const up = bucket.openUploadStream(tmpName, {
         metadata: {
           tenantId,
           numero,
           clientId,
           label,
           host: os.hostname(),
           pid: process.pid,
           savedAt: new Date()
         }
       });
       up.on("error", reject);
        up.on("finish", () => {
        try { uploadedFileId = up.id; } catch {}
        resolve();
      });
       up.end(buf);
     });

    if (!uploadedFileId) throw new Error("gridfs_upload_no_file_id");

    // 2) promover a nombre canónico (no hay ventana: el viejo sigue existiendo hasta acá)
    await promoteTempToCanonical("wa_localauth", uploadedFileId, filename);

    // 3) dejar SOLO 1 canónico (el más nuevo) y limpiar staging viejos
    await pruneOldLocalAuthFiles(bucket, filename, 1);
    // borrar cualquier staging remanente (por fallos anteriores)
    await deleteByFilenamePrefix("wa_localauth", `${filename}.tmp-`);

     sessionLog(`LocalAuth: BACKUP OK (${label}) filename=${filename} bytes=${bytes}`);
     EscribirLog(`REMOTE SESSION SAVED (LocalAuth->Mongo) bytes=${bytes} label=${label}`, "event");
     try { console.log(`REMOTE SESSION SAVED (LocalAuth->Mongo) bytes=${bytes} label=${label}`); } catch {}
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

const clientId = `asisto_${tenantId}_${numero}`;
const sessionDir = getLocalAuthSessionDir(clientId);
let movedAsideDir = null;

sessionLog(`LocalAuth: RESTORE check clientId=${clientId} sessionDir=${sessionDir}`);

// Cachear resultado (evita llamar 2 veces y tomar decisiones inconsistentes)
const forceRestore = String(process.env.LOCALAUTH_FORCE_RESTORE || "0").trim() === "1";


// Regla segura:
// - Si existe sesión local y está poblada -> NO restaurar automáticamente (evita borrar/renombrar por falsos negativos).
// - Solo se restaura si NO existe la carpeta / está vacía, o si el usuario fuerza LOCALAUTH_FORCE_RESTORE=1.
try {
  if (fs.existsSync(sessionDir) && dirLooksPopulated(sessionDir) && !forceRestore) {
    sessionLog(`LocalAuth: RESTORE skip (local folder populated). Use LOCALAUTH_FORCE_RESTORE=1 to override.`);
    return false;
  }
  if (fs.existsSync(sessionDir) && forceRestore) {
    sessionLog(`LocalAuth: FORCE restore -> limpio y restauro (${sessionDir})`);
    try { fs.rmSync(sessionDir, { recursive: true, force: true }); } catch {}
  }
} catch {}
// Si no existe, la creamos para el restore
try { fs.mkdirSync(sessionDir, { recursive: true }); } catch {}

 


  const filename = `LocalAuth-${clientId}.zip`;
  const bucket = getBucket();

  // Asegurar indices GridFS (evita: "Sort exceeded memory limit" en wa_localauth.chunks)
  // Se ejecuta una sola vez por proceso.
  try {
    if (!globalThis.__WA_LOCALAUTH_GRIDFS_INDEXES_OK) {
      const db = mongoose?.connection?.db;
      if (db) {
        await db.collection("wa_localauth.chunks").createIndex({ files_id: 1, n: 1 });
        await db.collection("wa_localauth.files").createIndex({ filename: 1, uploadDate: -1 });
        globalThis.__WA_LOCALAUTH_GRIDFS_INDEXES_OK = true;
      }
    }
  } catch (e) {
    // no frenamos el inicio, pero lo logueamos
    try { EscribirLog(`GridFS index ensure failed: ${e?.message || e}`, "error"); } catch {}
  }
  // Requisito: SOLO 1 backup. Entonces buscamos el canónico.


  const files = await bucket.find({ filename }).sort({ uploadDate: -1 }).limit(1).toArray();
  let picked = (files && files[0]) ? files[0] : null;

  // Si justo estamos en ventana de "staging promote", puede existir tmp pero no canónico.
  if (!picked) {
    const tmp = await bucket.find({ filename: { $regex: `^${escapeRegex(filename)}\\.tmp-` } }).sort({ uploadDate: -1 }).limit(1).toArray();
    if (tmp && tmp[0]) {
      picked = tmp[0];
      sessionLog(`LocalAuth: RESTORE no canonical; found staging fileId=${picked._id} uploadDate=${picked.uploadDate}`);
      // Lo promovemos para estabilizar estado (opcional pero recomendado)
      try {
        await promoteTempToCanonical("wa_localauth", picked._id, filename);
        await pruneOldLocalAuthFiles(bucket, filename, 1);
        await deleteByFilenamePrefix("wa_localauth", `${filename}.tmp-`);
      } catch (e) {
        sessionLog(`LocalAuth: RESTORE staging promote failed: ${e?.message || e}`);
      }
    }
  }

  if (!picked) {
    EscribirLog(`LocalAuth: no hay backup en Mongo (${filename}). Se pedirá QR.`, "event");
    sessionLog(`LocalAuth: RESTORE FAIL (no backup in Mongo) filename=${filename}`);
    return false;
  }

  sessionLog(`LocalAuth: RESTORE start filename=${picked.filename} fileId=${picked._id} uploadDate=${picked.uploadDate} timeoutMs=${RESTORE_TIMEOUT_MS}`);
 

  // Preparamos carpeta temporal para extraer/validar sin ensuciar sessionDir
  const tmpBase = path.join(os.tmpdir(), `asisto_localauth_restore_${clientId}_${Date.now()}`);
  let restoredBytes = 0;
  const f = picked;
  const tmpDir = `${tmpBase}_${String(f._id)}`;
  
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
    try { fs.mkdirSync(tmpDir, { recursive: true }); } catch {}

    try {
      // Validación clave: si el file existe pero NO tiene chunks, el download queda en 0 bytes y termina en timeout.
      const db = mongoose.connection.db;
      const chunksColl = db.collection("wa_localauth.chunks");
      const hasChunk = await chunksColl.findOne({ files_id: f._id }, { projection: { _id: 1 } });
      if (!hasChunk) {
        sessionLog(`LocalAuth: RESTORE FAIL (no chunks for fileId=${f._id}). Backup incompleto o limpieza. Se pedirá QR.`);
        return false;
      }


       const chunks = [];
      const t0 = Date.now();
      let total = 0;
      let nextLogAt = RESTORE_LOG_EVERY_BYTES;

      // Descarga con timeout + (opcional) 1 reintento si da timeout con bytes=0
      let attempt = 0;
      while (attempt <= RESTORE_RETRY_ON_TIMEOUT) {
        attempt++;
        try {
          await new Promise((resolve, reject) => {
            const dl = bucket.openDownloadStream(f._id);

            // timeout por INACTIVIDAD (se resetea con cada chunk)
            let to = null;
            const armTimeout = () => {
              try { if (to) clearTimeout(to); } catch {}
              to = setTimeout(() => {
                try { dl.destroy(new Error(`restore_timeout_${RESTORE_TIMEOUT_MS}ms`)); } catch {}
                reject(new Error(`LocalAuth restore timeout after ${RESTORE_TIMEOUT_MS}ms (bytes=${total})`));
              }, RESTORE_TIMEOUT_MS);
            };
            armTimeout();

            // Pin temporal: evita borrado durante restore (aunque corra cleanup)
           // best-effort, no rompe si falla
            try { pinGridFsFile(bucket, f._id, Math.max(LOCALAUTH_RESTORE_PIN_MS, RESTORE_TIMEOUT_MS + 60_000)); } catch {}

           try { sessionLog(`LocalAuth: RESTORE stream opened fileId=${f._id}`); } catch {}


            dl.on("data", (d) => {
              chunks.push(d);
              total += d.length;
              // IMPORTANTE: resetear timeout por actividad
              // (en este archivo estaba faltando, por eso corta aunque venga data lenta)
              armTimeout();
              if (total >= nextLogAt) {
                nextLogAt += RESTORE_LOG_EVERY_BYTES;
                sessionLog(`LocalAuth: RESTORE downloading... bytes=${total}`);
              }
            });

            // Logs extra para diagnosticar "bytes=0":
            // - si nunca aparece "file", suele ser stall/selección de server/permiso/stream bloqueado
            dl.on("file", (fileDoc) => {
              try { sessionLog(`LocalAuth: RESTORE file event name=${fileDoc?.filename || ""} len=${fileDoc?.length || ""}`); } catch {}
            });
            dl.on("close", () => {
              try { sessionLog(`LocalAuth: RESTORE stream closed fileId=${f._id} bytes=${total}`); } catch {}
            });


            dl.on("error", (err) => {
              try { if (to) clearTimeout(to); } catch {}
              try { unpinGridFsFile(bucket, f._id); } catch {}
              reject(err);
            });

            dl.on("end", () => {
              try { if (to) clearTimeout(to); } catch {}
              try { unpinGridFsFile(bucket, f._id); } catch {}
              resolve();
            });
          });
          break; // ok
        } catch (e) {
          const msg = String(e?.message || e);
          const isTimeout = msg.includes("restore timeout");
          if (isTimeout && total === 0 && attempt <= RESTORE_RETRY_ON_TIMEOUT) {
            sessionLog(`LocalAuth: RESTORE retry attempt=${attempt} reason=timeout_bytes0 fileId=${f._id}`);
            // reset acumuladores por si quedó basura
            chunks.length = 0;
            total = 0;
            nextLogAt = RESTORE_LOG_EVERY_BYTES;
            continue;
          }
          throw e;
        }




      }

      const buf = Buffer.concat(chunks);
      const bytes = buf?.length || 0;
      sessionLog(`LocalAuth: RESTORE downloaded bytes=${bytes} elapsedMs=${Date.now() - t0}`);
      if (!bytes) {
        sessionLog(`LocalAuth: RESTORE FAIL (download bytes=0) fileId=${f._id} -> pedirá QR`);
        return false;
      }
      extractZipBufferToDir(buf, tmpDir);

      // Validar que lo restaurado realmente tenga data clave
      if (!localAuthDirHasKeyData(tmpDir)) {
        sessionLog(`LocalAuth: RESTORE FAIL (incompleto) fileId=${f._id} uploadDate=${f.uploadDate}`);
        try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
        return false;
      }

      // Swap seguro: NO borrar sessionDir. Si falla el restore, revertimos.
      const parentDir = path.dirname(sessionDir);
      try { fs.mkdirSync(parentDir, { recursive: true }); } catch {}
      const prevDir = `${sessionDir}.prev-${Date.now()}`;
      let hadPrev = false;

      // si ya existía sesión local, la movemos a "prev" para poder volver atrás
      try {
        if (fs.existsSync(sessionDir)) {
          fs.renameSync(sessionDir, prevDir);
          hadPrev = true;
        }
      } catch (e) {
        // si no podemos renombrar, NO seguimos (evita quedarte sin sesión)
        sessionLog(`LocalAuth: RESTORE FAIL (cannot move existing session) err=${e?.message || e}`);
        try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
        return false;
      }

      // ahora intentamos colocar el restore
      try {
        try { fs.renameSync(tmpDir, sessionDir); } catch {
          // fallback cross-device
          extractZipBufferToDir(buf, sessionDir);
          try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
        }
      } catch (e) {
        sessionLog(`LocalAuth: RESTORE FAIL (swap exception) err=${e?.message || e}`);
        // revertir
        try { fs.rmSync(sessionDir, { recursive: true, force: true }); } catch {}
        if (hadPrev) {
          try { fs.renameSync(prevDir, sessionDir); } catch {}
        }
        return false;
      }

      // validar restore nuevo; si falla, revertimos al prev
      if (!localAuthDirHasKeyData(sessionDir)) {
        sessionLog(`LocalAuth: RESTORE FAIL (swap incompleto) -> revert to previous`);
        try { fs.rmSync(sessionDir, { recursive: true, force: true }); } catch {}
        if (hadPrev) {
          try { fs.renameSync(prevDir, sessionDir); } catch {}
        }
        return false;
      }

      // restore OK -> limpiar prev si existía
      if (hadPrev) {
        try { fs.rmSync(prevDir, { recursive: true, force: true }); } catch {}
      }


   // ✅ ACÁ MISMO es donde va la limpieza de la .bak (movedAsideDir)
      // (justo después de limpiar prevDir y antes de loguear RESTORE OK/return true)
      if (movedAsideDir) {
        try { fs.rmSync(movedAsideDir, { recursive: true, force: true }); } catch {}
        movedAsideDir = null;
      }


      restoredBytes = bytes;
      sessionLog(`LocalAuth: RESTORE OK (${sessionDir}) fileId=${f._id}`);
      EscribirLog(`LocalAuth: restore desde Mongo OK bytes=${restoredBytes} -> ${sessionDir}`, "event");
      try { console.log(`LocalAuth: restore desde Mongo OK bytes=${restoredBytes} -> ${sessionDir}`); } catch {}
      return true;
    } catch (e) {
      sessionLog(`LocalAuth: RESTORE error fileId=${f?._id} err=${e?.message || e}`);
      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
      // rollback: si movimos carpeta original a .bak y el restore falló, volvemos atrás
      if (movedAsideDir) {
        try { fs.rmSync(sessionDir, { recursive: true, force: true }); } catch {}
        try { fs.renameSync(movedAsideDir, sessionDir); } catch {}
        movedAsideDir = null;
      }
      return false;
    }
  
}


function scheduleLocalAuthBackups() {
   // OJO: 'ready' puede dispararse más de una vez por reconnect y duplicar intervals.
   if (localAuthBackupSchedulerStarted) {
     sessionLog("LocalAuth backup schedule: ya estaba iniciado (skip)");
     return;
   }
   localAuthBackupSchedulerStarted = true;

  if (!AdmZip) {
    console.log("ERROR: falta dependencia adm-zip. Instalá: npm i adm-zip");
    EscribirLog("ERROR: falta dependencia adm-zip. Instalá: npm i adm-zip", "error");
  }
  // IMPORTANTE: NO tragarnos errores: si falla el backup, lo logueamos.
  const safeSave = (label) => saveLocalAuthToMongo(label).catch((e) => {
    const msg = `saveLocalAuthToMongo failed (${label}): ${e?.message || e}`;
    try { console.log(msg); } catch {}
    EscribirLog(msg, "error");
  });

  // backups diferidos para evitar capturar sesión incompleta
  setTimeout(() => safeSave("ready+60s"), 60_000);
  setTimeout(() => safeSave("ready+120s"), 120_000);
  setTimeout(() => safeSave("ready+240s"), 240_000);

  // backup periódico
  const every = Math.max(160_000, Number(backup_every_ms) || 600_000);
    if (localAuthBackupInterval) {
     try { clearInterval(localAuthBackupInterval); } catch {}
     localAuthBackupInterval = null;
   }
   localAuthBackupInterval = setInterval(() => safeSave("interval"), every);

  EscribirLog(`LocalAuth backup schedule: every=${every}ms`, "event");
}


/**
 * Crea el cliente WhatsApp SOLO cuando Mongo está listo (mongoose.connection.db disponible).
 * Esto evita el crash de wwebjs-mongo: Cannot read properties of undefined (reading 'collection')
 */
async function createClientIfNeeded(opts = {}) {
  if (client) return client;

  // Necesitamos Mongo para lock + (backup/restore) LocalAuth
  const ok = await ensureMongo();
  if (!ok) throw new Error("mongo_not_ready");

  if (!tenantId || !numero) throw new Error("tenant_or_numero_missing");

  const clientId = `asisto_${tenantId}_${numero}`;

  const skipLocalRestore = !!opts.skipLocalRestore;

  // Antes de inicializar el navegador:
  // - RESTORE SOLO si realmente hace falta (no hay carpeta o está vacía)
  // - y SOLO una vez por proceso (evita restore repetido en recreaciones por "Target closed").
  try {
    if (!localAuthRestoreAttempted) {
      localAuthRestoreAttempted = true;
      const sessionDir = getLocalAuthSessionDir(clientId);
      const hasLocalFolder = (() => {
        try { return !!(sessionDir && fs.existsSync(sessionDir)); } catch { return false; }
      })();
      const localLooksOk = (() => {
        try {
          // En Windows/Chromium el "probe" de claves puede dar falsos negativos y romper la sesión.
          // Si la carpeta existe y está poblada, asumimos OK y NO tocamos.
          return hasLocalFolder && dirLooksPopulated(sessionDir);
        } catch { return false; }
      })();

      // IMPORTANTÍSIMO:
      // En recreates por "Target closed / execution context", NO restaurar ni mover carpetas.
      if (skipLocalRestore) {
        if (LOCALAUTH_VERBOSE) sessionLog(`LocalAuth: RESTORE skip (skipLocalRestore=true) (${sessionDir})`);
      } else if (!localLooksOk) {
 
        await restoreLocalAuthFromMongoIfNeeded();
      } else {
        if (LOCALAUTH_VERBOSE) sessionLog(`LocalAuth: RESTORE skip (createClientIfNeeded local exists) (${sessionDir})`);
      }
    } else {
      if (LOCALAUTH_VERBOSE) sessionLog(`LocalAuth: RESTORE skip (already attempted in this process)`);
    }
  } catch (e) {
    EscribirLog(`LocalAuth restore error: ${e?.message || e}`, "error");
  }


  const useRemoteAuth = isRemoteAuthMode();
  if (useRemoteAuth) {
    if (!store) store = new MongoStore({ mongoose });
  }

  client = new Client({
     // Con LocalAuth + restore/backup propio NO queremos que whatsapp-web.js borre la carpeta de sesión en auth_failure.
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
      return await client.sendMessage(to, content, sendOpts);
    } catch (e) {
      const msg = String(e && e.message ? e.message : e);
      const transient = msg.includes('Evaluation failed') ||
                        msg.includes('Execution context was destroyed') ||
                        msg.includes('Protocol error');
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
    if (!isOwner) return;
    if (!await ensureMongo()) return;
    if (!lockId) return;
    const now = new Date();
    const update = { $set: { state: state || null, lastSeenAt: now, tenantId: tenantId, tenantid: tenantId } };

    // Si salimos del estado QR, limpiamos el QR guardado para evitar confusión.
    if (state && state !== "qr") {
      update.$unset = { lastQrAt: "", lastQrDataUrl: "" };
    }

    await LockModel.updateOne({ _id: lockId, holderId: instanceId }, update);
  } catch {}
}

// Guarda el último QR en el lock para poder verlo desde el panel admin (/admin/wweb)
async function updateLockQrDataSafe(qrDataUrl, qrAtIso) {
  try {
    if (!isOwner) return;
    if (!qrDataUrl) return;
    if (!await ensureMongo()) return;
    if (!lockId) return;
    // OJO: este helper corre en el handler de QR. No debe depender de variables
    // locales de otras funciones (por ej. "staleNow"), porque si falla queda el
    // panel sin lastQrDataUrl y el botón "Ver QR" se deshabilita.
    const now = new Date();

    await LockModel.updateOne(
      { _id: lockId, holderId: instanceId },
      {
        $set: {
          state: "qr",
          tenantId: tenantId,
          // Mantener heartbeat "vivo" mientras se espera el escaneo.
          lastSeenAt: now,
          lastQrAt: String(qrAtIso || ""),
          lastQrDataUrl: String(qrDataUrl),
        },
      }
    );
  } catch {}
}

async function tryAcquireLock() {
  // Para Opción B necesitamos tenantId + numero + mongo_uri
  if (!tenantId || !numero || !mongo_uri) {
    console.log("ERROR: Falta tenantId/numero/mongo_uri en configuracion.json. No se inicia WhatsApp.");
    EscribirLog("ERROR: Falta tenantId/numero/mongo_uri en configuracion.json. No se inicia WhatsApp.", "error");
    return false;
  }

  // Normalizar tenantId (consistencia en Mongo / evitar dobles locks)
  tenantId = String(tenantId || '').trim();
  if (tenantId) tenantId = tenantId.toUpperCase();

  lockId = `${tenantId}:${numero}`;

  const okMongo = await ensureMongo();
  if (!okMongo || !LockModel) {
    console.log("ERROR: No se pudo conectar a Mongo. No se inicia WhatsApp.");
    EscribirLog("ERROR: No se pudo conectar a Mongo. No se inicia WhatsApp.", "error");
    return false;
  }

  
  // Aplicar política de sesión (bloqueos / pin por PC) antes de intentar lock
  const pol = await getPolicySafe();
  const hn = hostName();
  if (pol) {
    const blocked = Array.isArray(pol.blockedHosts) && pol.blockedHosts.includes(hn);
    const pinned =
     String(pol.mode || "any") === "pinned" &&
      String(pol.pinnedHost || "").trim() &&
      String(pol.pinnedHost || "").trim() !== hn;

    if (blocked) {
      isOwner = false;
      await updateLockStateSafe("standby");
      await pushHistory("policy_blocked", { host: hn, policy: pol });
      console.log(`POLICY: Esta PC (${hn}) está BLOQUEADA para ${lockId}. STANDBY.`);
      EscribirLog(`POLICY: PC bloqueada (${hn}) para ${lockId}.`, "event");
      return false;
    }
    if (pinned) {
      isOwner = false;
      await updateLockStateSafe("standby");
      await pushHistory("policy_pinned", { host: hn, pinnedHost: pol.pinnedHost, policy: pol });
      console.log(`POLICY: Sesión fijada a otra PC (${pol.pinnedHost}). Esta PC (${hn}) queda en STANDBY.`);
      EscribirLog(`POLICY: Sesión fijada a ${pol.pinnedHost}. Esta PC (${hn}) standby.`, "event");
      return false;
    }
  }

const now = new Date();
  const stale = new Date(now.getTime() - (Number(lease_ms) || 30000));

  // ------------------------------------------------------------
  // FIX: Evitar que DOS PCs "ganen" el lock en el arranque.
  // Antes se hacía upsert sobre {_id} y podía pisar holderId si otra PC insertaba justo antes.
  // Ahora: intentamos INSERT primero (solo 1 puede ganar). Si hay duplicate key, seguimos con el flujo normal.
  // ------------------------------------------------------------
  try {
    await LockModel.create({
      _id: lockId,
      tenantId,
      tenantid: tenantId,
      numero,
      holderId: instanceId,
      host: os.hostname(),
      pid: process.pid,
      state: "standby",
      startedAt: now,
      lastSeenAt: now
    });

    isOwner = true;
    lockAcquiredAt = now;
    try { console.log(`[LOCK] INSERT -> owner lockId=${lockId} holderId=${instanceId} host=${os.hostname()} pid=${process.pid}`); } catch {}
    return true;
  } catch (e) {
    const code = e && (e.code || e?.errorResponse?.code);
    const msg = String(e?.message || e || "");
    const dup = code === 11000 || msg.toLowerCase().includes("duplicate key");
    if (!dup) {
      try { console.log("tryAcquireLock insert error:", msg); } catch {}
      EscribirLog("tryAcquireLock insert error: " + msg, "error");
      return false;
    }
    // duplicate key: ya existe lock -> seguimos
  }
  // 1) Si ya existe un lock y NO está stale y NO es mío -> standby (sin upsert)
  const existing = await LockModel.findById(lockId).lean();
   if (!existing) {
    isOwner = false;
    return false;
  }

  const last = existing.lastSeenAt ? new Date(existing.lastSeenAt) : null;
  const isStale = !last || last < stale;
   const st = String(existing.state || "");
   // IMPORTANTE:
   // No permitimos takeover SOLO por state=offline (el panel a veces muestra "inactiva" aunque la sesión siga viva).
   // El takeover se controla exclusivamente por lease_ms (stale).
   const isOffline = ["offline", "release_requested", "reset_auth_requested"].includes(st);
   const isMine = existing.holderId === instanceId;
   const canTakeover = isStale;


  if (!isMine && !canTakeover) {
    try {
      const ageMs = last ? (now.getTime() - last.getTime()) : null;
      console.log(`[LOCK] OCUPADO -> standby lockId=${lockId} holderId=${existing.holderId} host=${existing.host || ''} lastSeenAt=${last ? last.toISOString() : 'null'} ageMs=${ageMs}`);
    } catch {}
    isOwner = false;
    return false;
  }
  if (!isMine && canTakeover) {
    try {
      const ageMs = last ? (now.getTime() - last.getTime()) : null;
      const reason = isOffline ? `state=${st}` : `ageMs=${ageMs} lease_ms=${lease_ms}`;
      console.log(`[LOCK] STALE -> takeover permitido lockId=${lockId} holderId=${existing.holderId} host=${existing.host || ''} lastSeenAt=${last ? last.toISOString() : 'null'} ${reason}`);

    } catch {}
  }

  if (isMine) {
    try { console.log(`[LOCK] REENTRY -> ya soy holder lockId=${lockId}`); } catch {}
  }

  // 2) Existe y es mío o está stale: intentamos tomarlo SIN upsert

  // 3) No existe: lo creo con upsert
  const doc = await LockModel.findOneAndUpdate(
   
    {
      _id: lockId,
      $or: [
        { holderId: instanceId },
        { lastSeenAt: { $lt: stale } },
         { lastSeenAt: { $exists: false } },
        
      ]
    },
    {
      $set: {
        tenantId,
        holderId: instanceId,
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
  if (isOwner) {
    // Si el doc tiene startedAt lo usamos, sino usamos "now"
    try { lockAcquiredAt = (doc && doc.startedAt) ? new Date(doc.startedAt) : now; } catch { lockAcquiredAt = now; }
  }
  return isOwner;
}


function startHeartbeat() {
  if (heartbeatTimer) return;
  heartbeatTimer = setInterval(async () => {
    try {
      if (!isOwner) return;
      if (!await ensureMongo()) return;
      if (!lockId) return;

      // Política: permitir bloquear/habilitar desde el panel.
      // Si disabled=true: apagamos el cliente (si estaba) y dejamos el lock vivo con state=disabled.
      try {
        const pol = await getPolicySafe();
        const disabled = !!(pol && pol.disabled === true);
        if (disabled && lastPolicyDisabled !== true) {
          lastPolicyDisabled = true;
          await pushHistory("policy_disabled", { disabled: true, by: "panel" });
        }
        if (!disabled && lastPolicyDisabled === true) {
          lastPolicyDisabled = false;
          await pushHistory("policy_disabled", { disabled: false, by: "panel" });
        }

        if (disabled && clientStarted) {
          try { if (client) await client.destroy(); } catch {}
          clientStarted = false;
        }
      } catch {}

      const desiredState = (lastPolicyDisabled === true)
        ? "disabled"
        : (clientStarted ? "online" : (lastQrDataUrl ? "qr" : "starting"));


      const r = await LockModel.updateOne(
        { _id: lockId, holderId: instanceId },
        {
          $set: {
            lastSeenAt: new Date(),
            // Mantener el panel "ONLINE" mientras esta instancia vive y tiene el lock.
             state: desiredState,
            host: os.hostname(),
            pid: process.pid
          }
        }
      );
      if (!r || r.matchedCount === 0) {
        // perdimos el lock
        isOwner = false;
        sessionLog(`[LOCK] LOST -> otra PC tomó el lockId=${lockId}`);
        if (clientStarted) {
          try { await client.destroy(); } catch {}
          clientStarted = false;
        }
      }

      // Si acaba de habilitarse desde el panel y todavía no iniciamos, intentamos arrancar.
      try {
        if (isOwner && lastPolicyDisabled !== true && !clientStarted && !startingNow) {
          startClientInitialize();
        }
      } catch {}

    } catch {}
  }, heartbeat_ms || 10000);
}


async function startClientInitialize() {
  // Inicializa WhatsApp SOLO si esta instancia es dueña del lock.
  if (clientStarted) return;
  if (!isOwner) return;


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

  // Evita que el timer de standby (poll) llame varias veces mientras inicializa
  if (startingNow) return;
  startingNow = true;

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
  } finally {
    startingNow = false;
  }
}
async function bootstrapWithLock() {
  // intenta adquirir
  const ok = await tryAcquireLock();
  if (ok) {
    isOwner = true;
    try { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } } catch {}
    startHeartbeat();
    startActionPoller();
    await startClientInitialize();
    return;
  }

  // standby
  console.log(`STANDBY: sesión activa en otra PC (${lockId}). No se inicializa WhatsApp acá.`);
  pushHistory('standby_other_pc', { lockId }).catch(()=>{});
  EscribirLog(`STANDBY: sesión activa en otra PC (${lockId}).`, "event");

  if (!pollTimer) {
    pollTimer = setInterval(async () => {
      try {
        const ok2 = await tryAcquireLock();
        if (ok2) {
          // Ya tomamos el lock: frenamos el poll para no inicializar 2 veces.
          try { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } } catch {}
          console.log("LOCK TOMADO (otra PC cayó) -> iniciando...");
          EscribirLog("LOCK TOMADO (otra PC cayó) -> iniciando...", "event");
          // IMPORTANTÍSIMO: si no seteamos isOwner, NO corre el heartbeat y el panel queda "inactiva"
          isOwner = true;
          startHeartbeat();
          startActionPoller();
          await startClientInitialize();
        }
      } catch {}
    }, 8000);
  }
}


async function forceReleaseLock(finalState) {
  // Libera el lock SIN borrarlo (así el panel sigue viéndolo) y permite takeover inmediato.
  // finalState: estado a dejar (ej: 'offline', 'release_requested', 'reset_auth_requested')
  const st = String(finalState || 'offline');
  try {
    if (!await ensureMongo()) return;
    if (!lockId) return;
    // Forzamos "stale inmediato" para permitir takeover sin esperar lease_ms.
    const staleNow = new Date(0);
    // Solo el holder actual puede soltar el lock.
    await LockModel.updateOne(
      { _id: lockId, holderId: instanceId },
      {
        $set: {
          state: st,
          // Dejamos stale inmediato SOLO cuando liberamos explícitamente.
          lastSeenAt: staleNow,
          releasedAt: new Date(),
          releasedBy: instanceId,
          lastOwnerHost: os.hostname(),
        },
        // IMPORTANTE:
        // NO hacemos unset de holderId/host/pid porque eso deja el lock "sin dueño"
        // y otra PC lo toma aunque esta siga viva (panel lo ve como inactiva).
        // El takeover se controla con lastSeenAt + lease_ms.
      }
    );

    isOwner = false;
    return 'released';
  } catch (e) {
    try { EscribirLog(`forceReleaseLock error: ${e?.message || e}`, 'error'); } catch {}
  }
}

async function releaseLock() {
  try {
    if (!isOwner) return;
    if (!await ensureMongo()) return;
    if (!lockId) return;
    await LockModel.updateOne(
      { _id: lockId, holderId: instanceId },
      { $set: { state: "offline", lastSeenAt: new Date() } }
    );
  } catch {}
}



let actionTimer = null;
let actionBusy = false;

async function handleActionDoc(doc) {
  const action = String(doc?.action || "").toLowerCase();
  const reason = String(doc?.reason || "");
  try {
    if (action === "release") {
      EscribirLog(`Accion RELEASE recibida: ${reason}`, "event");
      await updateLockStateSafe("release_requested");

      // apagar WA en esta PC y LIBERAR lock (sin borrarlo) para takeover inmediato
      // best-effort: forzar logout para que WhatsApp corte la sesion anterior
      try { if (client && typeof client.logout === "function") await client.logout(); } catch {}
      // 1) best-effort logout: fuerza a WA a cortar la sesión en esta PC
      try { if (client && typeof client.logout === "function") await client.logout(); } catch {}
      // 2) destruir hard si existe helper, sino destroy normal
      try {
        if (client && typeof destroyClientHard === "function") await destroyClientHard(client);
        else if (client) await client.destroy();
      } catch {}
      try { client = null; } catch {}
      clientStarted = false;
     // detener timers locales (no deben seguir marcando estados ni consumir acciones)
      try { if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; } } catch {}
      try { if (actionTimer) { clearInterval(actionTimer); actionTimer = null; } } catch {}

      // 3) detener timers locales (evita que siga marcando estados o procesando acciones)
      try { if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; } } catch {}
      try { if (actionTimer) { clearInterval(actionTimer); actionTimer = null; } } catch {}

      // 4) liberar lock y dejar stale inmediato para takeover
      try { await forceReleaseLock("offline"); } catch {}
   
      isOwner = false;
      return "released";
    }

    if (action === "restart") {
      EscribirLog(`Accion RESTART recibida: ${reason}`, "event");
      await updateLockStateSafe("restarting");

      try { if (client) await client.destroy(); } catch {}
      clientStarted = false;

      // si seguimos siendo owner, reiniciamos
      isOwner = true;
      await startClientInitialize();
      return "restarted";
    }

    if (action === "resetauth") {
      // La limpieza de auth remota (GridFS) normalmente se hace del lado servidor admin,
      // acá solo liberamos para forzar nuevo QR en la próxima inicialización.
      EscribirLog(`Accion RESET AUTH recibida: ${reason}`, "event");
      await updateLockStateSafe("reset_auth_requested");
      try { if (client) await client.destroy(); } catch {}
      clientStarted = false;
      try { await forceReleaseLock(); } catch {}
      isOwner = false;
      
      return "reset_auth_requested";
    }

    return "ignored";
  } catch (e) {
    EscribirLog(`Error manejando accion ${action}: ${e?.message || e}`, "error");
    return "error";
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
    // Tomar 1 acción pendiente (doneAt no seteado), por lockId
    const doc = await ActionModel.findOneAndUpdate(
      { lockId, doneAt: { $exists: false } },
      { $set: { doneAt: new Date(), doneBy: instanceId } },
      { sort: { requestedAt: 1 }, new: true }
    ).lean();

    if (!doc) return;
    // ✅ Ignorar acciones viejas (pendientes de un owner anterior).
    // Si se ejecutan, pueden llamar forceReleaseLock() y dejar lastSeenAt=Date(0),
    // haciendo que el panel muestre "Inactiva/offline" aunque el proceso esté vivo.
    try {
     const reqAt = doc.requestedAt ? new Date(doc.requestedAt) : null;
      if (lockAcquiredAt && reqAt && reqAt.getTime() < lockAcquiredAt.getTime()) {
        await ActionModel.updateOne(
          { _id: doc._id },
          { $set: { result: "stale_ignored" } }
        );
        return;
      }
    } catch {}


    const result = await handleActionDoc(doc);
    await ActionModel.updateOne({ _id: doc._id }, { $set: { result } });
  } catch (e) {
    // si algo falló, liberamos busy y seguimos
  } finally {
    actionBusy = false;
  }
}

function startActionPoller() {
  if (actionTimer) return;
  actionTimer = setInterval(pollActionsOnce, 4000);
}


async function gracefulShutdown(signal) {
  try { sessionLog(`[SHUTDOWN] ${signal} -> cerrando WhatsApp...`); } catch {}
  try { if (autoUpdateTimer) { clearInterval(autoUpdateTimer); autoUpdateTimer = null; } } catch {}
   try { if (client) { try { await client.destroy(); } catch {} } } catch {}
  try { await saveLocalAuthToMongo(`shutdown_${signal}`); } catch {}
  // IMPORTANTE: liberamos el lock para takeover inmediato (sin esperar lease_ms)
  try { await forceReleaseLock(); } catch {}
  try { isOwner = false; } catch {}
 
  process.exit(0);

}

process.on("SIGINT", () => { gracefulShutdown("SIGINT"); });
process.on("SIGTERM", () => { gracefulShutdown("SIGTERM"); });
// Windows: cerrar consola / Ctrl+Break
process.on("SIGBREAK", () => { gracefulShutdown("SIGBREAK"); });




////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
async function ConsultaApiMensajes(){
  console.log("Consultando a API ");
  let url =  api2+'?key='+key+'&nro_tel_from='+telefono_qr;
  let url_confirma_msg = api3+'?key='+key+'&nro_tel_from='+telefono_qr;
  var a= 0;
  console.log("Conectando a API "+url);
  EscribirLog("Conectando a API "+url,"event");




 // await io.emit('message', "Conectando a API "+url );
  
  await sleep(1000);

     while(a < 10){

      RecuperarJsonConfMensajes();
   
     seg_msg = Math.random() * (devolver_seg_hasta() - devolver_seg_desde()) + devolver_seg_desde();
      
     //console.log('segundos entre mensajes '+seg_msg);
         try{
     

        resp = await fetch(url, {
            method: "GET"
        })

        .catch(err =>   EscribirLog(err,"error"))
      //  console.log('resp '+JSON.stringify(resp));
      //  await io.emit('message', JSON.stringify(resp) );
        if (resp.ok  ) {
     
          tam_json = 0;
          json = await resp.json()
  
          var tam_mensajes = Object.keys(json[0].mensajes).length

          var tam_destinatarios = Object.keys(json[0].destinatarios).length
       
          for(var i = 0; i < tam_destinatarios; i++){


            //filtro mensaje en json
            function obtenerMensajesPorId() {
              return Id_msj_renglon = json[0].destinatarios[i].Id_msj_renglon;
            }
            function buscarID(element) {
                return element.Id_msj_renglon == obtenerMensajesPorId();
            }

           // obtengo json con mensaje filtrado
            var respuesta = json[0].mensajes.filter(buscarID)


            var tam_mensajes = Object.keys(respuesta).length
           // console.log("tamaño j "+tam_mensajes)

            for(var j = 0; j < tam_mensajes; j++){
              console.log('--------------------------------------------------')
               Id_msj_dest = json[0].destinatarios[i].Id_msj_dest;
               Id_msj_renglon = json[0].destinatarios[i].Id_msj_renglon;
              var Nro_tel = json[0].destinatarios[i].Nro_tel;
              var Nro_tel_format = Nro_tel+'@c.us';
              var Fecha_envio = respuesta[j].Fecha_envio;
              var Nro_tel_from = respuesta[j].Nro_tel_from;
              var nro_tel_from_format = Nro_tel_from+'@c.us';
              var Msj = respuesta[j].Msj;
              var contenido = respuesta[j].Content;
              var Content_nombre = respuesta[j].Content_nombre;
              var Id_msj_renglon = respuesta[j].Id_msj_renglon;

              console.log("Id_msj_dest "+JSON.stringify(json[0].destinatarios[i].Id_msj_dest))
              console.log("Id_msj_renglon "+JSON.stringify(json[0].destinatarios[i].Id_msj_renglon))
              console.log("Nro_tel "+JSON.stringify(json[0].destinatarios[i].Nro_tel))
              console.log("Fecha_envio "+JSON.stringify(respuesta[j].Fecha_envio))
              console.log("Nro_tel_from "+JSON.stringify(respuesta[j].Nro_tel_from))
              console.log("Msj "+JSON.stringify(respuesta[j].Msj))
              console.log("Content_nombre "+JSON.stringify(respuesta[j].Content_nombre))
              console.log("Id_msj_renglon "+JSON.stringify(respuesta[j].Id_msj_renglon))
              console.log('--------------------------------------------------')
              //console.log('IS REGISTER '+Nro_tel)  ;
              if (!isNaN(Number(Nro_tel))) {
              if (!await client.isRegisteredUser(Nro_tel_format)) {
                 EscribirLog('Mensaje: '+Nro_tel_format+': Número no Registrado',"event");
                console.log("numero no registrado");
                await io.emit('message', 'Mensaje: '+Nro_tel_format+': Número no Registrado' );
                actualizar_estado_mensaje(url_confirma_msg,'I',null,null,null,null,null,json[0].destinatarios[i].Id_msj_renglon,json[0].destinatarios[i].Id_msj_dest );
                   
              
              }
              else { 
               
              if(Content_nombre == null || Content_nombre == ''){Content_nombre = 'archivo'}
               
                //if (contenido != null || contenido != '' ){
                 if (contenido != null ){
                  console.log('tipo de dato: '+detectMimeType(contenido));
                  var media = await new MessageMedia(detectMimeType(contenido), contenido, Content_nombre);
                  //await safeSend(Nro_tel_format,media,{caption:  Msj });
                  await io.emit('message', 'Mensaje: '+Nro_tel_format+': '+ Msj );
                  await safeSend('5493462674128@c.us',media,{caption:  Msj });
                  
                  } else{
                    console.log("msj texto");
                    if (Msj == null){ Msj = ''}
                    //await safeSend(Nro_tel_format,  Msj );
                    await io.emit('message', 'Mensaje: '+Nro_tel_format+': '+ Msj );
                    await safeSend('5493462674128@c.us',  Msj );
                  }
                  /////////////////////////////////////////
                  let contact = await client.getContactById(Nro_tel_format);
                      let tipo, contacto, email, direccion, nombre

                    if(contact.isBusiness==true){
                        tipo = 'B';
                        contacto = contact.pushname;
                        email= contact.businessProfile.email;
                        direccion = contact.businessProfile.address;
                        nombre = contact.name;
                      } else{
                        tipo = 'C';
                        nombre =  contact.name;
                        contacto = contact.shortName;
                        direccion = null;
                        email = null;
                    }
                    actualizar_estado_mensaje(url_confirma_msg,'E',tipo,nombre,contacto,direccion,email,json[0].destinatarios[i].Id_msj_renglon,json[0].destinatarios[i].Id_msj_dest );
                  
                    await sleep(seg_msg);
                  
                }
              }
              else
              {
                console.log("numero invalido");
                await io.emit('message', 'Mensaje: '+Nro_tel_format+': Número Inválido' );
                actualizar_estado_mensaje(url_confirma_msg,'I',null,null,null,null,null,json[0].destinatarios[i].Id_msj_renglon,json[0].destinatarios[i].Id_msj_dest );
               

              }


                
          }   console.log('--------------------------------------------------')
          
        }
      }
     else
     {
      json = await resp.json();
       
       if (msg_errores == ''|| msg_errores == null){

       }
       else{
        console.log("ApiWhatsapp - Response ERROR "+JSON.stringify(json));
        EscribirLog("ApiWhatsapp - Response ERROR "+JSON.stringify(json),"error");
       
       }
      // return 'error'
     }
 }
 catch (err) {
   console.log(err);
    EscribirLog(err,"error");
   if (msg_errores == ''|| msg_errores == null){
   }
   else{
    
   
    
   }
   //return 'error'
 }
  RecuperarJsonConfMensajes();
  seg_tele = devolver_seg_tele();
  //console.log("esperando por API "+seg_tele/1000 + ' segundos...');
  await sleep(seg_tele);
    }

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

function attachClientHandlers() {

client.on('message', async message => {

if (message.from=='5493462514448@c.us'   ){

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

   
   var telefonoTo = message.to;
  // var telefonoFrom = message.from;

      const contact = await client.getContactById(message.from); // ej: '1203...@lid'
      console.log(contact.id._serialized); // '1203...@lid'
      console.log(contact.number); 
//tel_from = contact.number;
      var telefonoFrom = contact.number;  
    //var telefonoFrom = '5493425472992@c.us' 
   // var telefonoTo = '5493424293943@c.us'

    telefonoTo = telefonoTo.replace('@c.us','');

   // telefonoFrom = telefonoFrom.replace('@c.us','');
   
    var resp = null;
 

    if(telefonoFrom == 'status@broadcast'){
      console.log("mensaje de estado");
      return
    }
    if(message.type !== 'chat'){
      console.log("mensaje <> texto");
      return
    }

    if(message.to == ''|| message.to == null){
      console.log("message.to VACIO");
      return
    }

    if(message.from == ''|| message.from == null){
      console.log("message.from VACIO");
      return
    }
    console.log("mensaje");
   
      //////////////////////////////////////////////////////////
      // MENSAJE DE ESPERO POR FAVOR
      ////////////////////////////////////////////////////////
      if (msg_inicio == ''|| msg_inicio == null){
      }
      else{
        safeSend(message.from,msg_inicio );
      }

      await io.emit('message', 'Mensaje: '+telefonoFrom+': '+ message.body );

      var jsonTexto = {  Tel_Origen: telefonoFrom ?? "",  Tel_Destino: telefonoTo ?? "",  Mensaje: message?.body ?? "",  Respuesta: ""};
   
      jsonTexto = {Tel_Origen:telefonoFrom,Tel_Destino:telefonoTo, Mensaje:message.body,Respuesta:''};
      // jsonTexto = {Tel_Origen:'5493462674128',Tel_Destino:'5493424293943', Mensaje:message.body,Respuesta:''};

      let url =  api

      console.log(JSON.stringify(jsonTexto));
      EscribirLog("Mensaje "+JSON.stringify(jsonTexto),'event');
      
   let controller; let timeoutId;
   try{
         controller = new AbortController();
         timeoutId = setTimeout(() => controller.abort(), 55000);
 
         const resp = await fetch(url, {
           method: "POST",
           body: JSON.stringify(jsonTexto),
           headers: {"Content-type": "application/json; charset=UTF-8"},
        signal: controller.signal
         });

           clearTimeout(timeoutId);
        
         const raw = await resp.text();
        let json = null;
        try { json = raw ? JSON.parse(raw) : null; } catch { /* no es JSON */ }

         //json = await resp.json();
         console.log(json)
         
         if (!resp.ok) {
            const detalle = json ? JSON.stringify(json) : raw;
             EscribirLog("Error 02 ApiWhatsapp - Response ERROR " + detalle, "error");
             EnviarEmail("ApiWhatsapp - Response ERROR ", detalle);
             if (msg_errores) await safeSend(message.from, msg_errores);
            return "error";
          }

      

          tam_json = 0; // 👈 evitá globals sin const/let; ideal: const tam_json = 0;
            recuperar_json(message.from, json);
            await procesar_mensaje(json, message);

            if (msg_fin) {
               await safeSend(message.from, msg_fin);
            }



           return "ok";

} catch (err) {
  clearTimeout(timeoutId);
  // Importante: no dependas de jsonTexto indefinido en el log
  const detalle = "Error 03 Chatbot Error " + (err?.message || err) + " " + JSON.stringify(jsonTexto);
  console.log(detalle);
  EscribirLog(detalle, "error");
  EnviarEmail("Chatbot Error ", detalle);
  if (msg_errores) await safeSend(message.from, msg_errores);
  return "error";
}





////////////////////
    };
   
    var body = message.body;
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
}  //

});







client.on('ready', async () => {
  console.log("listo ready....");
  telefono_qr = client.info.me.user
  console.log("TEL QR: "+client.info.me.user);
  
    
   await io.emit('message', 'Whatsapp Listo!');
   EscribirLog('Whatsapp Listo!',"event");
  await ensureMongo();
  await cleanupOrphanGridFSChunks("wa_localauth");
  // Programar backups periódicos de sesión (LocalAuth -> Mongo)
  // Programar backups periódicos SOLO si usamos LocalAuth (RemoteAuth ya guarda en Mongo).
  if (isLocalAuthMode()) {
    try { scheduleLocalAuthBackups(); } catch (e) { EscribirLog(`scheduleLocalAuthBackups error: ${e?.message || e}`, 'error'); }
  }
   // Para el panel: sesión activa
  updateLockStateSafe('online').catch(()=>{});
  // Opcional: si querés conservar un "hito" ready en historial:
  // updateLockStateSafe('ready').catch(()=>{});

  //ConsultaApiMensajes();


});

client.on('qr', (qr) => {
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

  // Backup temprano (apenas se autentica) para minimizar casos donde se corta antes del "ready"
  try {
    setTimeout(() => saveLocalAuthToMongo('authenticated+20s').catch((e) => {
      const msg = `saveLocalAuthToMongo failed (authenticated+20s): ${e?.message || e}`;
      try { console.log(msg); } catch {}
      EscribirLog(msg, 'error');
    }), 20_000);
  } catch {}
});



client.on('auth_failure', async function(session) {
  io.emit('message', 'Auth failure');
  EnviarEmail('Chatbot error Auth failure','Auth failure: '+ String(session || '') + ' ' + client);
  EscribirLog('Error 04 - Chatbot error Auth failure', String(session || ''), "error");
  updateLockStateSafe('auth_failure').catch(()=>{});

  // Con LocalAuth, whatsapp-web.js puede borrar la carpeta de sesión si restartOnAuthFail=true.
  // Acá lo manejamos nosotros: intentamos restaurar desde Mongo y reiniciar el cliente SIN borrar la sesión.
  if (isLocalAuthMode() && isOwner && !authFailureHandling) {
    authFailureHandling = true;
    setTimeout(async () => {
      try {
        // Permitir un nuevo intento de RESTORE en este proceso
        localAuthRestoreAttempted = false;

        try { await destroyClientHard(client); } catch {}
        try { client = null; } catch {}

        // Si hay backup remoto, lo baja a disco antes de re-inicializar
        try { await restoreLocalAuthFromMongoIfNeeded(); } catch (e) {
          EscribirLog('LocalAuth restore on auth_failure failed: ' + String(e?.message || e), 'error');
        }

        // Reintenta inicializar (solo si seguimos teniendo el lock)
        if (isOwner && !clientStarted) {
          await startClientInitialize();
        }
      } catch (e) {
        EscribirLog('auth_failure recovery error: ' + String(e?.message || e), 'error');
      } finally {
        authFailureHandling = false;
      }
    }, 2000);
  }
});

client.on('disconnected', async (reason) => {
  io.emit('message', 'Whatsapp Desconectado!');
  EnviarEmail('Chatbot Desconectado ','Desconectando...'+client);
  EscribirLog('Chatbot Desconectado ','Desconectando...',"event");
  updateLockStateSafe('disconnected').catch(()=>{});

  try { await client.destroy(); } catch(e) {}
  clientStarted = false;

  // Solo reintenta si esta PC sigue siendo owner del lock.
  if (isOwner) {
    setTimeout(() => {
      if (isOwner && !clientStarted) startClientInitialize();
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

  for( var i=jsonGlobal[indice][1]; i < tam_json; i++){
   
    if(l_json[i].cod_error){ 
      var mensaje = l_json[i].msj_error;
      EscribirLog('Error 05 - procesar_mensaje() devuelve cod_error API ',"error");
      EnviarEmail('ChatBot Api error ',mensaje);
    }else{
      var mensaje =  l_json[i].Respuesta;
    }
      
      if (mensaje == '' || mensaje == null || mensaje == undefined ){
      }
      else{
    
        mensaje = mensaje.replaceAll("|","\n");
    
        console.log("mensaje "+message.from+" - "+mensaje);
        
        if(i<= cant_lim + jsonGlobal[indice][1] -1){
        
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
        msg_loc = msg_loc.replace('<recuento>', cant_lim+1);
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
     for(var i in jsonGlobal){
     
      if( jsonGlobal[i][3] !== ''){
        var fecha = new Date();
        var fecha_msg = jsonGlobal[i][3].getTime();
        var fecha_msg2 = fecha.getTime();
        var diferencia = fecha_msg2-fecha_msg;
        if(diferencia > time_cad ){
          if(msg_cad == '' || msg_cad  == undefined || msg_cad == 0 ){
            
          } else {
            safeSend(jsonGlobal[i][0],msg_cad );

          }
          console.log("timepo expirado "+ jsonGlobal[i][0]+' '+diferencia+' '+time_cad );
          // delete(jsonGlobal[i]);
          let now = new Date();
          jsonGlobal[i][3] = '';
          jsonGlobal[i][2] = '';
          jsonGlobal[i][1] = 0;
          }
        }

        
        
    }
   
    await sleep(5000);
  }   
}

 
function RecuperarJsonConfMensajes(){
  // Mensajes/config vienen de MongoDB (tenantConfig). Mantiene configuracion_errores.json desde archivo.
  let jsonError = null;
  try { jsonError = JSON.parse(fs.readFileSync('configuracion_errores.json')); } catch {}
  try {
    if (jsonError && jsonError.configuracion) {
      email_err = jsonError.configuracion.email_err;
      smtp = jsonError.configuracion.smtp;
      email_usuario = jsonError.configuracion.user;
      email_pas = jsonError.configuracion.pass;
      email_puerto = jsonError.configuracion.puerto;
      email_saliente = jsonError.configuracion.email_sal;
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


async function EnviarEmail(subjet,texto){
/*
  texto = JSON.stringify(texto);
  console.log("email "+email_err);
  console.log("email2 "+subjet);
  console.log("email3 "+texto);

  subjet= nom_chatbot +" - "+subjet;

  let testAccount = await nodemailer.createTestAccount();

  let transporter = nodemailer.createTransport({
    host: smtp,
    port: email_puerto,
    secure: false, // true for 465, false for other ports
    auth: {
      user: email_usuario, // generated ethereal user
      pass: email_pas, // generated ethereal password
    },
  });
  
  let info = await transporter.sendMail({
    from: email_saliente, // sender address
    to: email_err, // list of receivers
    subject: subjet, // Subject line
    text: texto, // plain text body
    html: texto, // html body
  });

  console.log("Message sent: %s", info.messageId);
  
  console.log("Preview URL: %s", nodemailer.getTestMessageUrl(info));
 */
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

async function destroyClientHard(c) {
  if (!c) return;
  // whatsapp-web.js expone (segun version) pupBrowser/pupPage en el client.
  try { await c.destroy(); } catch {}
  try { await c.pupPage?.close?.(); } catch {}
  try { await c.pupBrowser?.close?.(); } catch {}
 await sleep(2500);
}

async function recreateClientForRetry(reason) {
  try { console.log(`Recreando client por: ${reason}`); } catch {}
  try { EscribirLog(`Recreando client por: ${reason}`, "event"); } catch {}

  try { await destroyClientHard(client); } catch {}
  try { clientStarted = false; } catch {}
  try { client = null; } catch {}

  // mini backoff para que Chrome termine de cerrar (Windows)
  await sleep(2500);

  // Re-crea el client:
  // Si venimos por execution_context / detached_frame, NO tocar el storage local ni forzar restore,
  // porque esos errores suelen ser del navegador, no de la sesión.
  const skipLocalRestore = (reason === "execution_context" || reason === "detached_frame");
  await createClientIfNeeded({ skipLocalRestore });
  return client;
}

async function initializeWithRetry(clientInstance, maxRetries = 5) {
  // IMPORTANTE: ante ciertos errores (detached frame / execution context) conviene
  // recrear TODO el client y reintentar. Re-usar el mismo objeto suele quedar roto.
  let c = clientInstance;

  for (let i = 1; i <= maxRetries; i++) {
    try {
      try {
        console.log(`[INIT] attempt=${i} dataPath=${getAuthBasePath()} sessionDir=${getLocalAuthSessionDir(`asisto_${tenantId}_${numero}`)}`);
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
  } catch {}

  // Si ya hay config de BD, aplicarla (no rompe si es null)
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
