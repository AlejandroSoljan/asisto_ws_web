/*script:app_asisto*/
/*version:4.00.18   12/04/2026*/




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
// --- LocalAuth backup/restore removido ---
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

  // Core
  if (hasValue(conf.puerto)) port = asNumber(conf.puerto, port);
  if (conf.headless !== undefined) {
    headless = parseBoolLike(conf.headless, !!headless);
  }
  if (!numero && (conf.numero || conf.NUMERO)) numero = asString(conf.numero || conf.NUMERO, numero);
  if (conf.status_token !== undefined) status_token = asString(conf.status_token, status_token);

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

  // Mensajes / límites
  seg_desde = asNumber(conf.seg_desde, seg_desde);
  seg_hasta = asNumber(conf.seg_hasta, seg_hasta);
  if (conf.dsn !== undefined) dsn = String(conf.dsn);
  seg_msg = asNumber(conf.seg_msg, seg_msg);
  seg_tele = asNumber(conf.seg_tele, seg_tele);
  if (conf.api !== undefined) api = String(conf.api);
  if (conf.msg_inicio !== undefined) msg_inicio = String(conf.msg_inicio ?? "");
  if (conf.msg_fin !== undefined) msg_fin = String(conf.msg_fin ?? "");
  cant_lim = asNumber(conf.cant_lim, cant_lim);
  if (conf.msg_lim !== undefined) msg_lim = String(conf.msg_lim ?? "");
  time_cad = asNumber(conf.time_cad, time_cad);
  if (conf.msg_cad !== undefined) msg_cad = String(conf.msg_cad ?? "");
  if (conf.msg_can !== undefined) msg_can = String(conf.msg_can ?? "");
  if (conf.nom_emp !== undefined) nom_chatbot = String(conf.nom_emp);
  if (conf.nom_chatbot !== undefined) nom_chatbot = String(conf.nom_chatbot);

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


// Lease/heartbeat configurables (ms)
const MIN_LEASE_MS = Number(process.env.MIN_LEASE_MS || 180000);
let lease_ms = Number(process.env.LEASE_MS || MIN_LEASE_MS);
let heartbeat_ms = Number(process.env.HEARTBEAT_MS || 5000);
let backup_every_ms = Number(process.env.BACKUP_EVERY_MS || 300000);
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
  auto_update_repo_path = auto_update_repo_path || process.cwd();
  auto_update_remote = auto_update_remote || 'origin';
  if (!['tag', 'branch', 'tag_or_branch'].includes(auto_update_source)) auto_update_source = 'tag_or_branch';
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
  await runCommand('git', ['fetch', remote, '--tags', '--prune'], { cwd: repoPath, timeout: 120_000 });

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
      await runCommand('git', ['fetch', remote, '--tags', '--prune'], { cwd: repoPath, timeout: 120_000 });
    }

    if (selectedTag) {
      const headOut = await runCommand('git', ['rev-list', '-n', '1', selectedTag], { cwd: repoPath, timeout: 15_000 });
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
  autoUpdateLog(`[AUTO_UPDATE] activado repo=${repoPath} remote=${auto_update_remote} source=${auto_update_source} targetTag=${auto_update_target_tag || '(auto)'} branch=${auto_update_branch || '(auto)'} every=${auto_update_check_every_ms}ms startupDelay=${auto_update_startup_delay_ms}ms`, 'event');

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
let localWsPanelState = 'idle';
// Cache liviano: si la política marca disabled=true, no inicializamos WhatsApp.
let lastPolicyDisabled = null;
let mongoReady = false;
let LockModel = null;
let ActionModel = null;
let PolicyModel = null;      // wa_wweb_policies
let HistoryModel = null;     // wa_wweb_history
let MessageLogModel = null;  // wa_wweb_message_log
let heartbeatTimer = null;
let actionTimer = null;
let pollTimer = null;
let actionBusy = false;
let heartbeatBusy = false;
var a = 0;
var port = Number(process.env.PORT || 8002);
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
          tenantid: { type: String },
          tenantId: { type: String, index: true },
          numero: { type: String, index: true },
          disabled: { type: Boolean, default: false }
        },
        { collection: "wa_wweb_policies" }
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

    // Mantener la config completa del tenant en memoria y aplicarla al runtime.
    tenantConfig = conf;
    applyTenantConfig(conf);

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

    try { console.log(`[CONFIG] tenantId=${tenantId} numero=${numero} puerto=${port} headless=${headless} auth_mode=${auth_mode || 'local'} lease_ms=${lease_ms} heartbeat_ms=${heartbeat_ms} desiredTag=${auto_update_target_tag || '(auto)'}`); } catch {}
    return conf;
  } catch (e) {
    try { console.log("loadTenantConfigFromDbMinimal error:", e?.message || e); } catch {}
    try { EscribirLog("loadTenantConfigFromDbMinimal error: " + String(e?.message || e), "error"); } catch {}
    return null;
  }
}




async function refreshTenantConfigFromDbPerMessage() {
  try {
    if (!tenantId || !mongo_uri) return tenantConfig;
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
    if (!PolicyModel) return null;
    if (tenantId && numero) {
      const tid = String(tenantId);
      const num = String(numero);
      const p = await PolicyModel.findOne({
        numero: num,
        $or: [
          { tenantId: tid },
          { tenantid: tid }
        ]
      }).lean();
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

async function heartbeatTick() {
  try {
    if (heartbeatBusy) return;
    heartbeatBusy = true;
    if (!isOwner || !lockId) return;

    await updateLockStateSafe(localWsPanelState || 'online').catch(() => {});

    const pol = await getPolicySafe();
    const disabled = !!(pol && pol.disabled === true);

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
    waState,
    telefono_qr,
    runtimeInfo,
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



    
(async function startAsistoWs() {
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

    // Si el tenant pide una TAG concreta, validar al iniciar antes de levantar WhatsApp.
    try {
      await autoUpdateCheckAndApply('boot_target_tag');
      if (autoUpdateRestarting) return;
    } catch (e) {
      try { console.log('boot_target_tag auto-update error:', e?.message || e); } catch {}
      try { EscribirLog('boot_target_tag auto-update error: ' + String(e?.message || e), 'error'); } catch {}
    }

    server.listen(port, function() {
      console.log('App running on *: ' + port);
      EscribirLog('App running on *: ' + port,"event");
    });

    startAutoUpdateScheduler();

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
 * Crea el cliente WhatsApp SOLO cuando Mongo está listo (mongoose.connection.db disponible).
 * Esto evita el crash de wwebjs-mongo: Cannot read properties of undefined (reading 'collection')
 */
async function createClientIfNeeded(opts = {}) {
  if (client) return client;

  // Necesitamos Mongo para lock y estado del panel
  const ok = await ensureMongo();
  if (!ok) throw new Error("mongo_not_ready");

  if (!tenantId || !numero) throw new Error("tenant_or_numero_missing");

  const clientId = `asisto_${tenantId}_${numero}`;

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
      const sent = await client.sendMessage(to, content, sendOpts);
      try {
        const logPayload = (content && typeof content === 'object')
          ? { body: sendOpts.caption || '', type: content.mimetype ? 'media' : (content.type || 'text'), mimetype: content.mimetype || '', filename: content.filename || '', data: content.data ? '[data]' : '' }
          : { body: String(content || ''), type: 'text', hasMedia: false };
        await logMessageStat('out', to, logPayload);
      } catch {}
      return sent;
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

    if (!lockId) return;
    if (!await ensureMongo()) return;
    if (!LockModel) return;

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
  } catch {}
}

// Lock/lease multi-PC removido en modo simplificado.


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

async function handleActionDoc(doc) {
  const action = String(doc?.action || '').toLowerCase();
  const reason = String(doc?.reason || '');

  try {
    if (action === 'restart') {
      EscribirLog('Accion RESTART recibida: ' + reason, 'event');
      await updateLockStateSafe('restarting');

      try {
        if (client && typeof destroyClientHard === "function") await destroyClientHard(client);
        else if (client) await client.destroy();
      } catch {}
      try { client = null; } catch {}
      clientStarted = false;

      if (isOwner && !startingNow) {
        await startClientInitialize();
      }
      return 'restarted';
    }

    if (action === 'release') {
      EscribirLog('Accion RELEASE recibida: ' + reason, 'event');
      try {
        if (client && typeof destroyClientHard === "function") await destroyClientHard(client);
        else if (client) await client.destroy();
      } catch {}
      try { client = null; } catch {}
      clientStarted = false;
      localWsPanelState = 'offline';
      await forceReleaseLock('offline');
      isOwner = false;
      return 'released';
    }

    if (action === 'resetauth') {
      EscribirLog('Accion RESET AUTH recibida: ' + reason, 'event');
      try {
        if (client && typeof client.logout === 'function') await client.logout();
      } catch {}
      try {
        if (client && typeof destroyClientHard === "function") await destroyClientHard(client);
        else if (client) await client.destroy();
      } catch {}
      try { client = null; } catch {}
      clientStarted = false;
      localWsPanelState = 'offline';
      await forceReleaseLock('offline');
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
  try { sessionLog(`[SHUTDOWN] ${signal} -> cerrando WhatsApp...`); } catch {}
  try { if (autoUpdateTimer) { clearInterval(autoUpdateTimer); autoUpdateTimer = null; } } catch {}
  try { if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; } } catch {}
  try { if (actionTimer) { clearInterval(actionTimer); actionTimer = null; } } catch {}
  try { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } } catch {}
  try { if (client) { try { await client.destroy(); } catch {} } } catch {}
  try { localWsPanelState = 'offline'; } catch {}
  try { await updateLockStateSafe('offline'); } catch {}
  try { await forceReleaseLock('offline'); } catch {}
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

  try { await refreshTenantConfigFromDbPerMessage(); } catch {}
  try { RecuperarJsonConfMensajes(); } catch {}

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

    try {
      await logMessageStat('in', telefonoFrom, { body: message.body || '', type: message.type || 'chat', hasMedia: !!message.hasMedia });
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
//}  //

});







client.on('ready', async () => {
  console.log("listo ready....");
  telefono_qr = client.info.me.user
  console.log("TEL QR: "+client.info.me.user);
  
    
   await io.emit('message', 'Whatsapp Listo!');
   EscribirLog('Whatsapp Listo!',"event");
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

});



client.on('auth_failure', async function(session) {
  io.emit('message', 'Auth failure');
  EnviarEmail('Chatbot error Auth failure','Auth failure: '+ String(session || '') + ' ' + client);
  EscribirLog('Error 04 - Chatbot error Auth failure', String(session || ''), "error");
  updateLockStateSafe('auth_failure').catch(()=>{});

  // Sin backup/restore remoto: reiniciamos el cliente y dejamos que LocalAuth use solo la sesión local.
  if (isLocalAuthMode() && isOwner && !authFailureHandling) {
    authFailureHandling = true;
    setTimeout(async () => {
      try {
        try { await destroyClientHard(client); } catch {}
        try { client = null; } catch {}
        clientStarted = false;

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
