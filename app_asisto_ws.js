/*script:app_asisto*/
/*version: 4.00.98  06/07/2026   */



const dns = require("dns");

try {
  dns.setServers(["8.8.8.8", "1.1.1.1"]);
  console.log("[DNS] Servidores DNS forzados para Node:", dns.getServers().join(", "));
} catch (e) {
  console.error("[DNS] No se pudieron configurar DNS:", e.message);
}


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
let odbc = null;
try {
  odbc = require("odbc");
} catch (e) {
  // ODBC es opcional: solo se usa para comandos administrativos como /e l.
  odbc = null;
}
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

process.on('unhandledRejection', (reason) => {
  exitForFatalProcessError('unhandledRejection', reason);
});

process.on('uncaughtException', (err) => {
  exitForFatalProcessError('uncaughtException', err);
});

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
    if (!p) return {};
    const raw = JSON.parse(fs.readFileSync(p, "utf8"));
    const obj = (raw && raw.configuracion && typeof raw.configuracion === "object") ? raw.configuracion : raw;
    return obj && typeof obj === "object" ? obj : {};
  } catch {
    return {};
  }
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

  if (!tenantId || !mongo_uri) throw new Error("Falta tenantId/mongo_uri en configuracion.json");

  const ok = await ensureMongo();
  if (!ok || !mongoose?.connection?.db) throw new Error("No se pudo conectar a Mongo para cargar configuración");

  const collName = String(process.env.ASISTO_CONFIG_COLLECTION || "tenant_config").trim() || "tenant_config";
  const coll = mongoose.connection.db.collection(collName);

  let doc = await coll.findOne({ _id: tenantId });
  if (!doc) doc = await coll.findOne({ tenantId: tenantId });
  if (!doc) throw new Error(`No existe configuración en BD para tenantId=${tenantId} (${collName})`);

  const conf = extractTenantConfigFromDoc(doc);
  tenantConfig = conf;
  applyTenantConfig(conf);

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
    if (!mongoose?.connection?.db) return '';

    const raw = String(rawId || '').trim();
    const lid = stripWhatsappSuffix(raw);
    const coll = mongoose.connection.db.collection('wa_lid_phone_map');
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
    const delay = Math.max(500, Number(
      tenantConfig?.message_create_fallback_ms ||
      tenantConfig?.messageCreateFallbackMs ||
      process.env.MESSAGE_CREATE_FALLBACK_MS ||
      1500
    ));

    // message tiene prioridad. message_create queda solo como fallback demorado.
    if (!id) {
      setTimeout(() => handler(message, 'message_create_fallback').catch(() => {}), delay);
      return;
     
    }
    if (recentIncomingProcessIds.has(id) || incomingCreateFallbackTimers.has(id)) return;
    const timer = setTimeout(async () => {
      incomingCreateFallbackTimers.delete(id);
      if (recentIncomingProcessIds.has(id)) {
        try { console.log('[INCOMING] message_create fallback skip id=' + id); } catch {}
        return;
      }
     await handler(message, 'message_create_fallback');
        }, delay);

    incomingCreateFallbackTimers.set(id, timer);
  } catch (e) {
    try { console.log('[message_create] schedule error:', e?.message || e); } catch {}
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
let auth_base_path = process.env.ASISTO_AUTH_PATH || "";            // LocalAuth dataPath override
let auth_mode = String(process.env.ASISTO_AUTH_MODE || '').trim().toLowerCase(); // 'remote' | 'local' (default: local)

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

  // En auto-update/reinicio supervisado NO esperamos destroyClientHard/client.destroy(),
  // porque WhatsApp Web/Puppeteer puede quedar colgado en Windows.
  // Liberamos estado/lock con timeout corto y dejamos que el runner/PM2 levante el nuevo proceso.
  try { resetClientRuntimeFlags('fast_exit:' + String(reason || '')); } catch {}
  try { localWsPanelState = 'offline'; } catch {}
  try { await Promise.race([updateLockStateSafe('offline'), timeoutPromise(1200, 'update_lock_timeout')]); } catch {}
  try { await Promise.race([forceReleaseLock('offline'), timeoutPromise(1800, 'release_lock_timeout')]); } catch {}
  try { isOwner = false; } catch {}

  try { process.exitCode = code; } catch {}
  setTimeout(() => { try { process.exit(code); } catch {} }, 100);
  setTimeout(() => { try { process.exit(code); } catch {} }, 1500);
}
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

function buildApiChatCabUrlFromApi(baseApi) {
  const raw = String(baseApi || '').trim();
  if (!raw) return raw;
  try {
    const u = new URL(raw);
    u.pathname = '/v200/api/Api_Chat_Cab/ProcesarMensajePost';
    u.search = '';
   u.hash = '';
    return u.toString();
  } catch {
    return raw;
  }
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
    if (!mongoose?.connection?.db) return fallback;
    const rows = await mongoose.connection.db.collection('tenant_channels')
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
  if (m === 'chatgpt') return buildApiChatCabUrlFromApi(api);
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

    const conf = extractTenantConfigFromDoc(doc);

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
    if (mongoose?.connection?.db && or.length) {
      const p0 = await mongoose.connection.db.collection('wa_wweb_policies').findOne({ $or: or });
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
      await autoUpdateForceTargetTagOnBoot('boot_target_tag_force');
      if (autoUpdateRestarting) return;
    } catch (e) {
       try { console.log('boot_target_tag_force auto-update error:', e?.message || e); } catch {}
      try { EscribirLog('boot_target_tag_force auto-update error: ' + String(e?.message || e), 'error'); } catch {}
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
    return { removed, sessionDir };
  } catch (e) {
    try { EscribirLog('[CLEAR_AUTH] clearLocalAuthFilesSafe error: ' + String(e?.message || e), 'error'); } catch {}
    return { removed: false, error: String(e?.message || e) };
  }
}

async function clearRemoteAuthStoreSafe(clientId) {
  const result = { attempted: false, removed: false };
  try {
    if (!isRemoteAuthMode()) return result;
    result.attempted = true;
    if (!store) store = new MongoStore({ mongoose });

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
        rememberOutgoingStatLogged(sent);
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

    setTimeout(() => {
      // Reinicio completo desde panel: NO esperamos destroyClientHard/gracefulShutdown.
      // En Windows + tarea programada, whatsapp-web.js/Puppeteer puede colgar destroy()
      // y el helper queda esperando eternamente el PID viejo. Cerramos el proceso rápido;
      // Windows libera puerto/handles y el helper arranca un Node nuevo.
      try { EscribirLog('[PROCESS_RESTART] salida rapida del proceso actual pid=' + process.pid, 'event'); } catch {}
      try { if (autoUpdateTimer) { clearInterval(autoUpdateTimer); autoUpdateTimer = null; } } catch {}
      try { if (runtimeConfigPollTimer) { clearInterval(runtimeConfigPollTimer); runtimeConfigPollTimer = null; } } catch {}
      try { if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; } } catch {}
      try { if (actionTimer) { clearInterval(actionTimer); actionTimer = null; } } catch {}
      try { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } } catch {}
      try { clearAuthReadyWatchdog('process_restart_fast_exit'); } catch {}
      try { localWsPanelState = 'restarting'; } catch {}
      try { client = null; } catch {}
      try { isOwner = false; } catch {}
      try { process.exit(0); } catch {}
    }, 500);

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
    try { EscribirLog('ensureTenantVersionBeforeWhatsAppStart update error: ' + String(e?.message || e), 'error'); } catch {}
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
        console.log("Chequeo de versión antes de iniciar WhatsApp falló:", e?.message || e);
        EscribirLog("Chequeo de versión antes de iniciar WhatsApp falló: " + String(e?.message || e), "error");
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

    // 3) Borrar autenticación real según modo.
    const clearResult = isRemoteAuthMode()
      ? await clearRemoteAuthStoreSafe(clientId)
      : await clearLocalAuthFilesSafe(clientId);

    try { await pushHistory('clear_auth', { reason, clientId, authMode: isRemoteAuthMode() ? 'remote' : 'local', result: clearResult }); } catch {}

    // 4) Mantener el lock/owner y reiniciar WhatsApp para que vuelva a emitir QR.
    isOwner = true;
    if (!lockAcquiredAt) lockAcquiredAt = new Date();
    await updateLockStateSafe('starting');
    await sleep(1500);
    await startClientInitialize();
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

        // IMPORTANTE:
    // - task_runner: no lanzamos otro node.exe desde este proceso; salimos con exitCode=77
    //   y asisto_ws_runner.cmd lo vuelve a iniciar.
    // - pm2: salimos con exitCode=0 para que PM2 reinicie el proceso.
    try { localWsPanelState = 'restarting'; } catch {}
    try { await updateLockStateSafe('restarting'); } catch {}
    try { await forceReleaseLock('restarting'); } catch {}

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
  try { if (client) { try { await destroyClientHard(client); } catch { try { await client.destroy(); } catch {} } } } catch {}
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

    if (!tenantId || !await ensureMongo() || !mongoose?.connection?.db) {
      consultaMensajesHoursCache = { expiresAt: now + 30000, hours: null, updatedAt: null };
      return null;
    }

    const tenant = String(tenantId || '').trim();
    const coll = mongoose.connection.db.collection('settings');
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
    if (!mongoose?.connection?.db) return null;
    return mongoose.connection.db.collection('wa_api_mensajes_confirmaciones');
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
          await safeSend(nroTelFormat, media, { caption: msj });
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
          await safeSend(nroTelFormat, msj);
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
              await safeSend(Nro_tel_format, media, { caption: Msj });
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
              await safeSend(Nro_tel_format, Msj);
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

async function startCompraEntregaLoopIfEnabled(source = '') {
  try {
    
    if (habilitar_odbc_manager !== true) {
      try { EscribirLog('queryAccessComprasEntregas no inicia: habilitar_odbc_manager=false' + (source ? ' source=' + source : ''), 'event'); } catch {}
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
  if (compraEntregaQueryRunning) {
    try { EscribirLog('queryAccessComprasEntregas: ya estaba corriendo, no se inicia otro loop', 'event'); } catch {}
    return;
  }

  compraEntregaQueryRunning = true;
  compraEntregaQueryStopRequested = false;

  try {
    if (!odbc) {
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
      compraEntregaConnection = await odbc.connect('DSN=' + dsn + '; charset=UTF8');
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

  console.log('MENSAJES_INFO: consultando es_mensajes origen=' + telefono_qr);
  const data = await compraEntregaConnection.query("select first * from es_mensajes where estado <> 'S' and tipo = 'WS' and origen =" + telefono_qr + ' order by prioridad asc');
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

function getMessageBodyText(message) {
  return String(message?.body || message?._data?.body || '').trim();
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

    const parts = body.split(/\s+/).filter(Boolean);
    const cmd = String(parts[0] || '').trim().toLowerCase();
    const param = String(parts[1] || '').trim().toLowerCase();
    if (cmd !== '/e') return false;

    const replyTo = adminReplyTarget(message);
    if (!replyTo) return true;

    if (!odbc) {
      await safeSendMessage(replyTo, 'ODBC no está disponible en este script. No puedo consultar pedidos.');
      return true;
    }

    console.log('[admin-command] OK source=' + source + ' from=' + message.from + ' to=' + message.to + ' fromMe=' + message.fromMe + ' body=' + body);
    EscribirLog('[admin-command] OK source=' + source + ' body=' + body, 'event');

    const cmdConnection = await odbc.connect('DSN=' + dsn + '; charset=UTF8');
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

   
   var telefonoTo = message.to;
  // var telefonoFrom = message.from;

      const remoteFrom = String(message.from || '').trim();
      var telefonoFrom = await resolvePhoneFromIncomingMessage(message);
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

     // let url =  api
      const botLogicMode = await getWwebBotLogicModeForPhone(telefonoTo);
      let url = getIncomingApiUrlForLogicMode(botLogicMode);

      console.log('[BOT] logic_mode=' + botLogicMode + ' url=' + url);
      try { EscribirLog('[BOT] logic_mode=' + botLogicMode + ' url=' + url, 'event'); } catch {}

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
        await handleAdminDeliveryCommand(message, 'message_create');
        return;
      }
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
    // No procesar inmediatamente message_create: puede traer datos internos y
    // duplicar el evento message. Se agenda solo como fallback si message no llega.
    scheduleIncomingFromMessageCreate(message, processIncomingAsistoMessage);
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
     console.log('[CONFIG] habilitar_bot=' + habilitar_bot + ' habilitar_consulta_mensajes=' + consulta_api_mensajes_habilitado + ' wweb_bot_logic_mode=' + wweb_bot_logic_mode + ' time_cad_ms=' + time_cad);
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
  EnviarEmail('Chatbot error Auth failure','Auth failure: '+ String(session || '') + ' ' + client);
  EscribirLog('Error 04 - Chatbot error Auth failure', String(session || ''), "error");
  updateLockStateSafe('auth_failure').catch(()=>{});
  clearAuthReadyWatchdog('auth_failure');
 

  // Sin backup/restore remoto: reiniciamos el cliente y dejamos que LocalAuth use solo la sesión local.
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
  EnviarEmail('Chatbot Desconectado ','Desconectando...'+client);
  EscribirLog('Chatbot Desconectado ','Desconectando...',"event");
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
