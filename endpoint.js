// endpoint.js
// Servidor Express y endpoints (webhook, behavior API/UI, cache, salud) con multi-tenant
// Incluye logs de fixReply en el loop de corrección.

require("dotenv").config();
const express = require("express");
const app = express();

const crypto = require("crypto");
const VERIFY_TOKEN = process.env.VERIFY_TOKEN || process.env.WHATSAPP_VERIFY_TOKEN;

// ⬇️ Para catálogo en Mongo
const { ObjectId } = require("mongodb");
const { getDb } = require("./db");
const { getRuntimeByPhoneNumberId, getRuntimeByInstagramAccountId, findAnyByVerifyToken, upsertTenantChannel } = require("./tenant_runtime");
const TENANT_ID = (process.env.TENANT_ID || "").trim();
// ================== Debounce de textos (por tenant+canal+waId+convId) ==================
const pendingTextBatches = new Map();
const sleep = (ms) => new Promise(r => setTimeout(r, ms));


// ================== Coordinación de corridas por conversación ==================
// Evita respuestas duplicadas cuando entran múltiples webhooks muy juntos
// para el mismo tenant/canal/conversación.
// - Serializa el procesamiento por conversación.
// - Permite descartar resultados viejos si una corrida más nueva ya tomó prioridad.
const processingRunState = new Map();

function buildProcessingRunKey(tenantId, phoneNumberId, convId, waId) {
  return convId
    ? `${String(tenantId || "").trim()}:${String(phoneNumberId || "env").trim()}:${String(convId)}`
    : `${String(tenantId || "").trim()}:${String(phoneNumberId || "env").trim()}:${String(waId || "").trim()}`;
}

async function withConversationRunLock(runKey, job) {
  const key = String(runKey || "").trim();
  if (!key) return job({ runSeq: 0, isStale: () => false });

  let state = processingRunState.get(key);
  if (!state) {
    state = { seq: 0, tail: Promise.resolve() };
    processingRunState.set(key, state);
  }

  const mySeq = ++state.seq;
  const prevTail = state.tail;
  let release = null;
  const myTail = new Promise((resolve) => { release = resolve; });
  state.tail = myTail;

  await prevTail.catch(() => {});

  const isStale = () => {
    const current = processingRunState.get(key);
    return !current || current.seq !== mySeq;
  };

  try {
    return await job({ runSeq: mySeq, isStale });
  } finally {
    try { if (typeof release === "function") release(); } catch {}
    const current = processingRunState.get(key);
    if (current && current.seq === mySeq && current.tail === myTail) {
      processingRunState.delete(key);
    }
  }
}

function clampInt(v, min, max) {
  const n = Number.parseInt(v, 10);
  if (Number.isNaN(n)) return min;
  return Math.max(min, Math.min(max, n));
}

// Días de la semana para configuración de horarios
const STORE_HOURS_DAYS = [
  { key: "monday", label: "Lunes" },
  { key: "tuesday", label: "Martes" },
  { key: "wednesday", label: "Miércoles" },
  { key: "thursday", label: "Jueves" },
  { key: "friday", label: "Viernes" },
  { key: "saturday", label: "Sábado" },
  { key: "sunday", label: "Domingo" },
];
// ⬇️ Para páginas y formularios simples (admin)
const path = require("path");

// ⬇️ Auth UI (login + sesiones + admin usuarios)
const auth = require("./auth_ui");
// Servir assets estáticos locales (logo.png)
// Servir assets estáticos:
// 1) Logos del slider en /static/clientes -> <proyecto>/static/clientes
app.use("/static/clientes", express.static(path.join(__dirname, "static", "clientes")));
// 2) Mantener compatibilidad con /static/logo.png si está en la raíz del proyecto
app.use("/static", express.static(path.join(__dirname)));
// Necesario para formularios HTML (login / admin users)
app.use(express.urlencoded({ extended: true }));
// Adjunta req.user desde cookie de sesión (si existe)
app.use(auth.attachUser);
// Rutas: /login, /logout, /app, /admin/users...
auth.mountAuthRoutes(app);
// Asegura que el shell /ui quede protegido (aunque cambie protectRoutes)
app.use("/ui", auth.requireAuth);
// Protege rutas sensibles (admin/api/ui) detrás de login
auth.protectRoutes(app);


// ===================== Tenant Channels (WhatsApp/OpenAI por tenant/canal) =====================
// Permite definir por tenant (y por teléfono) los valores que antes estaban en .env:
// - phoneNumberId, whatsappToken, verifyToken, openaiApiKey
// Nota: el webhook usa esta colección para enrutar multi-teléfono.
//
// Requiere rol admin.
app.get("/api/tenant-channels", auth.requireAdmin, async (req, res) => {
  try {
    const db = await getDb();
    const tenant = String(req.query.tenantId || resolveTenantId(req) || "").trim();

    const q = {};
    if (tenant) q.tenantId = tenant;

    const rows = await db.collection("tenant_channels").find(q).sort({ updatedAt: -1, createdAt: -1 }).toArray();

    // Por seguridad, si NO sos superadmin, enmascaramos secretos al listar
    const isSuper = String(req.user?.role || "").toLowerCase() === "superadmin";

    const safe = rows.map(r => ({
      _id: String(r._id),
      tenantId: r.tenantId || null,
      channelType: r.channelType || "whatsapp",
      phoneNumberId: r.phoneNumberId || null,
      displayPhoneNumber: r.displayPhoneNumber || null,
      instagramAccountId: r.instagramAccountId || null,
      instagramPageId: r.instagramPageId || null,
      isDefault: !!r.isDefault,
      messageDebounceMs: r.messageDebounceMs ?? 0,
      updatedAt: r.updatedAt || null,
      createdAt: r.createdAt || null,
      whatsappToken: isSuper ? (r.whatsappToken || null) : (r.whatsappToken ? "********" : null),
      instagramAccessToken: isSuper ? (r.instagramAccessToken || null) : (r.instagramAccessToken ? "********" : null),
      verifyToken: isSuper ? (r.verifyToken || null) : (r.verifyToken ? "********" : null),
      openaiApiKey: isSuper ? (r.openaiApiKey || null) : (r.openaiApiKey ? "********" : null),
    }));

    res.json({ ok: true, tenant: tenant || null, items: safe });
  } catch (e) {
    console.error("GET /api/tenant-channels error:", e?.message || e);
    res.status(500).json({ error: "internal" });
  }
});

app.post("/api/tenant-channels", auth.requireAdmin, async (req, res) => {
  try {
    const body = req.body || {};
    // Si NO es superadmin, forzamos tenantId al del usuario (no puede escribir otros tenants)
    const isSuper = String(req.user?.role || "").toLowerCase() === "superadmin";
    const tenantForced = resolveTenantId(req);

    const payload = {
      tenantId: isSuper ? (body.tenantId || tenantForced) : tenantForced,
      phoneNumberId: body.phoneNumberId,
      displayPhoneNumber: body.displayPhoneNumber,
      // isDefault puede venir como "1" / "true" / "on"
      isDefault:
        body.isDefault === "1" ||
        body.isDefault === "true" ||
        body.isDefault === "on" ||
        body.isDefault === 1 ||
        body.isDefault === true
          ? true
          : undefined,
      whatsappToken: body.whatsappToken,
      verifyToken: body.verifyToken,
      openaiApiKey: body.openaiApiKey,
      messageDebounceMs: clampInt(req.body?.messageDebounceMs ?? 0, 0, 5000),
    };

    const r = await upsertTenantChannel(payload, { allowSecrets: true });

    // Si se marcó default, desmarcar cualquier otro canal default del mismo tenant
    if (payload.isDefault === true) {
      const db = await getDb();
      await db.collection("tenant_channels").updateMany(
        { tenantId: String(payload.tenantId), phoneNumberId: { $ne: String(payload.phoneNumberId) } },
        { $set: { isDefault: false, updatedAt: new Date() } }
      );
    }
    res.json({ ok: true, result: r });
  } catch (e) {
    console.error("POST /api/tenant-channels error:", e?.message || e);
    res.status(400).json({ error: e?.message || "bad_request" });
  }
});


// ===================== WhatsApp Web Sessions Control (WWeb) =====================
// Admin API para controlar políticas (allow any / pinned host / blocked hosts),
// ver estado de locks y leer historial.
// Requiere sesión (login) y rol admin.
app.use("/api/wweb", auth.requireAdmin);

async function wwebCollections(db) {
  return {
    locks: db.collection("wa_locks"),
    policies: db.collection("wa_wweb_policies"),
    history: db.collection("wa_wweb_history"),
    actions: db.collection("wa_wweb_actions"),
  };
}

function wwebLockId(tenantId, numero) {
  return `${String(tenantId || "").trim()}:${String(numero || "").trim()}`;
}

function wwebHostFromReq(req) {
  // quien hace el request (admin panel)
  return String(req.headers["x-hostname"] || req.headers["x-pc-name"] || req.ip || "").trim();
}

async function wwebLog(db, entry) {
  try {
    const { history } = await wwebCollections(db);
    await history.insertOne({
      lockId: entry.lockId || null,
      tenantId: entry.tenantId || null,
      numero: entry.numero || null,
      event: String(entry.event || "event"),
      host: entry.host || null,
      by: entry.by || (req?.user?.email || req?.user?.username || "admin"), // best-effort
      detail: entry.detail || null,
      at: new Date(),
    });
  } catch {}
}

// GET /api/wweb/sessions  -> lista estado (locks + policies) para el panel
app.get("/api/wweb/sessions", async (req, res) => {
  try {
    const tenantId = resolveTenantId(req);
    const db = await getDb();
    const { locks, policies } = await wwebCollections(db);

    const [lockDocs, policyDocs] = await Promise.all([
      locks.find(tenantId ? { _id: new RegExp("^" + tenantId + ":") } : {}).toArray(),
      policies.find(tenantId ? { tenantId } : {}).toArray(),
    ]);

    const policyById = new Map(policyDocs.map(p => [p._id, p]));
    const sessions = lockDocs.map(l => {
      const [tid, num] = String(l._id || "").split(":");
      const pol = policyById.get(l._id) || null;
      return {
        lockId: l._id,
        tenantId: tid,
        numero: num,
        state: l.state || null,
        holderId: l.holderId || null,
        host: l.host || l.hostname || null,
        pid: l.pid || null,
        lastSeenAt: l.lastSeenAt || l.updatedAt || null,
        policy: pol ? {
          mode: pol.mode || "any",
          pinnedHost: pol.pinnedHost || null,
          blockedHosts: Array.isArray(pol.blockedHosts) ? pol.blockedHosts : [],
          updatedAt: pol.updatedAt || null,
          updatedBy: pol.updatedBy || null,
        } : { mode: "any", pinnedHost: null, blockedHosts: [] },
      };
    });

    // incluir policies sin lock (por si querés preconfigurar)
    for (const p of policyDocs) {
      if (sessions.some(s => s.lockId === p._id)) continue;
      sessions.push({
        lockId: p._id,
        tenantId: p.tenantId,
        numero: p.numero,
        state: null,
        holderId: null,
        host: null,
        pid: null,
        lastSeenAt: null,
        policy: {
          mode: p.mode || "any",
          pinnedHost: p.pinnedHost || null,
          blockedHosts: Array.isArray(p.blockedHosts) ? p.blockedHosts : [],
          updatedAt: p.updatedAt || null,
          updatedBy: p.updatedBy || null,
        },
      });
    }

    sessions.sort((a,b)=> String(a.lockId).localeCompare(String(b.lockId)));
    return res.json({ ok: true, sessions });
  } catch (e) {
    return res.status(500).json({ ok:false, error: String(e?.message || e) });
  }
});

// POST /api/wweb/policy
// body: { tenantId?, numero, mode: "any"|"pinned", pinnedHost?, blockHost?, unblockHost? }
app.post("/api/wweb/policy", async (req, res) => {
  try {
    const tenantId = String(req.body?.tenantId || resolveTenantId(req) || "").trim();
    const numero = String(req.body?.numero || "").trim();
    if (!tenantId || !numero) return res.status(400).json({ ok:false, error:"tenantId_numero_required" });

    const id = wwebLockId(tenantId, numero);
    const mode = String(req.body?.mode || "").trim().toLowerCase();
    const pinnedHost = String(req.body?.pinnedHost || "").trim();
    const blockHost = String(req.body?.blockHost || "").trim();
    const unblockHost = String(req.body?.unblockHost || "").trim();

    const db = await getDb();
    const { policies, history } = await wwebCollections(db);

    const update = { $setOnInsert: { _id: id, tenantId, numero }, $set: { updatedAt: new Date(), updatedBy: (req.user?.email || req.user?.username || "admin") } };
    if (mode === "any" || mode === "pinned") update.$set.mode = mode;
    if (mode === "pinned") update.$set.pinnedHost = pinnedHost || wwebHostFromReq(req) || null;
    if (mode === "any") update.$set.pinnedHost = null;

    if (blockHost) update.$addToSet = { ...(update.$addToSet||{}), blockedHosts: blockHost };
    if (unblockHost) update.$pull = { ...(update.$pull||{}), blockedHosts: unblockHost };

    const r = await policies.updateOne({ _id: id }, update, { upsert: true });

    await history.insertOne({
      lockId: id, tenantId, numero,
      event: "policy_update",
      host: wwebHostFromReq(req) || null,
      by: (req.user?.email || req.user?.username || "admin"),
      detail: { mode: update.$set.mode, pinnedHost: update.$set.pinnedHost, blockHost, unblockHost },
      at: new Date(),
    });

    return res.json({ ok:true, lockId: id, modified: r.modifiedCount, upserted: !!r.upsertedId });
  } catch (e) {
    return res.status(500).json({ ok:false, error: String(e?.message || e) });
  }
});

// POST /api/wweb/release  body: { tenantId?, numero }
app.post("/api/wweb/release", async (req, res) => {
  try {
    const tenantId = String(req.body?.tenantId || resolveTenantId(req) || "").trim();
    const numero = String(req.body?.numero || "").trim();
    if (!tenantId || !numero) return res.status(400).json({ ok:false, error:"tenantId_numero_required" });

    const id = wwebLockId(tenantId, numero);
    const db = await getDb();
    const { locks, actions, history } = await wwebCollections(db);

    // borrar lock para que otra PC pueda tomarlo
    await locks.deleteOne({ _id: id });

    // opcional: encolar acción para que el script (si lo implementaste) la procese
    await actions.insertOne({ lockId: id, tenantId, numero, action: "release", requestedBy: (req.user?.email || req.user?.username || "admin"), at: new Date() });

    await history.insertOne({ lockId:id, tenantId, numero, event:"release_requested", host: wwebHostFromReq(req)||null, by:(req.user?.email||req.user?.username||"admin"), detail:null, at:new Date() });
    return res.json({ ok:true, lockId:id });
  } catch (e) {
    return res.status(500).json({ ok:false, error: String(e?.message || e) });
  }
});

// GET /api/wweb/history?tenantId=&numero=&limit=100
app.get("/api/wweb/history", async (req, res) => {
  try {
    const tenantId = String(req.query?.tenantId || "").trim();
    const numero = String(req.query?.numero || "").trim();
    const limit = Math.min(200, Math.max(1, Number(req.query?.limit || 100)));

    const db = await getDb();
    const { history } = await wwebCollections(db);

    const q = {};
    if (tenantId) q.tenantId = tenantId;
    if (numero) q.numero = numero;

    const items = await history.find(q).sort({ at: -1 }).limit(limit).toArray();
    return res.json({ ok:true, items });
  } catch (e) {
    return res.status(500).json({ ok:false, error: String(e?.message || e) });
  }
});


const {
  loadBehaviorTextFromMongo,
  loadBehaviorConfigFromMongo,
  invalidateBehaviorCache,
  getGPTReply, hasActiveEndedFlag, markSessionEnded, isPoliteClosingMessage,
  syncSessionConversation,
  START_FALLBACK, buildBackendSummary, coalesceResponse, recalcAndDetectMismatch,
  hydratePricesFromCatalog,
  putInCache, getFromCache, getMediaInfo, downloadMediaBuffer, transcribeAudioExternal,
  DEFAULT_TENANT_ID, setAssistantPedidoSnapshot, replaceLastAssistantHistory, calcularDistanciaKm,
  geocodeAddress, reverseGeocode, getStoreCoords, pickEnvioProductByDistance,clearEndedFlag,analyzeImageExternal,
  ensureEnvioSmart,hasContext,
} = require("./logic");

app.use(express.json({ verify: (req, res, buf) => { req.rawBody = buf; } }));

function isValidSignature(req) {
  const appSecret = process.env.WHATSAPP_APP_SECRET;
  const signature = req.get("X-Hub-Signature-256");
  if (!appSecret || !signature) return false;
  const hmac = crypto.createHmac("sha256", appSecret);
  hmac.update(req.rawBody);
  const expected = "sha256=" + hmac.digest("hex");
  try { return crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected)); } catch { return false; }
}

function resolveTenantId(req) {
  return auth.resolveTenantId(req, { defaultTenantId: DEFAULT_TENANT_ID, envTenantId: process.env.TENANT_ID });
}

// Health
// Root: en Render queremos que abra la UI (/app) en lugar de responder "OK".
// Conserva querystring si lo hubiera.
// ===================== SEO (landing pública) =====================
// Evitamos que "/" redirija a /app (privado). Si los crawlers caen en login,
// suelen mostrar "no se puede mostrar la descripción". Esta landing es indexable
// y define description + og:image (logo) para snippets.
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || "https://www.asistobot.com.ar").trim();
function absUrl(p = "/") {
  const base = String(PUBLIC_BASE_URL || "").replace(/\/+$/g, "");
  const path = String(p || "/").startsWith("/") ? String(p || "/") : `/${p}`;
  return base ? (base + path) : path;
}

app.get("/", (req, res) => {
  const qs = req.originalUrl && req.originalUrl.includes("?")
    ? req.originalUrl.slice(req.originalUrl.indexOf("?"))
    : "";
  return res.redirect(302, "/login" + qs);
});

// robots + sitemap para ayudar a indexación/snippet
app.get("/robots.txt", (_req, res) => {
  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  return res.status(200).send([
    "User-agent: *",
    "Allow: /",
    `Sitemap: ${absUrl("/sitemap.xml")}`,
    ""
  ].join("\n"));
});

app.get("/sitemap.xml", (_req, res) => {
  res.setHeader("Content-Type", "application/xml; charset=utf-8");
  const urls = [
    { loc: absUrl("/"), priority: "1.0" },
    { loc: absUrl("/login"), priority: "0.3" }
  ];
  const body =
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n` +
    urls.map(u => `  <url><loc>${u.loc}</loc><priority>${u.priority}</priority></url>`).join("\n") +
    `\n</urlset>\n`;
  return res.status(200).send(body);
});


app.get("/healthz", (_req, res) => res.json({ ok: true }));

// Cache público (audio)
function sendCacheItem(res, item) {
  if (!item) return res.status(404).send("Not found");
  res.setHeader("Content-Type", item.mime || "application/octet-stream");
  res.setHeader("Cache-Control", "no-store");
  return res.send(item.buffer);
}

// Backward compatible (audio)
app.get("/cache/audio/:id", (req, res) => {
  const item = getFromCache(req.params.id);
 return sendCacheItem(res, item);
});

// Nuevo: cache genérico para media (imágenes / audio / etc.)
app.get("/cache/media/:id", (req, res) => {
  const item = getFromCache(req.params.id);
  return sendCacheItem(res, item);
});

// ===================================================================
// ===============       Catálogo de productos        ================
// ===================================================================

// ---------- LOGS: helpers ----------
const withTenant = (q = {}, tenantId) => {
  const out = { ...q };
  const tid = (tenantId || TENANT_ID || "").trim();
  if (tid) out.tenantId = tid;
  return out;
};

// Normaliza el estado para mostrarlo en /admin (ej: CANCELLED -> CANCELADA).
// Importante: si el registro ya trae un status explícito, ese status debe
// prevalecer (aunque finalized=true), porque puede ser una conversación
// finalizada por cancelación del usuario.
function adminStatusLabel(conv) {
  const raw = String(conv?.status || "").trim();
  const up = raw.toUpperCase();

  // Cancelaciones (aceptamos variantes)
  if (up === "CANCELLED" || up === "CANCELED" || up === "CANCELADA" || up === "CANCELADO") {
    return "CANCELADA";
  }

  // Si hay status persistido, lo devolvemos tal cual (en mayúsculas típicas)
  if (raw) return up;

  // Fallback si no hay status explícito
  return conv?.finalized ? "COMPLETED" : "OPEN";
}
// Parseo tolerante de filtro "entregado":
// - true  => entregadas
// - false => NO entregadas
// - null  => todas
function parseDeliveredFilter(raw) {
  const v = String(raw ?? "").trim().toLowerCase();
  if (!v) return null;
  if (["1", "true", "yes", "si", "sí", "entregado", "entregada", "delivered"].includes(v)) return true;
  if (["0", "false", "no", "pendiente", "pendientes", "noentregado", "no_entregado", "not_delivered"].includes(v)) return false;
  return null;
}

// ===== Orden "próximos a entregar" (AR -03:00) =====
function _toMs(v) {
  const ms = Date.parse(v);
  return Number.isFinite(ms) ? ms : 0;
}

// Convierte fechaEntrega (YYYY-MM-DD) + horaEntrega (HH:MM) a ms.
// Si falta hora, usamos 23:59 para que quede "más tarde" dentro del mismo día.
function pedidoEntregaMs(fechaEntrega, horaEntrega) {
  const f = String(fechaEntrega || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(f)) return null;
  const h = /^\d{2}:\d{2}$/.test(String(horaEntrega || "").trim())
    ? String(horaEntrega).trim()
   : "23:59";
  // Argentina (Córdoba): -03:00
  const iso = `${f}T${h}:00-03:00`;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : null;
}

function compareConvsForEntrega(a, b) {
  const aHasPedido = a?.fechaEntrega && a.fechaEntrega !== "-";
  const bHasPedido = b?.fechaEntrega && b.fechaEntrega !== "-";

  const aPending = aHasPedido && a?.delivered !== true;
  const bPending = bHasPedido && b?.delivered !== true;

  // 1) Pendientes arriba
  if (aPending !== bPending) return aPending ? -1 : 1;

  const aPedidoMs = aHasPedido ? pedidoEntregaMs(a.fechaEntrega, a.horaEntrega) : null;
  const bPedidoMs = bHasPedido ? pedidoEntregaMs(b.fechaEntrega, b.horaEntrega) : null;

  // 2) Si ambos pendientes: más próximos primero
  if (aPending && bPending) {
    if (aPedidoMs !== null && bPedidoMs !== null && aPedidoMs !== bPedidoMs) return aPedidoMs - bPedidoMs;
    if (aPedidoMs !== null && bPedidoMs === null) return -1;
    if (aPedidoMs === null && bPedidoMs !== null) return 1;
    return _toMs(b.lastAt) - _toMs(a.lastAt);
  }

  // 3) No pendientes: primero los que tienen pedido
  if (aHasPedido !== bHasPedido) return aHasPedido ? -1 : 1;

  // 4) Entregadas / resto: por fecha/hora del pedido (más reciente primero)
  if (aHasPedido && bHasPedido) {
    const am = aPedidoMs ?? 0, bm = bPedidoMs ?? 0;
    if (am !== bm) return bm - am;
  }

  // 5) Fallback por actividad reciente
  return _toMs(b.lastAt) - _toMs(a.lastAt);
}


async function saveLog(entry) {
  try {
    const db = await getDb();
    const doc = {
      tenantId: (entry?.tenantId || TENANT_ID || DEFAULT_TENANT_ID || null),
      waId: entry.waId || null,
      role: entry.role,             // 'user' | 'assistant' | 'system'
      content: entry.content ?? "",
      payload: entry.payload ?? null, // cualquier JSON extra (Pedido/estado/trace)
    createdAt: new Date()
    };
    await db.collection("logs").insertOne(doc);
  } catch (e) {
    console.error("[logs] saveLog error:", e);
  }
}

 
// ================== Persistencia de conversaciones y mensajes ==================
// ================== Persistencia de conversaciones y mensajes ==================
async function upsertConversation(waId, init = {}, tenantId) {
  const db = await getDb();
  const now = new Date();
  const tenant = String(tenantId || TENANT_ID || DEFAULT_TENANT_ID || "default");

   // 👉 Solo conversaciones abiertas: si la última está finalizada/cancelada, se creará un registro nuevo
  const filter = { waId: String(waId), tenantId: tenant, finalized: { $ne: true }, status: { $nin: ["COMPLETED", "CANCELLED"] } };
  const update = {
    $setOnInsert: { createdAt: now, openedAt: now, status: "OPEN", manualOpen: false },
   
    $set: { updatedAt: now, ...init },
  };
  // Para driver moderno
  const opts = { upsert: true, returnDocument: "after" };
  // Si tu driver es 4.x antiguo, usar en su lugar: { upsert:true, returnOriginal:false }

  const res = await db.collection("conversations").findOneAndUpdate(filter, update, opts);
  if (res && res.value) return res.value;
  // (Quitar fallback insertOne para evitar duplicados en condiciones de carrera)
  return await db.collection("conversations").findOne(filter);
}
// ------- helper para cerrar conversación (finalizar/cancelar) -------
async function closeConversation(convId, status = "COMPLETED", extra = {}) {
  try {
    const db = await getDb();
    const now = new Date();
    await db.collection("conversations").updateOne(
      { _id: new ObjectId(String(convId)) },
      { $set: { finalized: true, status, closedAt: now, updatedAt: now, ...extra } }
    );
  } catch (e) {
    console.error("closeConversation error:", e?.message || e);
  }
}

// ------- helper para validar que el pedido esté completo antes de cerrar -------
function isPedidoCompleto(p) {
  try {
    if (!p) return false;
    const pedido = normalizePedidoDateTimeFields(cloneJsonSafe(p) || {});
    const itemsOk   = Array.isArray(pedido.items) && pedido.items.length > 0;
    const entregaOk = pedido.Entrega === 'retiro' || pedido.Entrega === 'domicilio';
    const dirOk     = pedido.Entrega !== 'domicilio'
      || (pedido.Domicilio && pedido.Domicilio.direccion && String(pedido.Domicilio.direccion).trim());
    const fechaOk   = /^\d{4}-\d{2}-\d{2}$/.test(String(pedido.Fecha || ""));
    const horaOk    = /^\d{2}:\d{2}$/.test(String(pedido.Hora || ""));
    return itemsOk && entregaOk && dirOk && fechaOk && horaOk;
  } catch {
    return false;
  }
}

// ------- helper: validación de fecha/hora del pedido contra horarios configurados -------
function _hhmmToMinutes(value) {
  const m = /^([01]\d|2[0-3]):([0-5]\d)$/.exec(String(value || "").trim());
  if (!m) return null;
  return Number(m[1]) * 60 + Number(m[2]);
}

function _weekdayKeyFromISODate(isoDate) {
  try {
    const val = String(isoDate || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(val)) return null;
    // Usamos el mediodía para evitar problemas de timezone
    const d = new Date(`${val}T12:00:00`);
    const idx = d.getDay(); // 0=Domingo ... 6=Sábado
    const map = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"];
    return map[idx] || null;
  } catch {
    return null;
  }
}

/**
 * Valida que Pedido.Fecha / Pedido.Hora caigan dentro de los horarios configurados.
 * @param {Object} pedido   Pedido con { Fecha: 'YYYY-MM-DD', Hora: 'HH:MM', ... }
 * @param {Object} hoursCfg Objeto { monday:[{from,to}], tuesday:[...], ... }
 * @returns {{ok:boolean, reason:string|null, msg:string}}
 */
function validatePedidoSchedule(pedido, hoursCfg) {
  const result = { ok: true, reason: null, msg: "" };
  try {
    if (!pedido || !hoursCfg) return result;

    const p = normalizePedidoDateTimeFields(cloneJsonSafe(pedido) || {});
    const fecha = String(p.Fecha || "").trim();
    const hora  = String(p.Hora || "").trim();

    // Si el formato no es el esperado, no forzamos nada (lo valida la lógica actual)
    if (!/^\d{4}-\d{2}-\d{2}$/.test(fecha)) return result;
    if (!/^([01]\d|2[0-3]):[0-5]\d$/.test(hora)) return result;

    const dayKey = _weekdayKeyFromISODate(fecha);
    if (!dayKey) return result;

    const ranges = Array.isArray(hoursCfg[dayKey]) ? hoursCfg[dayKey] : [];

    // Día sin horarios configurados => consideramos cerrado
    if (!ranges.length) {
      const dayLabel =
        (STORE_HOURS_DAYS.find(d => d.key === dayKey)?.label) || "ese día";

      // Armamos una línea con todos los horarios configurados para informar bien
      const parts = [];
      for (const d of STORE_HOURS_DAYS) {
        const rs = Array.isArray(hoursCfg[d.key]) ? hoursCfg[d.key] : [];
        if (!rs.length) continue;
        const slots = rs.map(r => `${r.from} a ${r.to}`).join(" y ");
        parts.push(`${d.label}: ${slots}`);
      }
      const allLabel = parts.length ? parts.join(" | ") : "";

      result.ok = false;
      result.reason = "day_closed";
      result.msg = allLabel
        ? `Para ${dayLabel} no tenemos horarios disponibles para recibir pedidos.\n\nNuestros horarios configurados son:\n${allLabel}.\n\n¿Querés elegir otro día y horario dentro de esas franjas?`
        : `Para ${dayLabel} no tenemos horarios disponibles para recibir pedidos. ¿Querés elegir otro día en el que estemos abiertos?`;
      return result;
    }

    const minutes = _hhmmToMinutes(hora);
    if (minutes == null) return result;

    const inside = ranges.some(r => {
      const fromM = _hhmmToMinutes(r.from);
      const toM   = _hhmmToMinutes(r.to);
      if (fromM == null || toM == null) return false;
      return minutes >= fromM && minutes <= toM;
    });

    if (!inside) {
      const dayLabel =
        (STORE_HOURS_DAYS.find(d => d.key === dayKey)?.label) || "ese día";
      const slots = ranges.map(r => `${r.from} a ${r.to}`).join(" y ");

      result.ok = false;
      result.reason = "time_outside_ranges";
      result.msg =
        `En ${dayLabel} no tomamos pedidos para las ${hora}. ` +
        `Para ese día nuestro horario de atención es: ${slots}.\n\n` +
        `¿Querés elegir otro horario dentro de esa franja?`;
      return result;
    }

    return result;
  } catch {
    // Ante cualquier error no rompemos el flujo ni sobreescribimos el mensaje
    return result;
  }
}


async function saveMessageDoc({ conversationId, waId, role, content, type = "text", meta = {}, tenantId }) {
  console.log("[messages] entering saveMessageDoc", { conversationId: String(conversationId || ""), role, type, hasMeta: !!meta });
  if (!conversationId) {
    throw new Error("conversationId_missing");
  }
  try {
    const db = await getDb();
    const now = new Date();
    const convObjectId =
      (conversationId && typeof conversationId === "object" && conversationId._bsontype === "ObjectID")
        ? conversationId
        : new ObjectId(String(conversationId));

    const roleStr = String(role);
    const typeStr = String(type || "text");

    // 🔹 Helper: actualizar resumen de Pedido en `conversations` usando el JSON del assistant
    async function persistPedidoResumenFromAssistantJson() {
      if (roleStr !== "assistant") return;
      try {
        const s = String(content || "").trim();
        if (!s.startsWith("{")) return;
        const j = JSON.parse(s);
        if (!j || !j.Pedido || !Array.isArray(j.Pedido.items)) return;
        const pedido = j.Pedido || {};
 
        // Estado del flujo (IN_PROGRESS | COMPLETED | CANCELLED | PENDIENTE)
        const estadoStr = String(j.estado || "").trim().toUpperCase();
        const isFinalEstado = (estadoStr === "COMPLETED" || estadoStr === "CANCELLED");


        // 🧾 Nombre y apellido directo desde el Pedido (toma prioridad sobre heurísticas de texto)
        const nombreFromPedido = String(
          pedido.nombre_apellido || pedido.nombre || ""
        ).trim();

       // 🛠️ Normalizar/Inferir entrega:
       const entregaRaw = String(pedido.Entrega || "").trim().toLowerCase();
        const envioItem = (pedido.items || []).find(i =>
          /env[ií]o/i.test(String(i?.descripcion || ""))
        );

        // ¿Hay dirección en el JSON?
        const hasAddress =
          pedido?.Domicilio &&
          typeof pedido.Domicilio === "object" &&
          Object.values(pedido.Domicilio).some(v => String(v || "").trim() !== "");

        // Si Entrega no es 'domicilio'/'retiro' pero hay dirección o envío → forzar 'domicilio'
        let entrega = (entregaRaw === "domicilio" || entregaRaw === "retiro")
          ? entregaRaw
          : ((hasAddress || !!envioItem) ? "domicilio" : (entregaRaw || ""));

        // Dirección amigable:
        // - Si Domicilio es string → usarlo directo
        // - Si es objeto → usar .direccion / .calle
        // - Si no hay dirección pero sí ítem de envío → usar descripción del envío
        let direccion = "";
        if (typeof pedido.Domicilio === "string") {
          direccion = pedido.Domicilio.trim();
        } else if (pedido.Domicilio && typeof pedido.Domicilio === "object") {
          direccion = String(
            pedido.Domicilio.direccion ||
            pedido.Domicilio.calle ||
            ""
          ).trim();
        }
        if (!direccion && envioItem) {
          direccion = String(envioItem.descripcion || "").trim();
        }

        // Etiqueta de entrega para la tabla: solo "Envío" o "Retiro"
        let entregaLabel;
        if (entrega === "domicilio") {
          entregaLabel = "Envío";
        } else if (entrega === "retiro") {
          entregaLabel = "Retiro";
        } else {
          entregaLabel = "-";
        }

        // Fecha/Hora sólo si vienen en campos normales
        const fechaEntrega = /^\d{4}-\d{2}-\d{2}$/.test(String(pedido.Fecha || "")) ? pedido.Fecha : null;
        const horaEntrega  = /^\d{2}:\d{2}$/.test(String(pedido.Hora  || "")) ? pedido.Hora  : null;

        await db.collection("conversations").updateOne(
          { _id: convObjectId },
          {
            $set: {
              updatedAt: now,
              lastAssistantTs: now,
              pedidoEntrega: entrega || null,
              pedidoEntregaLabel: entregaLabel || null,
              ...(direccion ? { pedidoDireccion: direccion } : {}),
              ...(fechaEntrega ? { pedidoFecha: fechaEntrega } : {}),
              ...(horaEntrega  ? { pedidoHora:  horaEntrega  } : {}),
              ...(nombreFromPedido ? { contactName: nombreFromPedido } : {}),
              ...(estadoStr ? { pedidoEstado: estadoStr } : {}),
              lastPedidoSnapshot: {
                estado: estadoStr || null,
                Pedido: pedido
              },
              lastPedidoSnapshotAt: now,
              ...(isFinalEstado ? { finalized: true, status: estadoStr, closedAt: now } : {})
    
            }
          }
        );
      } catch (e) {
        console.warn("[messages] no se pudo persistir resumen de Pedido en conversations:", e?.message);
      }
    }

    // 🚫 Si es el mensaje JSON del assistant, NO lo guardamos en `messages`,
    //     solo actualizamos el resumen del Pedido en `conversations` y salimos.
    if (roleStr === "assistant" && typeStr === "json") {
      await persistPedidoResumenFromAssistantJson();
      return;
    }

    // Dedupe: WhatsApp puede reintentar el webhook y enviar el mismo mensaje más de una vez.
    // Si viene un id de WhatsApp (meta.raw.id), evitamos insertar duplicados en la misma conversación.
    const effectiveTenantId = (tenantId ?? TENANT_ID ?? null);
    const waMsgId = (meta && meta.raw && meta.raw.id) ? String(meta.raw.id) : "";
    if (waMsgId) {
      const dup = await db.collection("messages").findOne(
        withTenant({ conversationId: convObjectId, "meta.raw.id": waMsgId }, effectiveTenantId),
        { projection: { _id: 1 } }
      );
      if (dup) {
        console.log("[messages] duplicate wa msg ignored:", waMsgId);
        return;
      }
    }


    const doc = {
      tenantId: effectiveTenantId,
      conversationId: convObjectId,
      waId: String(waId || ""),
      role: String(role),
      content: String(content ?? ""),
      type: typeStr,
      meta: { ...meta },
      ts: now,
      createdAt: now
    };
    const ins = await db.collection("messages").insertOne(doc);
    console.log("[messages] inserted:", ins?.insertedId?.toString?.());
    // 🟢 Actualizar conversación: nombre/apellido (desde texto)
    // 1) Intento de extracción de NOMBRE desde cualquier mensaje (user/assistant)
    try {
      const text = String(content || "").trim();
      let nameHit = "";
      // a nombre de <Nombre>
      let m = text.match(/\ba nombre de\s+([a-záéíóúñü][a-záéíóúñü\s.'-]{2,})/i);
      if (m && m[1]) nameHit = m[1].trim();
      // Nombre: <Nombre>
      if (!nameHit) { m = text.match(/\bNombre\s*:\s*([a-záéíóúñü][a-záéíóúñü\s.'-]{2,})/i); if (m && m[1]) nameHit = m[1].trim(); }
      // Gracias, <Nombre>
      if (!nameHit) { m = text.match(/\bgracias[,:\s]+([a-záéíóúñü][a-záéíóúñü\s.'-]{2,})/i); if (m && m[1]) nameHit = m[1].trim(); }
      if (nameHit) {
        await db.collection("conversations").updateOne(
          { _id: convObjectId },
          { $set: { updatedAt: now, contactName: nameHit } }
        );
      }
    } catch (e) {
      console.warn("[messages] nombre no detectado:", e?.message);
    }

    
  } catch (e) {
    console.error("[messages] save error:", e?.stack || e?.message || e);
    throw e;
  }
}

 
function cloneJsonSafe(v) {
  try { return JSON.parse(JSON.stringify(v)); } catch { return v; }
}

function isFilledValue(v) {
  if (v === null || v === undefined) return false;
  if (typeof v === "string") return v.trim() !== "";
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  return true;
}

function normalizeDomicilioObj(v) {
  if (typeof v === "string") return { direccion: v };
  if (v && typeof v === "object") return cloneJsonSafe(v) || {};
  return {};
}

function normalizePedidoDateTimeFields(pedido) {
  const p = (pedido && typeof pedido === "object") ? pedido : {};

  const fechaNew = typeof p.fecha_pedido === "string" ? p.fecha_pedido.trim() : "";
  const fechaOld = typeof p.Fecha === "string" ? p.Fecha.trim() : "";
  const horaNew = typeof p.hora_pedido === "string" ? p.hora_pedido.trim() : "";
  const horaOld = typeof p.Hora === "string" ? p.Hora.trim() : "";

  if (fechaNew) p.Fecha = fechaNew;
  else if (fechaOld) p.fecha_pedido = fechaOld;

  if (horaNew) p.Hora = horaNew;
  else if (horaOld) p.hora_pedido = horaOld;

  return p;
}

function mergePedidoState(prevPedido, nextPedido) {
  const prev = (prevPedido && typeof prevPedido === "object") ? cloneJsonSafe(prevPedido) : {};
  const next = (nextPedido && typeof nextPedido === "object") ? cloneJsonSafe(nextPedido) : {};

  const merged = { ...prev, ...next };
  for (const key of ["nombre_apellido", "Entrega", "Pago", "fecha_pedido", "hora_pedido", "Fecha", "Hora", "distancia_km"]) {
    merged[key] = isFilledValue(next[key]) ? next[key] : prev[key];
  }

  const prevDom = normalizeDomicilioObj(prev.Domicilio);
  const nextDom = normalizeDomicilioObj(next.Domicilio);
  const dom = {};
  for (const key of new Set([...Object.keys(prevDom), ...Object.keys(nextDom)])) {
    dom[key] = isFilledValue(nextDom[key]) ? nextDom[key] : prevDom[key];
  }
  merged.Domicilio = dom;

  if (Array.isArray(next.items) && next.items.length > 0) merged.items = cloneJsonSafe(next.items);
  else if (Array.isArray(prev.items)) merged.items = cloneJsonSafe(prev.items);
  else merged.items = [];

  if (isFilledValue(next.total_pedido)) merged.total_pedido = next.total_pedido;
  else if (isFilledValue(prev.total_pedido)) merged.total_pedido = prev.total_pedido;
  else merged.total_pedido = 0;

  return normalizePedidoDateTimeFields(merged);
}


function pedidoItemsArray(pedido) {
  return Array.isArray(pedido?.items) ? pedido.items : [];
}

function lowerText(value) {
  return String(value || "").trim().toLowerCase();
}

function pedidoEntregaValue(pedido) {
  return lowerText(pedido?.Entrega || "");
}

function pedidoIsDomicilio(pedido) {
  return pedidoEntregaValue(pedido) === "domicilio";
}

function pedidoHasAddress(pedido) {
  const dom = normalizeDomicilioObj(pedido?.Domicilio);
  if (String(dom.direccion || "").trim()) return true;
  if ([dom.calle, dom.numero].filter(Boolean).join(" ").trim()) return true;
  if (String(dom.address || "").trim()) return true;
  const hasCoords = Number.isFinite(Number(dom.lat)) && Number.isFinite(Number(dom.lon));
  return hasCoords;
}

function pedidoHasPago(pedido) {
  return !!String(pedido?.Pago || "").trim();
}

function pedidoHasHora(pedido) {
  const p = normalizePedidoDateTimeFields(cloneJsonSafe(pedido) || {});
  return /^\d{2}:\d{2}$/.test(String(p.hora_pedido || p.Hora || "").trim());
}

function pedidoHasNombreCompleto(pedido) {
  const name = String(pedido?.nombre_apellido || "").trim().replace(/\s+/g, " ");
  return /\S+\s+\S+/.test(name);
}

function pedidoHasAnyItems(pedido) {
  return pedidoItemsArray(pedido).some(it => String(it?.descripcion || "").trim());
}

function pedidoHasChickenMainItem(pedido) {
  return pedidoItemsArray(pedido).some((it) => {
    const id = String(it?.id ?? "").trim();
    const desc = lowerText(it?.descripcion || "");
    return id === "55" || id === "42" || desc === "pollo entero" || desc === "medio pollo";
  });
}

function pedidoHasResolvedMilanesaType(pedido) {
  return pedidoItemsArray(pedido).some((it) => {
    const desc = lowerText(it?.descripcion || "");
    return desc === "milanesas de carne" || desc === "milanesas de pollo";
  });
}

function stripEnvioItemsFromPedido(pedido) {
  if (!pedido || typeof pedido !== "object") return pedido;
  if (Array.isArray(pedido.items)) {
    pedido.items = pedido.items.filter((it) => !/env[ií]o/i.test(String(it?.descripcion || "")));
  }
  return pedido;
}

function asksChickenCondimentQuestion(text) {
  const s = lowerText(text);
  return /lim[oó]n,?\s*chimichurri\s*o\s*solo/.test(s)
    || /lo quer[eé]s con lim[oó]n/.test(s)
    || /quer[eé]s.*chimichurri.*solo/.test(s);
}

function asksMilanesaTypeQuestion(text) {
  const s = lowerText(text);
  return /milanesas?.*de carne o de pollo/.test(s)
    || /milanesas?.*carne.*pollo/.test(s)
    || /la milanesa la quer[eé]s de carne o de pollo/.test(s);
}

function asksPaymentQuestion(text) {
  return /efectivo o por transferencia/i.test(String(text || ""));
}

function isExplicitUserCancellation(text) {
  const raw = String(text || "").trim();
  if (!raw) return false;
  const norm = raw
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

  const wantsCancelRaw =
    /\b(cancel|anul)(ar|o|a|e|en|ado|ada|alo|ala)?\b/.test(norm) ||
    /\bdar de baja\b/.test(norm) ||
    /\bsuspende\b/.test(norm);

  const cancelNeg =
    /\bno\s+(quiero\s+)?cancel/.test(norm) ||
    /\bno\s+(quiero\s+)?anul/.test(norm);

  return !!(wantsCancelRaw && !cancelNeg);
}

function looksLikeSummaryOrConfirmation(text) {
  const s = String(text || "");
  return /¿\s*confirm[aá]s?\??/i.test(s)
    || /resumen de tu pedido/i.test(s)
    || /hora de entrega:/i.test(s)
    || /modalidad:/i.test(s)
    || /\*productos:\*/i.test(s);
}

function isExplicitUserConfirmation(text) {
  const raw = String(text || "").trim();
  if (!raw) return false;

  const norm = raw
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

  // confirmaciones verbales explícitas
  if (/\bconfirm(ar|o|a|ame|alo|ado)\b/.test(norm)) return true;
  if (/\b(esta\s+bien|perfecto|joya|okey|okay|ok|dale|listo|de una|mandale|obvio)\b/.test(norm)) return true;

  // variantes de "sí": si, sí, sisi, siii, sip, sep, sisa, etc.
  // exige que el mensaje arranque con esa confirmación
  // para no confundir frases largas ambiguas.
  if (/^(s[i1]+|s[i1]+s[i1]+|sip+|sep+)\b/.test(norm)) return true;

  // emojis de confirmación
  if (["👍", "👌", "✅", "✔️", "☑️"].includes(raw)) return true;

  return false;
}


function nextRequiredQuestionFromPedido(pedido) {
  const p = normalizePedidoDateTimeFields(cloneJsonSafe(pedido) || {});
  if (!pedidoHasAnyItems(p)) return "¿Qué te gustaría pedir? 😊";
  if (!pedidoEntregaValue(p)) return "¿Lo retirás o te lo enviamos? 😊";
  if (pedidoIsDomicilio(p) && !pedidoHasAddress(p)) return "¿A qué dirección te lo enviamos? 😊";
  if (pedidoIsDomicilio(p) && pedidoHasAddress(p) && !pedidoHasPago(p)) return "¿Vas a pagar en efectivo o por transferencia? 😊";
  if (!pedidoHasHora(p)) return "¿Para qué hora te gustaría hacer el pedido? 😊";
  if (!pedidoHasNombreCompleto(p)) return "¿A nombre de quién hacemos el pedido? 😊";
  return "Perfecto 😊 ¿Querés confirmar o cambiar algo?";
}

function pedidoHasRequiredFieldsForClose(pedido) {
  const p = normalizePedidoDateTimeFields(cloneJsonSafe(pedido) || {});
  if (!pedidoHasAnyItems(p)) return false;
  if (!pedidoEntregaValue(p)) return false;
  if (pedidoIsDomicilio(p) && !pedidoHasAddress(p)) return false;
  if (pedidoIsDomicilio(p) && !pedidoHasPago(p)) return false;
  if (!pedidoHasHora(p)) return false;
  if (!pedidoHasNombreCompleto(p)) return false;
  return true;
}

function applyCriticalPedidoGuards({ pedido, responseText, estado, currentText }) {
  let nextPedido = normalizePedidoDateTimeFields(cloneJsonSafe(pedido) || { Entrega: "", Domicilio: {}, items: [], total_pedido: 0 });
  let nextResponse = String(responseText || "").trim();
  let nextEstado = String(estado || "IN_PROGRESS").trim() || "IN_PROGRESS";
  const guardHits = [];

  const userCancelled = isExplicitUserCancellation(currentText);
  const assistantCancelled =
    nextEstado.toUpperCase() === "CANCELLED" ||
    /\bpedido cancelado\b/i.test(nextResponse);

  // ✅ Si el usuario canceló explícitamente o el modelo ya devolvió CANCELLED,
  // NO ejecutar guardas de flujo normal (pago, dirección, resumen, etc.).
  if (userCancelled || assistantCancelled) {
    nextEstado = "CANCELLED";
    if (!nextResponse) nextResponse = "Pedido cancelado ✅";
    return { pedido: nextPedido, responseText: nextResponse, estado: nextEstado, guardHits };
  }
  if (pedidoIsDomicilio(nextPedido) && !pedidoHasAddress(nextPedido)) {
    nextEstado = "IN_PROGRESS";
    nextPedido.distancia_km = null;
    stripEnvioItemsFromPedido(nextPedido);
    try {
      const { pedidoCorr } = recalcAndDetectMismatch(nextPedido);
      nextPedido = pedidoCorr;
    } catch {}
    nextResponse = "¿A qué dirección te lo enviamos? 😊";
    guardHits.push("delivery_address_required");
  }

  if (asksPaymentQuestion(nextResponse) && !(pedidoIsDomicilio(nextPedido) && pedidoHasAddress(nextPedido))) {
    nextEstado = "IN_PROGRESS";
    nextResponse = nextRequiredQuestionFromPedido(nextPedido);
    guardHits.push("payment_only_after_address");
  }

  if (pedidoIsDomicilio(nextPedido) && pedidoHasAddress(nextPedido) && !pedidoHasPago(nextPedido)) {
    if (!asksPaymentQuestion(nextResponse)) {
      nextEstado = "IN_PROGRESS";
      nextResponse = "¿Vas a pagar en efectivo o por transferencia? 😊";
      guardHits.push("force_payment_question");
    }
  }

  if (asksChickenCondimentQuestion(nextResponse) && !pedidoHasChickenMainItem(nextPedido)) {
    nextEstado = "IN_PROGRESS";
    nextResponse = nextRequiredQuestionFromPedido(nextPedido);
    guardHits.push("invalid_chicken_condiment_question");
  }

  if (asksMilanesaTypeQuestion(nextResponse) && pedidoHasResolvedMilanesaType(nextPedido)) {
    nextEstado = "IN_PROGRESS";
    nextResponse = nextRequiredQuestionFromPedido(nextPedido);
    guardHits.push("resolved_milanesa_type_no_reask");
  }

  if ((looksLikeSummaryOrConfirmation(nextResponse) || /^(COMPLETED|PENDIENTE)$/i.test(nextEstado)) && !pedidoHasRequiredFieldsForClose(nextPedido)) {
    nextEstado = "IN_PROGRESS";
    nextResponse = nextRequiredQuestionFromPedido(nextPedido);
    guardHits.push("summary_blocked_missing_required_fields");
  }

  return { pedido: nextPedido, responseText: nextResponse, estado: nextEstado, guardHits };
}

async function loadLastPedidoSnapshot(tenantId, conversationId) {
  try {
    if (!conversationId) return null;
    const db = await getDb();
    const convObjectId = new ObjectId(String(conversationId));

    const conv = await db.collection("conversations").findOne(
      withTenant({ _id: convObjectId }, tenantId),
      { projection: { lastPedidoSnapshot: 1 } }
    );
    const convSnap = conv?.lastPedidoSnapshot?.Pedido;
    if (convSnap && typeof convSnap === "object") {
      return cloneJsonSafe(convSnap) || convSnap;
    }

    const doc = await db.collection("messages").findOne(
      withTenant({
        conversationId: convObjectId,
        role: "assistant",
        type: "json",
        "meta.kind": "pedido-snapshot"
      }, tenantId),
      { sort: { ts: -1, createdAt: -1, _id: -1 }, projection: { content: 1 } }
    );
    if (!doc?.content) return null;
    const parsed = JSON.parse(String(doc.content));
    return (parsed?.Pedido && typeof parsed.Pedido === "object") ? parsed.Pedido : null;
  } catch (e) {
    console.warn("[snapshot] loadLastPedidoSnapshot error:", e?.message || e);
    return null;
  }
}

function _pedidoItemsToDisplayLines(pedido) {
  const items = Array.isArray(pedido?.items) ? pedido.items : [];
  return items
    .map((it) => {
      const desc = String(it?.descripcion || "").trim();
      if (!desc) return "";
      const qty = Number(it?.cantidad || 0);
      const qtyText = Number.isFinite(qty) && qty > 0 ? String(qty).replace(/\.0+$/, "") + " x " : "";
      return (qtyText + desc).trim();
    })
    .filter(Boolean);
}

function _safePedidoItemsForAdmin(pedido) {
  const items = Array.isArray(pedido?.items) ? pedido.items : [];
  return items
    .map((it) => {
      const desc = String(it?.descripcion || it?.nombre || "").trim();
      if (!desc) return null;
      const qtyNum = Number(it?.cantidad);
      const unitNum = Number(it?.importe_unitario);
      const totalNum = Number(it?.total);
      return {
        id: it?.id ?? "",
        descripcion: desc,
        cantidad: Number.isFinite(qtyNum) ? qtyNum : 0,
        importe_unitario: Number.isFinite(unitNum) ? unitNum : 0,
        total: Number.isFinite(totalNum) ? totalNum : 0,
      };
    })
    .filter(Boolean);
}

function _extractPedidoSummaryData(pedido) {
  if (!pedido || typeof pedido !== "object") {
    return { entregaLabel: "-", fechaEntrega: "-", horaEntrega: "-", direccion: "-" };
  }

  pedido = normalizePedidoDateTimeFields(cloneJsonSafe(pedido) || {});
  const items = Array.isArray(pedido.items) ? pedido.items : [];
  const entregaRaw = String(pedido.Entrega || pedido.entrega || "").trim().toLowerCase();
  const envio = items.find(i => /env[ií]o/i.test(String(i?.descripcion || "")));

  let entregaLabel = "-";
  if (entregaRaw === "domicilio") entregaLabel = "Envío";
  else if (entregaRaw === "retiro") entregaLabel = "Retiro";

  let direccion = "";
  const domicilio = pedido.Domicilio ?? pedido.domicilio;
  if (typeof domicilio === "string") {
    direccion = domicilio.trim();
  } else if (domicilio && typeof domicilio === "object") {
    direccion = String(
      domicilio.direccion ||
      domicilio.calle ||
      domicilio.address ||
      ""
    ).trim();
  }
  if (!direccion && envio) {
    direccion = String(envio.descripcion || "").trim();
  }

  const fechaRaw = String(
    pedido.Fecha ||
    pedido.fecha ||
    pedido.fecha_pedido ||
    ""
  ).trim();
  const horaRaw = String(
    pedido.Hora ||
    pedido.hora ||
    pedido.hora_pedido ||
    ""
  ).trim();

  const fechaEntrega = /^\d{4}-\d{2}-\d{2}$/.test(fechaRaw) ? fechaRaw : "-";
  const horaEntrega = /^\d{2}:\d{2}$/.test(horaRaw) ? horaRaw : "-";

  return { entregaLabel, fechaEntrega, horaEntrega, direccion: direccion || "-" };
}

async function _loadLastPedidoForConversation(db, convId, tenantId, orderHint = null) {
  let pedido =
    orderHint && orderHint.pedido && Array.isArray(orderHint.pedido.items)
      ? orderHint.pedido
      : null;

  if (!pedido) {
    try {
      const conv = await db.collection("conversations").findOne(
        withTenant({ _id: new ObjectId(String(convId)) }, tenantId),
        { projection: { lastPedidoSnapshot: 1 } }
      );
      const convSnap = conv?.lastPedidoSnapshot?.Pedido;
      if (convSnap && Array.isArray(convSnap.items)) {
        pedido = convSnap;
      }
    } catch {}
  }

  if (!pedido) {
    const filter = withTenant(
      { conversationId: new ObjectId(String(convId)), role: "assistant" },
      tenantId
    );
    const cursor = db.collection("messages")
      .find(filter)
      .sort({ ts: -1, createdAt: -1 })
      .limit(200);

    for await (const m of cursor) {
      const s = String(m.content || "").trim();
      try {
        const j = JSON.parse(s);
        if (j && j.Pedido && Array.isArray(j.Pedido.items)) {
          pedido = j.Pedido;
          break;
        }
      } catch {}
    }
  }

  return pedido || null;
}

async function _getLastPedidoProducts(db, convId, tenantId, orderHint = null) {
  try {
    const pedido = await _loadLastPedidoForConversation(db, convId, tenantId, orderHint);
    return _pedidoItemsToDisplayLines(pedido);
  } catch {
    return [];
  }
}

// === Helpers para resumen de Pedido (fecha/hora/entrega/envío) ===
async function _getLastPedidoSummary(db, convId, tenantId, orderHint = null) {
 try {
    const pedido = await _loadLastPedidoForConversation(db, convId, tenantId, orderHint);
    if (!pedido) return { entregaLabel: "-", fechaEntrega: "-", horaEntrega: "-", direccion: "-" };
    return _extractPedidoSummaryData(pedido);
  } catch {
    return { entregaLabel: "-", fechaEntrega: "-", horaEntrega: "-", direccion: "-" };
  }
}

// Listado de conversaciones reales (colección `conversations`)
// deliveredFilter:
//   - true  => solo entregadas
//   - false => solo NO entregadas (incluye docs sin el campo)
//   - null  => todas
async function listConversations(limit = 50, tenantId, deliveredFilter = null) {
  const db = await getDb();
  const q = withTenant({}, tenantId);

  // Filtro entregadas/no entregadas
  if (deliveredFilter === true) {
    q.delivered = true;
  } else if (deliveredFilter === false) {
    // incluye docs legacy sin el campo
    q.delivered = { $ne: true };
  }


  // 1) Traer conversaciones
  const rows = await db.collection("conversations")
    .find(q)
    .sort({ updatedAt: -1, closedAt: -1, openedAt: -1 })
    .limit(limit)
    .toArray();

  // 2) Traer órdenes asociadas para obtener distancia_km
  const convIds = rows.map(c => c._id).filter(Boolean);
  const ordersByConvId = new Map();

  if (convIds.length) {
    const cursor = db.collection("orders")
      .find({ conversationId: { $in: convIds } })
      .sort({ createdAt: -1 });

    for await (const ord of cursor) {
      const key = String(ord.conversationId);
      // Nos quedamos con la más reciente por conversación
      if (!ordersByConvId.has(key)) {
        ordersByConvId.set(key, ord);
      }
    }
  }

  // 3) Enriquecer con resumen de Pedido (fecha/hora/entrega/envío + distancia)
  const out = [];
  for (const c of rows) {
    const base = {
      _id: String(c._id),
      waId: c.waId,
      contactName: c.contactName || "-",
      status: adminStatusLabel(c),
      manualOpen: !!c.manualOpen,
      kitchenSent: !!c.kitchenSent,
      kitchenAt: c.kitchenAt || null,
      delivered: !!c.delivered,
      deliveredAt: c.deliveredAt || null,
      lastAt: c.lastUserTs || c.lastAssistantTs || c.updatedAt || c.closedAt || c.openedAt
    };

    const ord = ordersByConvId.get(String(c._id));
    const distanceKm =
      ord && ord.distancia_km !== undefined && ord.distancia_km !== null
        ? ord.distancia_km
        : null;
    const pedido = await _loadLastPedidoForConversation(db, c._id, tenantId, ord);
    const products = _pedidoItemsToDisplayLines(pedido);
    const pedidoItems = _safePedidoItemsForAdmin(pedido);

    // Preferir campos persistidos en conversations; si faltan, fallback al último pedido detectado
    if (c.pedidoEntregaLabel || c.pedidoFecha || c.pedidoHora || c.pedidoEntrega) {
      const extra = {
        entregaLabel:
          c.pedidoEntregaLabel
          || (c.pedidoEntrega === "domicilio"
                ? "Envío"
                : c.pedidoEntrega === "retiro"
                  ? "Retiro"
                  : "-"),
        fechaEntrega: c.pedidoFecha || "-",
        horaEntrega: c.pedidoHora || "-",
        direccion: c.pedidoDireccion || "-",
        ...(distanceKm !== null ? { distanceKm } : {})
      };
      out.push({ ...base, ...extra, products, pedidoItems });
    } else {
      const extra = pedido ? _extractPedidoSummaryData(pedido) : { entregaLabel: "-", fechaEntrega: "-", horaEntrega: "-", direccion: "-" };
      out.push({
        ...base,
        ...extra,
        products,
        pedidoItems,
        ...(distanceKm !== null ? { distanceKm } : {})
      });
    }
  }
 
  // Orden final: pendientes arriba y más próximos a entregar
  out.sort(compareConvsForEntrega);
  return out;
}

// Mensajes por conversación
// Mensajes por conversación (colección `messages`)
async function getConversationMessagesByConvId(convId, limit = 500, tenantId) {
  const db = await getDb();
  const filter = withTenant({ conversationId: new ObjectId(String(convId)) }, tenantId);
  return db.collection("messages")
    .find(filter).sort({ ts: 1, createdAt: 1 }).limit(limit).toArray();
}
async function getConversationMessagesByWaId(waId, limit = 500, tenantId) {
  const db = await getDb();
  const conv = await db.collection("conversations").findOne(
    withTenant({ waId }, tenantId),
    { sort: { updatedAt: -1, openedAt: -1 } }
  );
  if (!conv) return [];
  return getConversationMessagesByConvId(conv._id, limit, tenantId);
}

// ---------- API de logs ----------
// Conversaciones (lista)
app.get("/api/logs/conversations", async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(parseInt(req.query.limit || "50", 10), 500));
    const deliveredFilter = parseDeliveredFilter(req.query.delivered);
    const rows = await listConversations(limit, resolveTenantId(req), deliveredFilter);
  
    res.json(rows);
  } catch (e) {
    console.error("GET /api/logs/conversations error:", e);
    res.status(500).json({ error: "internal" });
  }
});

// Mensajes de una conversación

// ---------- Media proxy (para ver/descargar adjuntos en /admin y /admin/inbox) ----------
function _safeFileName(name) {
  const s = String(name || "").trim() || "archivo";
  // quitar caracteres peligrosos para header
  return s.replace(/[\r\n"]/g, "").slice(0, 180) || "archivo";
}
function _extFromMime(mime) {
  const mt = String(mime || "").toLowerCase();
  if (mt.includes("jpeg")) return "jpg";
  if (mt.includes("png")) return "png";
  if (mt.includes("webp")) return "webp";
  if (mt.includes("gif")) return "gif";
  if (mt.includes("pdf")) return "pdf";
  if (mt.includes("mp3")) return "mp3";
  if (mt.includes("wav")) return "wav";
  if (mt.includes("ogg") || mt.includes("opus")) return "ogg";
  if (mt.includes("mp4")) return "mp4";
  return "";
}
function _appendTenantParam(url, tenantId) {
  const t = String(tenantId || "").trim();
  if (!t) return url;
  const u0 = String(url || "");
  // No tocar URLs absolutas externas
  if (/^https?:\/\//i.test(u0)) return u0;
  try {
    const u = new URL(u0, "http://localhost");
    u.searchParams.set("tenant", t);
    return u.pathname + (u.search || "") + (u.hash || "");
  } catch {
    // fallback simple
    if (!u0) return u0;
    return u0.includes("?")
      ? (u0 + "&tenant=" + encodeURIComponent(t))
      : (u0 + "?tenant=" + encodeURIComponent(t));
  }
}

function buildMediaDescriptorForAdmin(m, tenantId) {
  try {
    const raw = (m && m.meta && m.meta.raw) ? m.meta.raw : null;
    const type = String(m?.type || raw?.type || "").trim().toLowerCase();

    let mediaId = null;
    let filename = "";
    let mime = "";
    let caption = "";

    if (type === "image") {
      mediaId = raw?.image?.id || null;
      mime = raw?.image?.mime_type || "";
      caption = String(raw?.image?.caption || "").trim();
      filename = "imagen";
    } else if (type === "audio") {
      mediaId = raw?.audio?.id || null;
      mime = raw?.audio?.mime_type || "";
      filename = "audio";
    } else if (type === "document") {
      mediaId = raw?.document?.id || null;
      filename = String(raw?.document?.filename || raw?.document?.file_name || "archivo").trim();
      mime = raw?.document?.mime_type || "";
      caption = String(raw?.document?.caption || "").trim();
    } else if (type === "video") {
      mediaId = raw?.video?.id || null;
      mime = raw?.video?.mime_type || "";
      caption = String(raw?.video?.caption || "").trim();
      filename = "video";
    } else if (type === "sticker") {
      mediaId = raw?.sticker?.id || null;
      mime = raw?.sticker?.mime_type || "";
      filename = "sticker";
    }

    // fallback: cache de webhook (por si existiera)
    const cachedPublicUrl = m?.meta?.media?.publicUrl || null;
    const cachedCacheId = m?.meta?.media?.cacheId || null;
    const cachedUrl = cachedPublicUrl || (cachedCacheId ? (`/cache/media/${cachedCacheId}`) : null);

    let url = mediaId ? (`/api/media/${String(m._id)}`) : cachedUrl;
    if (!url) return null;

    url = _appendTenantParam(url, tenantId);

    const mimeFinal = (mime || (m?.meta?.media?.mime) || "").toLowerCase();
    const filenameFinal = filename || (m?.meta?.media?.filename) || null;

    // infer kind por mime si ayuda (ej: image/pdf enviado como "document")
    let kind = type || (m?.meta?.media?.kind) || "file";
    if (mimeFinal.startsWith("image/")) kind = "image";
    else if (mimeFinal.startsWith("audio/")) kind = "audio";
    else if (mimeFinal.startsWith("video/")) kind = "video";
    else if (mimeFinal.includes("pdf")) kind = "pdf";
    else if (type === "document") kind = "document";

    return {
      kind,
      url,
      mime: mimeFinal || null,
      filename: filenameFinal,
      caption: caption || null,
    };
  } catch {
    return null;
  }
}


// GET /api/media/:msgId  -> proxy seguro (requiere login) para descargar/ver media desde WhatsApp
app.get("/api/media/:msgId", async (req, res) => {
  try {
    const tenant = resolveTenantId(req);
    const msgId = String(req.params.msgId || "").trim();
    if (!ObjectId.isValid(msgId)) return res.status(400).send("invalid_id");

    const db = await getDb();
    const msgDoc = await db.collection("messages").findOne(
      withTenant({ _id: new ObjectId(msgId) }, tenant)
    );
    if (!msgDoc) return res.status(404).send("not_found");

    const raw = msgDoc?.meta?.raw || {};
    const type = String(msgDoc?.type || raw?.type || "").trim().toLowerCase();

    let mediaId = null;
    let filename = "";
    let mimeHint = "";

    if (type === "image") {
      mediaId = raw?.image?.id || null;
      mimeHint = raw?.image?.mime_type || "";
      filename = "imagen";
    } else if (type === "audio") {
      mediaId = raw?.audio?.id || null;
      mimeHint = raw?.audio?.mime_type || "";
      filename = "audio";
    } else if (type === "document") {
      mediaId = raw?.document?.id || null;
      mimeHint = raw?.document?.mime_type || "";
      filename = String(raw?.document?.filename || raw?.document?.file_name || "archivo").trim();
    } else if (type === "video") {
      mediaId = raw?.video?.id || null;
      mimeHint = raw?.video?.mime_type || "";
      filename = "video";
    } else if (type === "sticker") {
      mediaId = raw?.sticker?.id || null;
      mimeHint = raw?.sticker?.mime_type || "";
      filename = "sticker";
    }

    // fallback: si el webhook guardó cacheId/publicUrl (ej imagen analizada), usamos eso
    if (!mediaId) {
      const cached = msgDoc?.meta?.media?.cacheId || null;
      if (cached) {
        const item = getFromCache(cached);
        if (item) {
          res.setHeader("Content-Type", item.mime || "application/octet-stream");
          res.setHeader("Cache-Control", "no-store");
          res.setHeader("Content-Disposition", `inline; filename="${_safeFileName(filename || ("archivo." + (_extFromMime(item.mime) || "bin")))}"`);
          return res.send(item.buffer);
        }
      }
      return res.status(400).send("no_media");
    }

    // Obtener token correcto (multi-phone) leyendo la conversación
    let convPhoneNumberId = null;
    try {
      if (msgDoc.conversationId) {
        const conv = await db.collection("conversations").findOne(
          withTenant({ _id: msgDoc.conversationId }, tenant),
          { projection: { phoneNumberId: 1 } }
        );
        convPhoneNumberId = conv?.phoneNumberId || null;
      }
    } catch {}

    let rt = null;
    try {
      if (convPhoneNumberId) rt = await getRuntimeByPhoneNumberId(convPhoneNumberId);
    } catch {}

    const waOpts = {
      whatsappToken: rt?.whatsappToken || null,
      phoneNumberId: rt?.phoneNumberId || convPhoneNumberId || null,
    };

    const info = await getMediaInfo(mediaId, waOpts);
    const buf = await downloadMediaBuffer(info.url, waOpts);
    const mime = String(info?.mime_type || mimeHint || "application/octet-stream");

    let finalName = _safeFileName(filename || "archivo");
    const ext = _extFromMime(mime);
    if (ext && !finalName.toLowerCase().endsWith("." + ext)) {
      finalName += "." + ext;
    }

    const forceDl = String(req.query?.download || "") === "1";
    const inlinePreferred =
      !forceDl && (
        mime.startsWith("image/") ||
        mime.startsWith("audio/") ||
        mime.startsWith("video/") ||
        mime.includes("pdf")
      );

    res.setHeader("Content-Type", mime);
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("Content-Disposition", `${inlinePreferred ? "inline" : "attachment"}; filename="${finalName}"`);
    return res.send(buf);
  } catch (e) {
    console.error("GET /api/media error:", e?.message || e);
    return res.status(500).send("internal");
  }
});


app.get("/api/logs/messages", async (req, res) => {
  try {
    const { convId, waId } = req.query;
    if (!convId && !waId) return res.status(400).json({ error: "convId or waId is required" });
    const t = resolveTenantId(req);
    const rows = convId
      ? await getConversationMessagesByConvId(convId, 500, t)
      : await getConversationMessagesByWaId(waId, 500, t);
    res.json(rows.map(m => ({
      _id: String(m._id),
      role: m.role,                     // "user" | "assistant" | "system"
      content: m.content,
      type: m.type,
      media: buildMediaDescriptorForAdmin(m, t),
      createdAt: m.ts || m.createdAt
    })));
  } catch (e) {
    console.error("GET /api/logs/messages error:", e);
    res.status(500).json({ error: "internal" });
  }
});


// ---------- Meta de conversación para admin (incluye manualOpen) ----------
app.get("/api/admin/conversation-meta", async (req, res) => {
  try {
    const { convId, waId } = req.query;
    if (!convId && !waId) {
      return res.status(400).json({ error: "convId_or_waId_required" });
    }
    const tenant = resolveTenantId(req);
    const db = await getDb();

    let conv = null;
    if (convId) {
      conv = await db.collection("conversations").findOne(
        withTenant({ _id: new ObjectId(String(convId)) }, tenant)
      );
    } else {
      conv = await db.collection("conversations").findOne(
        withTenant({ waId: String(waId) }, tenant),
        { sort: { updatedAt: -1, openedAt: -1 } }
      );
    }

    if (!conv) {
      return res.status(404).json({ error: "conv_not_found" });
    }

    res.json({
      ok: true,
      convId: String(conv._id),
      waId: conv.waId,
      contactName: conv.contactName || "",
      status: adminStatusLabel(conv),
      manualOpen: !!conv.manualOpen,
      kitchenSent: !!conv.kitchenSent,
      kitchenAt: conv.kitchenAt || null,
      delivered: !!conv.delivered,
      deliveredAt: conv.deliveredAt || null,
      channelType: conv.channelType || "whatsapp",
       // Canal/telefono del negocio por el que entró la conversación
       phoneNumberId: conv.phoneNumberId || null,
       displayPhoneNumber: conv.displayPhoneNumber || null,
      instagramAccountId: conv.instagramAccountId || null,
      instagramPageId: conv.instagramPageId || null,
    });
  } catch (e) {
    console.error("GET /api/admin/conversation-meta error:", e);
    res.status(500).json({ error: "internal" });
  }
});


// ---------- Página /admin/inbox (UI estilo WhatsApp Web) ----------
app.get("/admin/inbox", async (req, res) => {
  try {
    const tenant = resolveTenantId(req);
    const conversations = await listConversations(200, tenant);
    const urlConvId = String(req.query.convId || "");

    const initialConvs = JSON.stringify(conversations || []);

    const html = `<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin | Inbox</title>
  <style>
    :root{
      --bg:#0b141a;
      --panel:#111b21;
      --panel-2:#202c33;
      --text:#e9edef;
      --muted:#aebac1;
      --accent:#00a884;
      --bubble-me:#005c4b;
      --bubble-them:#202c33;
      --border:rgba(255,255,255,.08);
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Helvetica, Arial;
      background:var(--bg);
      color:var(--text);
      height:100vh;
      overflow:hidden;
    }
    .app{
      display:flex;
      height:100vh;
      width:100vw;
    }

    /* ===== Left: Sidebar ===== */
    .sidebar{
      width:360px;
      min-width:280px;
      background:var(--panel);
      border-right:1px solid var(--border);
      display:flex;
      flex-direction:column;
    }
    .side-header{
      padding:14px 12px;
      background:var(--panel-2);
      display:flex;
      gap:8px;
      align-items:center;
      border-bottom:1px solid var(--border);
    }
    .side-header h1{
      font-size:16px;
      margin:0 8px 0 0;
      font-weight:600;
    }
    .search{
      padding:10px 12px;
      border-bottom:1px solid var(--border);
    }
    .search input{
      width:100%;
      padding:10px 12px;
      border-radius:8px;
      border:1px solid var(--border);
      background:#0f1a20;
      color:var(--text);
      outline:none;
      font-size:13px;
    }
    .conv-list{
      overflow:auto;
      flex:1;
    }
    .conv-item{
      padding:12px 12px;
      border-bottom:1px solid var(--border);
      cursor:pointer;
      display:flex;
      gap:10px;
      align-items:flex-start;
    }
    .conv-item:hover{background:#0f1a20}
    .conv-item.active{background:#0d2420}
    .avatar{
      width:38px;height:38px;border-radius:50%;
      background:#2a3942;
      display:flex;align-items:center;justify-content:center;
      font-weight:700;font-size:14px;color:#d1d7db;
      flex:0 0 38px;
    }
    .conv-meta{
      flex:1;
      min-width:0;
    }
    .conv-row{
      display:flex;justify-content:space-between;gap:8px;
      align-items:center;
    }
    .conv-name{
      font-size:14px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
    }
    .conv-wa{
      font-size:11px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
    }
    .conv-status{
      font-size:10px;color:var(--muted);
      border:1px solid var(--border);
      padding:2px 6px;border-radius:999px;
    }
     .conv-delivered{
      font-size:12px;
      color:var(--accent);
      margin-left:6px;
      font-weight:800;
      line-height:1;
    }
    .conv-last{
      font-size:11px;color:var(--muted);
    }

    /* ===== Right: Chat ===== */
    .chat{
      flex:1;
      display:flex;
      flex-direction:column;
      background:linear-gradient(180deg, #0b141a 0%, #0b141a 100%);
    }
    .chat-header{
      background:var(--panel-2);
      padding:10px 14px;
      border-bottom:1px solid var(--border);
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
    }
    .chat-title{
      display:flex;align-items:center;gap:10px;min-width:0;
    }
    .chat-title .name{
      font-size:15px;font-weight:600;
      white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
    }
    .chat-title .sub{
      font-size:11px;color:var(--muted);
    }
    .chat-actions{
      display:flex;align-items:center;gap:10px;
      font-size:12px;
      display:flex;
      align-items:center;
      gap:8px;
      font-size:11px;
      flex-wrap:wrap;
      justify-content:flex-end;
    }
    .toggle{
      display:flex;align-items:center;gap:6px;
      padding:3px 6px;border:1px solid var(--border);border-radius:8px;
      background:#0f1a20;
    }
    .toggle input{transform:translateY(1px)}
    .chat-body{
      flex:1;
      overflow:auto;
      padding:18px 16px 8px 16px;
    }
    .empty{
      color:var(--muted);
      font-size:13px;
      padding:20px;
    }
    .msg{
      max-width:70%;
      padding:8px 10px;
      border-radius:10px;
      margin:6px 0;
      font-size:13.5px;
      line-height:1.3;
      word-wrap:break-word;
      white-space:pre-wrap;
    }
    .msg.them{
      background:var(--bubble-them);
      border-top-left-radius:4px;
      align-self:flex-start;
    }
    .msg.me{
      background:var(--bubble-me);
      border-top-right-radius:4px;
      align-self:flex-end;
    }
    .msg-meta{
      font-size:10px;color:rgba(255,255,255,.55);
      margin-top:4px; text-align:right;
    }
    .msg-row{
      display:flex;flex-direction:column;
    }

    .chat-footer{
      border-top:1px solid var(--border);
      background:var(--panel);
      padding:10px;
    }
    .send-form{
      display:flex;gap:8px;
    }
    .send-form input{
      flex:1;
      padding:12px 12px;
      border-radius:10px;
      border:1px solid var(--border);
      background:#0f1a20;
      color:var(--text);
      outline:none;
      font-size:13px;
    }
    .send-form button{
      padding:0 14px;
      border-radius:10px;
      border:1px solid var(--border);
      background:var(--accent);
      color:white;
      cursor:pointer;
      font-weight:600;
    }
    .send-form button:disabled{
      opacity:.5; cursor:not-allowed;
    }

    .pill{
      font-size:10px;
      padding:2px 6px;border-radius:999px;
      border:1px solid var(--border);
      color:var(--muted);
    }

    @media (max-width: 720px){
  body{ margin: 12px; }
  header{ flex-wrap: wrap; }

  /* Ocultamos encabezados y colgroup */
  .adminTable colgroup,
  .adminTable thead{
    display:none;
  }

  /* Tabla → lista de cards */
  .adminTable,
  .adminTable tbody,
  .adminTable tr,
  .adminTable td{
    display:block;
    width:100%;
  }

  .adminTable{
    border:0;
  }

  .adminTable tr{
    border:1px solid #ddd;
    border-radius:12px;
    padding:10px 12px;
    margin:10px 0;
    background:#fff;
  }

  /* Cada “fila” (td) con etiqueta a la izquierda */
  .adminTable td{
    border:none;
    padding:6px 0;
    overflow:visible;          /* evita “celdas” altísimas por ellipsis */
    text-overflow:unset;
    display:flex;
    gap:10px;
    justify-content:space-between;
    align-items:flex-start;
  }

  .adminTable td::before{
    content: attr(data-label);
    font-weight:700;
    color:#475467;
    min-width:42%;
    max-width:42%;
  }

  /* Acciones: que ocupen toda la línea y permitan wrap */
  .adminTable td[data-label="Acciones"]{
    display:block;
    padding-top:10px;
  }
  .adminTable td[data-label="Acciones"]::before{
    display:none;
  }
  .adminTable td[data-label="Acciones"] .actions{
    flex-wrap:wrap;
    white-space:normal;
    gap:8px;
  }
}

  </style>
</head>
<body>
  <div class="app">
    <!-- SIDEBAR -->
    <aside class="sidebar">
      <div class="side-header">
        <h1>Inbox</h1>
        <button id="refreshBtn" class="pill" title="Actualizar">↻ Actualizar</button>
      </div>
      <div class="search">
        <input id="searchInput" placeholder="Buscar contacto o número..." />
      </div>
      <div id="convList" class="conv-list"></div>
    </aside>

    <!-- CHAT -->
    <main class="chat">
      <div class="chat-header">
        <div class="chat-title">
          <div class="avatar" id="chatAvatar">?</div>
          <div>
            <div class="name" id="chatName">Seleccioná un chat</div>
            <div class="sub" id="chatSub"></div>
          </div>
        </div>

        <div class="chat-actions">
          <span id="chatStatus" class="pill"></span>
          <label class="toggle">
            <input type="checkbox" id="manualToggle" />
            <span>Modo humano</span>
          </label>
          <label class="toggle">
            <input type="checkbox" id="deliveredToggle" />
            <span>Entregado</span>
          </label>
        </div>
      </div>

      <div id="chatBody" class="chat-body">
        <div class="empty">No hay conversación seleccionada.</div>
      </div>

      <div class="chat-footer">
        <form id="sendForm" class="send-form">
          <input id="msgInput" placeholder="Escribí un mensaje..." autocomplete="off" />
          <button id="sendBtn" type="submit" disabled>Enviar</button>
        </form>
      </div>
    </main>
  </div>

<script>
  // Datos iniciales pre-render
  window.__INITIAL_CONVS__ = ${initialConvs};

  const qs = new URLSearchParams(location.search);
  const TENANT = qs.get("tenant") || "";
  const PRESELECT = ${JSON.stringify(urlConvId || "")};

  let conversations = Array.isArray(window.__INITIAL_CONVS__) ? window.__INITIAL_CONVS__ : [];
  let activeConvId = "";

  const convListEl = document.getElementById("convList");
  const searchInput = document.getElementById("searchInput");
  const refreshBtn = document.getElementById("refreshBtn");

  const chatAvatar = document.getElementById("chatAvatar");
  const chatName = document.getElementById("chatName");
  const chatSub = document.getElementById("chatSub");
  const chatStatus = document.getElementById("chatStatus");
  const deliveredToggle = document.getElementById("deliveredToggle");

  const chatBody = document.getElementById("chatBody");
  const sendForm = document.getElementById("sendForm");
  const msgInput = document.getElementById("msgInput");
  const sendBtn = document.getElementById("sendBtn");

  function api(url){
    try{
      const u = new URL(url, location.origin);
      if (TENANT) u.searchParams.set("tenant", TENANT);
      return u.toString();
    }catch{
      // fallback simple
      if (!TENANT) return url;
      return url.includes("?") ? (url + "&tenant=" + encodeURIComponent(TENANT)) : (url + "?tenant=" + encodeURIComponent(TENANT));
    }
  }

  function initials(nameOrWa){
    const s = String(nameOrWa || "").trim();
    if (!s) return "?";
    const parts = s.split(/\\s+/).filter(Boolean);
    if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
    return s.slice(0,2).toUpperCase();
  }

  function fmtTime(d){
    try{
      const dt = new Date(d);
      if (Number.isNaN(dt.getTime())) return "";
      return dt.toLocaleString("es-AR", { hour:"2-digit", minute:"2-digit", day:"2-digit", month:"2-digit" });
    }catch{ return ""; }
  }

  function renderList(){
    const f = String(searchInput.value || "").toLowerCase().trim();
    const rows = conversations.filter(c => {
      const name = String(c.contactName || "").toLowerCase();
      const wa = String(c.waId || "").toLowerCase();
      return !f || name.includes(f) || wa.includes(f);
    });

    convListEl.innerHTML = rows.map(c => {
      const id = c._id;
      const name = c.contactName && c.contactName !== "-" ? c.contactName : (c.waId || "Sin nombre");
      const last = c.lastAt ? fmtTime(c.lastAt) : "";
      const status = c.status || "OPEN";
      const deliveredMark = c.delivered ? '<span class="conv-delivered" title="Entregado">✓</span>' : '';
      const cls = id === activeConvId ? "conv-item active" : "conv-item";
      return \`
        <div class="\${cls}" data-id="\${id}">
          <div class="avatar">\${initials(name)}</div>
          <div class="conv-meta">
            <div class="conv-row">
              <div class="conv-name">\${name}</div>
               <span class="conv-status">${status}</span>${deliveredMark}
            </div>
            <div class="conv-row">
              <div class="conv-wa">\${c.waId || ""}</div>
              <span class="pill">\${manual}</span>
            </div>
            <div class="conv-last">\${last}</div>
          </div>
        </div>
      \`;
    }).join("");

    // bind clicks
    convListEl.querySelectorAll(".conv-item").forEach(el => {
      el.addEventListener("click", () => {
        const id = el.getAttribute("data-id");
        if (id) selectConversation(id);
      });
    });

    if (!rows.length){
      convListEl.innerHTML = '<div class="empty">Sin resultados.</div>';
    }
  }

  async function refreshConversations(){
    const r = await fetch(api("/api/logs/conversations?limit=200"));
    conversations = await r.json();
    renderList();
  }

  function setUrlConv(id){
    const u = new URL(location.href);
    if (id) u.searchParams.set("convId", id);
    else u.searchParams.delete("convId");
    if (TENANT) u.searchParams.set("tenant", TENANT);
    history.replaceState(null, "", u.toString());
  }

  async function loadMeta(convId){
    const r = await fetch(api("/api/admin/conversation-meta?convId=" + encodeURIComponent(convId)));
    if (!r.ok) throw new Error("meta_error");
    return r.json();
  }

  async function loadMessages(convId){
    const r = await fetch(api("/api/logs/messages?convId=" + encodeURIComponent(convId)));
    if (!r.ok) throw new Error("messages_error");
    return r.json();
  }

  function renderMessages(msgs){
    if (!Array.isArray(msgs) || !msgs.length){
      chatBody.innerHTML = '<div class="empty">Sin mensajes todavía.</div>';
      return;
    }
    chatBody.innerHTML = "";
    const frag = document.createDocumentFragment();

        function buildMediaNode(m){
      const md = m && m.media;
      if (!md || !md.url) return null;

      const url = api(md.url);
      const kind = String(md.kind || "").toLowerCase();
      const mime = String(md.mime || "").toLowerCase();
      const filename = String(md.filename || "archivo").trim() || "archivo";
      const fnameLower = filename.toLowerCase();

      const isImage = (kind === "image") || mime.startsWith("image/");
      const isAudio = (kind === "audio") || mime.startsWith("audio/");
      const isVideo = (kind === "video") || mime.startsWith("video/");
      const isPdf = (kind === "pdf") || mime.includes("pdf") || fnameLower.endsWith(".pdf");

      const wrap = document.createElement("div");
      wrap.style.marginTop = "6px";

      // helper: download link
      const dlUrl = url + (url.includes("?") ? "&" : "?") + "download=1";

      const links = document.createElement("div");
      links.style.display = "flex";
      links.style.gap = "10px";
      links.style.marginTop = "4px";
      links.style.alignItems = "center";

      const open = document.createElement("a");
      open.href = url;
      open.textContent = "Abrir";
      open.target = "_blank";
      open.rel = "noopener";
      open.style.fontSize = "11px";
      open.style.color = "rgba(255,255,255,.82)";
      open.style.textDecoration = "underline";

      const dl = document.createElement("a");
      dl.href = dlUrl;
      dl.textContent = "Descargar";
      dl.style.fontSize = "11px";
      dl.style.color = "rgba(255,255,255,.82)";
      dl.style.textDecoration = "underline";

      if (isImage) {
        const a = document.createElement("a");
        a.href = url;
        a.target = "_blank";
        a.rel = "noopener";

        const img = document.createElement("img");
        img.src = url;
        img.alt = filename;
        img.loading = "lazy";
        img.style.maxWidth = "280px";
        img.style.borderRadius = "10px";
        img.style.display = "block";

        a.appendChild(img);
        wrap.appendChild(a);

        links.appendChild(dl);
        wrap.appendChild(links);
        return wrap;
      }

      if (isAudio) {
        const audio = document.createElement("audio");
        audio.controls = true;
        audio.src = url;
        audio.style.maxWidth = "320px";
        audio.style.display = "block";
        wrap.appendChild(audio);

        links.appendChild(open);
        links.appendChild(dl);
        wrap.appendChild(links);
        return wrap;
      }

      if (isVideo) {
        const video = document.createElement("video");
        video.controls = true;
        video.src = url;
        video.style.maxWidth = "320px";
        video.style.borderRadius = "10px";
        video.style.display = "block";
        wrap.appendChild(video);

        links.appendChild(open);
        links.appendChild(dl);
        wrap.appendChild(links);
        return wrap;
      }

      if (isPdf) {
        const frame = document.createElement("iframe");
        frame.src = url;
        frame.style.width = "320px";
        frame.style.maxWidth = "100%";
        frame.style.height = "380px";
        frame.style.border = "0";
        frame.style.borderRadius = "10px";
        frame.style.display = "block";
        wrap.appendChild(frame);

        links.appendChild(open);
        links.appendChild(dl);
        wrap.appendChild(links);
        return wrap;
      }

      // documentos u otros
      const a = document.createElement("a");
      a.href = dlUrl;
      a.textContent = filename ? ("📎 " + filename) : "📎 Archivo";
      a.style.color = "rgba(255,255,255,.92)";
      a.style.textDecoration = "underline";
      a.style.fontSize = "13px";
      wrap.appendChild(a);

      links.appendChild(open);
      links.appendChild(dl);
      wrap.appendChild(links);
      return wrap;
    }


    msgs.forEach(m => {
      const row = document.createElement("div");
      row.className = "msg-row";
      row.style.alignItems = (m.role === "user") ? "flex-start" : "flex-end";

      const bubble = document.createElement("div");
      bubble.className = "msg " + ((m.role === "user") ? "them" : "me");

      // texto del mensaje (si existe)
      const txt = document.createElement("div");
      txt.textContent = String(m.content || "");
      bubble.appendChild(txt);

      // media preview (si existe)
      const mediaNode = buildMediaNode(m);
      if (mediaNode) bubble.appendChild(mediaNode);

      const meta = document.createElement("div");
      meta.className = "msg-meta";
      meta.textContent = m.createdAt ? fmtTime(m.createdAt) : "";

      bubble.appendChild(meta);
      row.appendChild(bubble);
      frag.appendChild(row);
    });

    chatBody.appendChild(frag);
    chatBody.scrollTop = chatBody.scrollHeight;
  }


  async function selectConversation(convId){
    activeConvId = convId;
    setUrlConv(convId);
    renderList();

    sendBtn.disabled = !convId;

    chatBody.innerHTML = '<div class="empty">Cargando...</div>';

    try{
      const meta = await loadMeta(convId);
      const name = meta.contactName || meta.waId || "Chat";
      chatAvatar.textContent = initials(name);
      chatName.textContent = name;
       const ch = (meta.displayPhoneNumber || meta.phoneNumberId || meta.instagramPageId || meta.instagramAccountId || "");
       const channelLabel = String(meta.channelType || 'whatsapp').toLowerCase() === 'instagram' ? 'Instagram' : 'WhatsApp';
       chatSub.textContent = meta.waId
         ? (channelLabel + ': ' + meta.waId + (ch ? (' · Canal: ' + ch) : ''))
         : "";
      chatStatus.textContent = meta.status || "";
      if (deliveredToggle) deliveredToggle.checked = !!meta.delivered;

      const msgs = await loadMessages(convId);
      renderMessages(msgs);
    }catch(e){
      chatBody.innerHTML = '<div class="empty">No se pudo cargar la conversación.</div>';
    }
  }

  // Toggle humano/bot
  manualToggle.addEventListener("change", async () => {
    if (!activeConvId) return;
    try{
      await fetch(api("/api/admin/conversation-manual"), {
        method:"POST",
        headers:{ "Content-Type":"application/json" },
        body: JSON.stringify({ convId: activeConvId, manualOpen: manualToggle.checked })
      });
      // refrescamos lista para que refleje pill HUMANO/BOT
      await refreshConversations();
    }catch{}
  });


  // Toggle entregado/no entregado
  if (deliveredToggle) {
    deliveredToggle.addEventListener("change", async () => {
      if (!activeConvId) return;
      try{
        await fetch(api("/api/admin/conversation-delivered"), {
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify({ convId: activeConvId, delivered: deliveredToggle.checked })
        });
        // refrescamos lista para que se refleje el check
        await refreshConversations();
      }catch{}
    });
  }



  // Enviar mensaje manual
  sendForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (!activeConvId) return;
    const text = String(msgInput.value || "").trim();
    if (!text) return;

    sendBtn.disabled = true;

    try{
      await fetch(api("/api/admin/send-message"), {
        method:"POST",
        headers:{ "Content-Type":"application/json" },
        body: JSON.stringify({ convId: activeConvId, text })
      });
      msgInput.value = "";
      const msgs = await loadMessages(activeConvId);
      renderMessages(msgs);
      await refreshConversations();
    }catch(e){}
    finally{
      sendBtn.disabled = false;
      msgInput.focus();
    }
  });

  // Buscador
  searchInput.addEventListener("input", renderList);

  // Refresh
  refreshBtn.addEventListener("click", async () => {
    refreshBtn.textContent = "↻ ...";
    try{ await refreshConversations(); }
    finally{ refreshBtn.textContent = "↻ Actualizar"; }
  });

  // Init
  renderList();
  if (PRESELECT) {
    selectConversation(PRESELECT);
  } else if (conversations[0] && conversations[0]._id) {
    selectConversation(conversations[0]._id);
  }
</script>
</body>
</html>`;

    res.send(html);
  } catch (e) {
    console.error("GET /admin/inbox error:", e);
    res.status(500).send("Error interno");
  }
});



// ---------- Marcar conversación como entregada / no entregada ----------
app.post("/api/admin/conversation-delivered", async (req, res) => {
  try {
    const { convId, waId, delivered } = req.body || {};
    if (!convId && !waId) {
      return res.status(400).json({ error: "convId_or_waId_required" });
    }
    const tenant = resolveTenantId(req);
    const db = await getDb();

    let filter;
    if (convId) {
      filter = withTenant({ _id: new ObjectId(String(convId)) }, tenant);
    } else {
      filter = withTenant({ waId: String(waId) }, tenant);
    }

    const now = new Date();
   const flag = !!delivered;
    const update = flag
      ? { $set: { delivered: true, deliveredAt: now, updatedAt: now } }
      : { $set: { delivered: false, updatedAt: now }, $unset: { deliveredAt: "" } };

    const result = await db.collection("conversations").findOneAndUpdate(
      filter,
      update,
      { returnDocument: "after" }
    );

    const conv = result && typeof result === "object" && Object.prototype.hasOwnProperty.call(result, "value")
      ? result.value
      : result;
   if (!conv) {
      return res.status(404).json({ error: "conv_not_found" });
    }

    res.json({
      ok: true,
      convId: String(conv._id),
      delivered: !!conv.delivered,
      deliveredAt: conv.deliveredAt || null,
    });
  } catch (e) {
    console.error("POST /api/admin/conversation-delivered error:", e);
    res.status(500).json({ error: "internal" });
  }
});


// ---------- Marcar conversación como enviada a cocina / pendiente de cocina ----------
app.post("/api/admin/conversation-kitchen", async (req, res) => {
  try {
    const { convId, waId, kitchenSent } = req.body || {};
    if (!convId && !waId) {
      return res.status(400).json({ error: "convId_or_waId_required" });
    }
    const tenant = resolveTenantId(req);
    const db = await getDb();

    let filter;
    if (convId) {
      filter = withTenant({ _id: new ObjectId(String(convId)) }, tenant);
    } else {
      filter = withTenant({ waId: String(waId) }, tenant);
    }

    const now = new Date();
    const flag = !!kitchenSent;
    const update = flag
      ? { $set: { kitchenSent: true, kitchenAt: now, updatedAt: now } }
      : { $set: { kitchenSent: false, updatedAt: now }, $unset: { kitchenAt: "" } };

    const result = await db.collection("conversations").findOneAndUpdate(
      filter,
      update,
      { returnDocument: "after" }
    );

    const conv = result && typeof result === "object" && Object.prototype.hasOwnProperty.call(result, "value")
      ? result.value
      : result;
    if (!conv) {
      return res.status(404).json({ error: "conv_not_found" });
    }

    res.json({
      ok: true,
      convId: String(conv._id),
      kitchenSent: !!conv.kitchenSent,
      kitchenAt: conv.kitchenAt || null,
    });
  } catch (e) {
    console.error("POST /api/admin/conversation-kitchen error:", e);
    res.status(500).json({ error: "internal" });
  }
});


// ---------- Marcar conversación como manual (humano) / automática (bot) ----------
app.post("/api/admin/conversation-manual", async (req, res) => {
  try {
    const { convId, waId, manualOpen } = req.body || {};
    if (!convId && !waId) {
      return res.status(400).json({ error: "convId_or_waId_required" });
    }
    const tenant = resolveTenantId(req);
    const db = await getDb();

    let filter;
    if (convId) {
      filter = withTenant({ _id: new ObjectId(String(convId)) }, tenant);
    } else {
      filter = withTenant({ waId: String(waId) }, tenant);
    }

    const now = new Date();
    const result = await db.collection("conversations").findOneAndUpdate(
      filter,
      { $set: { manualOpen: !!manualOpen, updatedAt: now } },
      { returnDocument: "after" }
    );

    const conv = result && typeof result === "object" && Object.prototype.hasOwnProperty.call(result, "value")
      ? result.value
      : result;
    if (!conv) {
      return res.status(404).json({ error: "conv_not_found" });
    }

    res.json({
      ok: true,
      convId: String(conv._id),
      manualOpen: !!conv.manualOpen,
    });
  } catch (e) {
    console.error("POST /api/admin/conversation-manual error:", e);
    res.status(500).json({ error: "internal" });
  }
});


// ---------- Enviar mensaje manual al cliente desde /admin ----------
app.post("/api/admin/send-message", async (req, res) => {
  try {
    const { convId, waId, text } = req.body || {};
    const body = String(text || "").trim();
    if (!body) {
      return res.status(400).json({ error: "text_required" });
    }

    const tenant = resolveTenantId(req);
    const db = await getDb();

    let conv = null;
    if (convId) {
      conv = await db.collection("conversations").findOne(
        withTenant({ _id: new ObjectId(String(convId)) }, tenant)
      );
    } else if (waId) {
      conv = await db.collection("conversations").findOne(
        withTenant({ waId: String(waId) }, tenant),
        { sort: { updatedAt: -1, openedAt: -1 } }
      );
    }

    if (!conv) {
      return res.status(404).json({ error: "conv_not_found" });
    }

    const to = conv.waId;
    const channelType = String(conv.channelType || "whatsapp").trim().toLowerCase() || "whatsapp";
    let rt = null;
    try {
      if (channelType === "instagram") {
        rt = await getRuntimeByInstagramAccountId(conv.instagramAccountId || conv.instagramPageId || null);
      } else {
        const convPhoneNumberId = conv.phoneNumberId || null;
        if (convPhoneNumberId) {
          rt = await getRuntimeByPhoneNumberId(convPhoneNumberId);
        }
      }
    } catch {}

    const channelOpts = {
      channelType,
      whatsappToken: rt?.whatsappToken || null,
      phoneNumberId: rt?.phoneNumberId || conv.phoneNumberId || null,
      instagramAccountId: rt?.instagramAccountId || conv.instagramAccountId || null,
      instagramPageId: rt?.instagramPageId || conv.instagramPageId || null,
      instagramAccessToken: rt?.instagramAccessToken || null,
    };

    await require("./logic").sendChannelMessage(to, body, channelOpts);

    const now = new Date();

    // Guardar en colección messages como "assistant" pero marcado como humano
    await saveMessageDoc({
      tenantId: tenant,
      conversationId: conv._id,
      waId: to,
      role: "assistant",
      content: body,
      type: "text",
      meta: { from: "admin" },
    });

    // Actualizar timestamps de la conversación
    await db.collection("conversations").updateOne(
      { _id: conv._id },
      { $set: { lastAssistantTs: now, updatedAt: now } }
    );

    res.json({ ok: true });
  } catch (e) {
    console.error("POST /api/admin/send-message error:", e);
    res.status(500).json({ error: "internal" });
  }
});




// Pedido de una conversación (detalle JSON)
app.get("/api/logs/pedido", async (req, res) => {
  try {
    const { convId } = req.query;
    if (!convId) {
      return res.status(400).json({ error: "convId required" });
    }

    const db = await getDb();
    const tenantId = resolveTenantId(req);
    const convObjectId = new ObjectId(String(convId));

    // Datos básicos de la conversación
    let waId = "";
    let contactName = "";
    let fecha = new Date().toLocaleString("es-AR");
    try {
      const conv = await db.collection("conversations").findOne(
        withTenant({ _id: convObjectId }, tenantId)
      );
      if (conv) {
        waId = conv.waId || "";
        contactName = conv.contactName || "";
        fecha = conv.lastAt
          ? new Date(conv.lastAt).toLocaleString("es-AR")
          : fecha;
      }
    } catch (e) {
      console.warn("[pedido] no se pudo leer conversation:", e?.message);
    }

    // 1) Intentar leer el pedido desde orders
    let pedido = null;
    try {
      const order = await db.collection("orders").findOne(
        withTenant({ conversationId: convObjectId }, tenantId),
        { sort: { createdAt: -1 } }
      );
      if (order && order.pedido) {
        pedido = order.pedido;
      }
    } catch (e) {
      console.warn("[pedido] no se pudo leer order:", e?.message);
    }

    // 2) Fallback: si no hay pedido en orders, buscar JSON en mensajes
    if (!pedido) {
      const msgs = await getConversationMessagesByConvId(convId, 1000, tenantId);
      for (let i = msgs.length - 1; i >= 0; i--) {
        const m = msgs[i];
        if (m.role !== "assistant") continue;
        const s = String(m.content || "").trim();
        try {
          const j = JSON.parse(s);
          if (j && j.Pedido && Array.isArray(j.Pedido.items)) {
            pedido = j.Pedido;
            break;
          }
        } catch {
          // no era JSON, seguimos
        }
      }
    }

    if (!pedido) {
      return res.status(404).json({ error: "pedido_not_found" });
    }

    const items = (pedido.items || []).map((it) => ({
      descripcion: String(it.descripcion || "").trim(),
      cantidad: Number(it.cantidad || 0),
      importe_unitario: Number(it.importe_unitario || 0),
      total: Number(it.total || 0),
    }));

    let total = Number(pedido.total_pedido || 0);
    if (!total && items.length) {
      total = items.reduce(
        (sum, it) => sum + (Number(it.total) || 0),
        0
      );
    }

    let entregaLabel = "-";
    const entregaRaw = String(pedido.Entrega || "").trim().toLowerCase();
    if (entregaRaw === "domicilio") entregaLabel = "Envío";
    else if (entregaRaw === "retiro") entregaLabel = "Retiro";

    let direccion = "-";
    if (pedido.Entrega === "domicilio") {
      if (typeof pedido.Domicilio === "string") {
        direccion = pedido.Domicilio.trim() || "-";
      } else if (pedido.Domicilio && typeof pedido.Domicilio === "object") {
        direccion =
          String(
            pedido.Domicilio.direccion ||
              pedido.Domicilio.calle ||
              ""
          ).trim() || "-";
      }
    }

    res.json({
      ok: true,
      convId,
      waId,
      contactName,
      fecha,
      entrega: entregaLabel,
      direccion,
      fechaEntrega: pedido.Fecha || null,
      horaEntrega: pedido.Hora || null,
      items,
      total,
    });
  } catch (e) {
    console.error("GET /api/logs/pedido error:", e);
    res.status(500).json({ error: "internal" });
  }
});


// ---------- Página /admin (HTML liviano) ----------
app.get("/admin", async (req, res) => {
  try {
    const conversations = await listConversations(200, resolveTenantId(req));
    const urlConvId = String(req.query.convId || "");
   
    const html = `<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin | Conversaciones</title>
  <style>
    :root{
      --bg:#f3f7fc;
      --card:#ffffff;
      --card-soft:#f8fbff;
      --line:#dbe5f0;
      --line-strong:#c7d4e2;
      --text:#0f172a;
      --muted:#5b6472;
      --primary:#0f3b68;
      --primary-2:#164e86;
      --shadow:0 10px 28px rgba(15, 23, 42, .08);
      --radius:18px;
    }
    *{box-sizing:border-box}
    html,body{margin:0;padding:0}
    body{
      font-family:system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
      background:var(--bg);
      color:var(--text);
      padding:16px 18px 24px;
    }
    .page-shell{display:flex;flex-direction:column;gap:16px}
    .page-header{
      display:flex;
      justify-content:space-between;
      align-items:flex-start;
      gap:12px;
      padding:4px 2px 0;
    }
    .eyebrow{
      display:inline-flex;
      align-items:center;
      gap:6px;
      font-size:12px;
      font-weight:700;
      color:var(--primary-2);
      text-transform:uppercase;
      letter-spacing:.08em;
      margin-bottom:6px;
    }
    .eyebrow::before{
      content:"";
      width:8px;height:8px;border-radius:999px;background:#22c55e;
      box-shadow:0 0 0 4px rgba(34,197,94,.14);
    }
    .page-header h2{margin:0;font-size:28px;line-height:1.1}
    .subtitle{margin:6px 0 0;color:var(--muted);font-size:14px}
    .live-indicator{
      display:inline-flex;align-items:center;gap:8px;
      background:#fff;border:1px solid var(--line);border-radius:999px;
      padding:8px 12px;font-size:13px;color:#334155;box-shadow:var(--shadow);
      white-space:nowrap;
    }
    .live-indicator .dot{
      width:8px;height:8px;border-radius:999px;background:#22c55e;
      box-shadow:0 0 0 4px rgba(34,197,94,.16);
    }
    .toolbar-card,.table-card,.stat-card{
      background:var(--card);
      border:1px solid var(--line);
      border-radius:var(--radius);
      box-shadow:var(--shadow);
    }
    .toolbar-card{padding:10px 12px}
    .toolbar{display:flex;gap:10px;align-items:flex-end;justify-content:flex-start;flex-wrap:wrap}
    .field{display:flex;flex-direction:column;gap:4px;min-width:150px}
    .field.grow{flex:1 1 300px}
    .field label{font-size:11px;font-weight:700;color:#334155}
    .field input,.field select,#modalReplyText{
      width:100%;
      font-size:13px;
      padding:8px 10px;
      border:1px solid var(--line-strong);
      border-radius:12px;
      background:#fff;
      color:var(--text);
      outline:none;
      transition:border-color .16s ease, box-shadow .16s ease, background .16s ease;
    }
    .field input:focus,.field select:focus,#modalReplyText:focus{
      border-color:#8fb7df;
      box-shadow:0 0 0 4px rgba(59,130,246,.12);
    }
    .field.field-filter{position:relative}
    .field input[type="hidden"]{display:none}
    .filter-trigger{
      width:100%;
      font-size:13px;
      padding:8px 10px;
      border:1px solid var(--line-strong);
      border-radius:12px;
      background:#fff;
      color:var(--text);
      outline:none;
      transition:border-color .16s ease, box-shadow .16s ease, background .16s ease;
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
      min-height:35px;
      cursor:pointer;
      text-align:left;
      box-shadow:none;
      font-weight:400;
    }
    .filter-trigger:hover{background:#f8fafc}
    .filter-trigger:focus{
      border-color:#8fb7df;
      box-shadow:0 0 0 4px rgba(59,130,246,.12);
    }
    .filter-trigger .caret{font-size:10px;color:#64748b;flex:0 0 auto}
    .filter-trigger .filter-text{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .filter-popover{
      position:absolute;
      top:calc(100% + 6px);
      left:0;
      min-width:220px;
      width:max-content;
      max-width:280px;
      padding:10px;
      border:1px solid var(--line-strong);
      border-radius:14px;
      background:#fff;
      box-shadow:0 18px 40px rgba(15,23,42,.16);
      z-index:40;
    }
    .filter-popover[hidden]{display:none}
    .filter-popover-title{font-size:12px;font-weight:800;color:#0f172a;margin:0 0 8px}
    .filter-option{display:grid;grid-template-columns:16px minmax(0,1fr);align-items:start;justify-content:flex-start;column-gap:8px;row-gap:0;width:100%;padding:6px 2px;font-size:13px;color:#0f172a;cursor:pointer;text-align:left}
    .filter-option span{display:block;min-width:0;text-align:left;line-height:1.25;white-space:normal;word-break:break-word}
    .filter-option input[type="checkbox"]{width:16px !important;min-width:16px;max-width:16px;height:16px;margin:1px 0 0;padding:0;border:none;box-shadow:none;background:transparent;accent-color:var(--primary-2);appearance:auto;flex:none;justify-self:start;align-self:start}
    .filter-popover-actions{display:flex;justify-content:flex-end;gap:6px;margin-top:8px;padding-top:8px;border-top:1px solid #e5e7eb}
    .btn.btn-mini{padding:7px 10px;font-size:11px;border-radius:10px}
    .toolbar-actions{display:flex;gap:6px;align-items:center;justify-content:flex-start;flex-wrap:wrap}
    .btn{
      appearance:none;
      border:1px solid var(--line-strong);
      background:#fff;
      color:#0f172a;
      border-radius:10px;
      padding:8px 12px;
      font-size:12px;
      font-weight:700;
      cursor:pointer;
      transition:transform .12s ease, box-shadow .16s ease, background .16s ease, border-color .16s ease;
      box-shadow:0 4px 12px rgba(15,23,42,.04);
    }
    .btn:hover{transform:translateY(-1px);box-shadow:0 8px 20px rgba(15,23,42,.08)}
    .btn-primary{background:var(--primary);border-color:var(--primary);color:#fff}
    .btn-primary:hover{background:var(--primary-2);border-color:var(--primary-2)}
    .btn-soft{background:#eff6ff;border-color:#cfe0f2;color:var(--primary-2)}
    .btn-icon{padding:10px 11px;min-width:42px;text-align:center}
    .muted{color:var(--muted)}

    .table-card{padding:10px 12px}
    .table-toolbar{
      display:flex;justify-content:space-between;align-items:flex-start;gap:8px;flex-wrap:wrap;
      margin-bottom:8px;
    }
    .table-toolbar h3{margin:0;font-size:16px}
    .table-toolbar p{margin:0;color:var(--muted);font-size:12px}
    .table-wrap{
      overflow:auto;
      border:1px solid var(--line);
      border-radius:16px;
      background:#fff;
    }
    .adminTable{border-collapse:separate;border-spacing:0;width:100%;min-width:0;table-layout:auto}
    .adminTable th,
    .adminTable td{padding:8px 5px;font-size:12px;vertical-align:top;word-break:break-word;border-bottom:1px solid #edf2f7}
    .adminTable th{
      position:sticky;top:0;z-index:2;
      background:#f8fbff;
      color:#475569;
      text-align:left;
      font-size:11px;
      text-transform:uppercase;
      letter-spacing:.04em;
      white-space:nowrap;
    }
    .adminTable tbody tr{background:#fff;transition:background .14s ease}
    .adminTable tbody tr:nth-child(even){background:#fcfdff}
    .adminTable tbody tr:hover{background:#f6fbff}
    .adminTable td{color:#1f2937;overflow:hidden;text-overflow:ellipsis}
    .adminTable td[data-label="Acciones"]{overflow:visible;text-overflow:clip}
    .adminTable td[data-label="Distancia"],
    .adminTable td[data-label="Día"],
    .adminTable td[data-label="Hora"],
    .adminTable td[data-label="Estado"],
    .adminTable th:nth-child(6),
    .adminTable th:nth-child(7),
    .adminTable th:nth-child(8),
    .adminTable th:nth-child(9){padding-left:6px;padding-right:6px;white-space:nowrap;font-size:11px}
    .cell-strong{font-weight:700;color:#0f172a}
    .cell-subtle{display:block;margin-top:2px;color:#64748b;font-size:11px}
    .cell-ellipsis{display:block;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .cell-products{display:block;white-space:normal;overflow:visible;text-overflow:clip;line-height:1.32;word-break:break-word;overflow-wrap:anywhere}
    .cell-products > div + div{margin-top:2px}
    .cell-address{display:block;white-space:normal;overflow:visible;text-overflow:clip;line-height:1.32;word-break:break-word;overflow-wrap:anywhere}
    .delivery-pill{
      display:inline-flex;align-items:center;gap:5px;
      padding:4px 9px;border-radius:999px;
      background:#f8fafc;border:1px solid #dbe5f0;font-size:11px;font-weight:700;color:#334155;
    }
    .delivery-pill.is-envio{background:#eef6ff;border-color:#cfe0f2;color:#164e86}
    .delivery-pill.is-retiro{background:#f8fafc;border-color:#dbe5f0;color:#475569}
    .distance-badge{
      display:inline-flex;align-items:center;justify-content:center;
      min-width:40px;padding:4px 6px;border-radius:999px;background:#f8fafc;border:1px solid #dbe5f0;font-weight:700;color:#334155;font-size:11px;
    }
    .actions{display:flex;gap:4px;align-items:center;justify-content:flex-start;flex-wrap:nowrap;width:100%}
    .actions .btn{padding:0;border-radius:10px;min-width:30px;width:30px;height:30px;display:inline-flex;align-items:center;justify-content:center;font-size:13px;line-height:1}
    .actions .btn-detail{font-size:14px}
    .actions .btn-soft{background:#f8fafc;color:#334155;border-color:#dbe5f0}
    .actions .btn-icon{padding:0;min-width:30px;width:30px;height:30px;display:inline-flex;align-items:center;justify-content:center;font-size:13px;line-height:1}
    .c-actividad{width:72px}
    .c-telefono{width:84px}
    .c-nombre{width:92px}
    .c-productos{width:320px}
    .c-entrega{width:66px}
    .c-direccion{width:180px}
    .c-dist{width:52px}
    .c-dia{width:64px}
    .c-hora{width:54px}
    .c-estado{width:86px}
    .c-ent{width:34px}
    .c-acciones{width:96px}
    .delivered-row{opacity:.72}
    .kitchen-row{background:rgba(255,243,205,.45)}
    .delivChk{cursor:pointer;width:16px;height:16px;accent-color:#22c55e}

    .stats-panel{padding:10px 12px}
    .stats-panel.is-collapsed .stats-body{display:none}
    .stats-head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px;flex-wrap:wrap}
    .stats-title{font-size:15px;font-weight:800;color:#0f172a}
    .stats-subtitle{margin:2px 0 0;color:var(--muted);font-size:12px}
    .stats-refresh{font-size:11px;white-space:nowrap}
    .stats-body{margin-top:10px;display:flex;flex-direction:column;gap:10px}
    .stats-grid{display:grid;grid-template-columns:repeat(5,minmax(110px,1fr));gap:8px}
    .kpi{
      border:1px solid var(--line);
      background:linear-gradient(180deg,#fff 0%,#f8fbff 100%);
      border-radius:14px;
      padding:10px 10px 8px;
      min-height:78px;
    }
    .kpi-value{font-size:24px;line-height:1;font-weight:800;color:#0f172a}
    .kpi-label{margin-top:4px;font-size:11px;font-weight:700;color:#475467;text-transform:uppercase;letter-spacing:.04em}
    .kpi-meta{margin-top:4px;font-size:11px;color:#64748b;line-height:1.25}
    .stats-charts{display:grid;grid-template-columns:1fr 1fr;gap:8px}
    .chart-card{
      border:1px solid var(--line);
      background:#fff;
      border-radius:14px;
      padding:10px;
    }
    .chart-title{font-size:12px;font-weight:800;color:#0f172a;margin-bottom:8px}
    .bar-list{display:flex;flex-direction:column;gap:8px}
    .bar-row{display:grid;grid-template-columns:minmax(74px,auto) 1fr auto;gap:8px;align-items:center}
    .bar-label,.top-item-label{font-size:11px;color:#475467;font-weight:700}
    .bar-track{
      position:relative;height:8px;border-radius:999px;background:#edf2f7;overflow:hidden;
    }
    .bar-fill{
      position:absolute;left:0;top:0;bottom:0;border-radius:999px;
      background:linear-gradient(90deg,#164e86 0%,#3b82f6 100%);
    }
    .bar-value,.top-item-value{font-size:11px;font-weight:800;color:#0f172a}
    .top-list{display:flex;flex-direction:column;gap:8px}
    .top-list.is-scrollable{max-height:240px;overflow-y:auto;padding-right:4px}
    .top-item{display:grid;grid-template-columns:1fr auto;gap:8px;align-items:start}
    .top-item + .top-item{padding-top:8px;border-top:1px dashed #e5edf6}

    .status-badge{
      display:inline-block;
      padding:4px 8px;
      border-radius:999px;
      font-size:10px;
      font-weight:800;
      letter-spacing:.02em;
      border:1px solid transparent;
      line-height:1.2;
      white-space:nowrap;
    }
    .st-open{background:#eef2ff;color:#1e3a8a;border-color:#c7d2fe}
    .st-progress{background:#fff7ed;color:#9a3412;border-color:#fed7aa}
    .st-completed{background:#ecfdf5;color:#065f46;border-color:#a7f3d0}
    .st-cancelled{background:#fef2f2;color:#991b1b;border-color:#fecaca}

    .modal-backdrop{position:fixed;inset:0;background:rgba(2,6,23,.42);display:none;align-items:center;justify-content:center;z-index:1000;padding:16px}
    .modal{background:#fff;border:1px solid var(--line);border-radius:18px;box-shadow:0 24px 60px rgba(15,23,42,.22);width:min(960px,95vw);max-height:88vh;display:flex;flex-direction:column;overflow:hidden}
    .modal header{display:flex;align-items:center;justify-content:space-between;padding:14px 16px;border-bottom:1px solid #eef2f7;margin:0}
    .modal header h3{margin:0;font-size:18px}
    .modal .body{padding:14px 16px;overflow:auto;background:#fbfdff}
    .chip{display:inline-flex;align-items:center;gap:8px}
    .iconbtn{border:none;background:#f8fafc;cursor:pointer;font-size:16px;width:36px;height:36px;border-radius:10px;border:1px solid #dbe5f0}
    .msg{border:1px solid #e7eef6;border-radius:14px;padding:10px 12px;margin-bottom:8px;background:#fff}
    .role-user{background:#f0fafc}
    .role-assistant{background:#f8f6ff}
    small{color:#64748b}
    pre{white-space:pre-wrap;margin:6px 0 0;font-family:inherit}
    .badge{padding:5px 10px;border-radius:999px;font-size:12px;font-weight:700}
    .badge-bot{background:#e3f7e3;color:#145214}
    .badge-manual{background:#ffe4e1;color:#8b0000}
    .modal-meta-row{display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap}
    #modalChatBox{margin-top:14px;border-top:1px solid #eef2f7;padding-top:12px;display:flex;flex-direction:column;gap:8px}
    #modalReplyText{min-height:88px;font-family:inherit}
    .chat-actions{display:flex;align-items:center;gap:8px;justify-content:flex-end;flex-wrap:wrap}

    .ticket-modal-backdrop {position: fixed;inset: 0;background: rgba(0,0,0,.45);display: none;align-items: center;justify-content: center;z-index: 1000;}
    .ticket-modal {background: #fff;border-radius: 12px;padding: 12px;box-shadow: 0 10px 30px rgba(0,0,0,.25);width: 90mm;max-height: 90vh;display: flex;flex-direction: column;overflow: auto;gap: 8px;}
    .ticket-modal iframe {border: none;width: 100%;flex: 1;}
    .ticket-modal-actions {display: flex;justify-content: flex-end;gap: 8px;margin-top: 6px;}

    @media (max-width: 1180px){
      .stats-grid{grid-template-columns:repeat(3,minmax(120px,1fr))}
      .stats-charts{grid-template-columns:1fr}
    }
    @media (max-width: 1080px){
      .page-header{flex-direction:column}
    }
    @media (max-width: 720px){
      body{padding:10px}
      .toolbar{align-items:stretch}
      .field{min-width:100%}
      .toolbar-actions{width:100%}
      .toolbar-actions .btn{flex:1 1 auto;justify-content:center}
      .stats-grid{grid-template-columns:repeat(2,minmax(120px,1fr))}
      .stats-panel,.table-card,.toolbar-card{padding:10px}
      .table-card{padding:10px}
      .table-wrap{border:none;background:transparent;overflow:visible}
      .adminTable colgroup,.adminTable thead{display:none}
      .adminTable,.adminTable tbody,.adminTable tr,.adminTable td{display:block;width:100%}
      .adminTable{min-width:0;border:0}
      .adminTable tr{border:1px solid var(--line);border-radius:16px;padding:10px 12px;margin:10px 0;background:#fff;box-shadow:0 8px 20px rgba(15,23,42,.05)}
      .adminTable td{border:none;padding:6px 0;overflow:visible;text-overflow:unset;display:flex;gap:10px;justify-content:space-between;align-items:flex-start}
      .adminTable td::before{content: attr(data-label);font-weight:700;color:#475467;min-width:42%;max-width:42%}
      .adminTable td[data-label="Acciones"]{display:block;padding-top:10px}
      .adminTable td[data-label="Acciones"]::before{display:none}
      .adminTable td[data-label="Acciones"] .actions{flex-wrap:wrap;white-space:normal;gap:8px}
    }
  </style>
</head>
<body>
  <div class="page-shell">
   
    
    <section class="stat-card stats-panel" id="statsPanel">
      <div class="stats-head">
        <div>
          <div class="stats-title">Resumen del movimiento</div>
          <p class="stats-subtitle" id="statsHint">Estados visibles sobre lo filtrado · pendientes y entrega sobre el total cargado.</p>
        </div>
        <div class="toolbar-actions">
          <span class="muted stats-refresh">Refresco automático cada 1 minuto</span>
          <button class="btn btn-soft" id="btnToggleStats">Ocultar resumen</button>
        </div>
      </div>
      <div class="stats-body" id="statsBody">
        <div class="stats-grid" id="statsGrid">
          <div class="kpi"><div class="kpi-value">-</div><div class="kpi-label">Pendientes</div><div class="kpi-meta">Total cargado</div></div>
          <div class="kpi"><div class="kpi-value">-</div><div class="kpi-label">Pollos</div><div class="kpi-meta">Pendientes totales</div></div>
          <div class="kpi"><div class="kpi-value">-</div><div class="kpi-label">Papas</div><div class="kpi-meta">Pendientes totales</div></div>
          <div class="kpi"><div class="kpi-value">-</div><div class="kpi-label">A enviar</div><div class="kpi-meta">Pendientes totales</div></div>
          <div class="kpi"><div class="kpi-value">-</div><div class="kpi-label">A retirar</div><div class="kpi-meta">Pendientes totales</div></div>
        </div>
        <div class="stats-charts">
          <div class="chart-card">
            <div class="chart-title">Estados visibles</div>
            <div class="bar-list" id="statusBars"></div>
          </div>
          <div class="chart-card">
            <div class="chart-title">Entrega pendiente</div>
            <div class="bar-list" id="deliveryBars"></div>
          </div>
        </div>
      </div>
    </section>

<section class="toolbar-card">
      <div class="toolbar">
        <div class="field grow">
          <label for="qFilter">Buscar</label>
          <input id="qFilter" placeholder="waId, nombre, dirección o producto"/>
        </div>
        <div class="field field-filter">
          <label for="delivFilter">Entrega</label>
          <input id="delivFilter" type="hidden" value="pending"/>
          <button type="button" class="filter-trigger" id="delivFilterBtn" aria-haspopup="true" aria-expanded="false">
            <span class="filter-text">No entregadas</span><span class="caret">▾</span>
          </button>
          <div class="filter-popover" id="delivFilterPopover" hidden>
            <div class="filter-popover-title">Entrega</div>
            <label class="filter-option"><input type="checkbox" value="pending" checked /> <span>No entregadas</span></label>
            <label class="filter-option"><input type="checkbox" value="kitchen" /> <span>En cocina</span></label>
            <label class="filter-option"><input type="checkbox" value="delivered" /> <span>Entregadas</span></label>
            <div class="filter-popover-actions">
              <button type="button" class="btn btn-mini" data-filter-all="delivFilter">Todas</button>
              <button type="button" class="btn btn-mini btn-primary" data-filter-apply="delivFilter">Aplicar</button>
            </div>
          </div>
        </div>
        <div class="field field-filter">
         <label for="statusFilter">Estado</label>
          <input id="statusFilter" type="hidden" value="completed"/>
          <button type="button" class="filter-trigger" id="statusFilterBtn" aria-haspopup="true" aria-expanded="false">
            <span class="filter-text">Completadas</span><span class="caret">▾</span>
          </button>
          <div class="filter-popover" id="statusFilterPopover" hidden>
            <div class="filter-popover-title">Estado</div>
            <label class="filter-option"><input type="checkbox" value="completed" checked /> <span>Completadas</span></label>
            <label class="filter-option"><input type="checkbox" value="open" /> <span>Abiertas</span></label>
            <label class="filter-option"><input type="checkbox" value="cancelled" /> <span>Canceladas</span></label>
            <div class="filter-popover-actions">
              <button type="button" class="btn btn-mini" data-filter-all="statusFilter">Todas</button>
              <button type="button" class="btn btn-mini btn-primary" data-filter-apply="statusFilter">Aplicar</button>
            </div>
          </div>
        </div>
        <div class="field">
          <label for="dateFrom">Desde</label>
          <input id="dateFrom" type="date"/>
        </div>
        <div class="field">
          <label for="dateTo">Hasta</label>
          <input id="dateTo" type="date"/>
        </div>
        <div class="toolbar-actions">
          <button class="btn btn-soft" id="btnToday">Hoy</button>
          <button class="btn" id="btnClearDates">Limpiar fechas</button>
          <button class="btn btn-primary" id="btnReload">Actualizar</button>

        </div>
      </div>
    </section>

    <section class="table-card">
      <div class="table-toolbar">
        <div>
           <p id="tableHint">Cargando conversaciones…</p>
        </div>
      </div>
      <div class="table-wrap">
        <table id="tbl" class="adminTable">
          <colgroup>
            <col class="c-actividad"/>
            <col class="c-telefono"/>
            <col class="c-nombre"/>
            <col class="c-productos"/>
            <col class="c-entrega"/>
            <col class="c-direccion"/>
            <col class="c-dist"/>
            <col class="c-dia"/>
            <col class="c-hora"/>
            <col class="c-estado"/>
            <col class="c-coc"/>
            <col class="c-ent"/>
            <col class="c-acciones"/>
          </colgroup>
          <thead>
            <tr>
              <th>Actividad</th>
              <th>Teléfono</th>
              <th>Nombre</th>
              <th>Productos</th>
              <th>Entrega</th>
              <th>Dirección</th>
              <th>Distancia</th>
              <th>Día</th>
              <th>Hora</th>
              <th>Estado</th>
              <th>Coc.</th>
              <th>Ent.</th>
              <th>Acciones</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>
  </div>

    <!-- Modal -->
   <div id="modalRoot" class="modal-backdrop" role="dialog" aria-modal="true" aria-hidden="true">
     <div class="modal" role="document">
       <header>
         <h3>Detalle de conversación</h3>
         <div class="chip">
           <button class="iconbtn" title="Imprimir" id="modalPrintBtn">🖨️</button>
           <button class="iconbtn" title="Cerrar" id="modalCloseBtn">✖</button>
         </div>
       </header>
       <div class="body" id="modalBody">
         <div class="modal-meta-row">
           <span id="modalManualBadge" class="badge badge-bot">Estado: ...</span>
           <button class="btn" id="modalToggleManualBtn">Tomar chat (pausar bot)</button>
         </div>
         <div id="modalMsgs">
           <p class="muted">Cargando…</p>
         </div>
         <div id="modalChatBox">
           <textarea id="modalReplyText" placeholder="Escribí un mensaje para el cliente… (Ctrl/⌘+Enter para enviar)"></textarea>
           <div class="chat-actions">
             <button class="btn" id="modalSendBtn">Enviar</button>
             <span class="muted" style="font-size:12px">El bot solo se pausa si el chat está en modo manual.</span>
           </div>
         </div>
       </div>
     </div>
   </div>

    <!-- Modal Detalle de Pedido -->
    <div id="pedidoModalBackdrop" class="modal-backdrop" role="dialog" aria-modal="true" aria-hidden="true">
      <div class="modal" role="document">
        <header>
          <h3>Detalle de pedido</h3>
          <div class="chip">
            <button class="iconbtn" title="Cerrar" id="pedidoModalCloseBtn">✖</button>
          </div>
        </header>
        <div class="body" id="pedidoModalBody">
          <p class="muted">Cargando…</p>
        </div>
      </div>
    </div>


   <!-- Modal para mostrar el ticket -->
    <div id="ticketModalBackdrop" class="ticket-modal-backdrop">
      <div class="ticket-modal">
        <iframe id="ticketFrame" title="Ticket"></iframe>
        <div class="ticket-modal-actions">
          <button onclick="closeTicketModal()">Cerrar</button>
          <button onclick="printTicket()">Imprimir</button>
        </div>
      </div>
    </div>

  <script>
  // =========================
  // Helpers DOM
  // =========================
  const modalRoot = document.getElementById('modalRoot');
  const modalCloseBtn = document.getElementById('modalCloseBtn');
  const modalPrintBtn = document.getElementById('modalPrintBtn');

  const pedidoModalBackdrop = document.getElementById('pedidoModalBackdrop');
  const pedidoModalBody = document.getElementById('pedidoModalBody');
  const pedidoModalCloseBtn = document.getElementById('pedidoModalCloseBtn');

  const modalMsgs = () => document.getElementById('modalMsgs');
  const modalManualBadge = () => document.getElementById('modalManualBadge');
  const modalToggleManualBtn = () => document.getElementById('modalToggleManualBtn');
  const modalReplyText = () => document.getElementById('modalReplyText');
  const modalSendBtn = () => document.getElementById('modalSendBtn');

  // =========================
  // Estado global del modal
  // =========================
  let currentConvId = null;
  let modalManualOpen = false;

  // Auto-refresh modal
  let modalPollTimer = null;
  let lastMsgsFingerprint = "";

  // =========================
  // Utilidades UI
  // =========================
  function escHtml(str) {
    return String(str || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }


  // Normaliza cualquier estado a un set canónico para poder colorear.
  function normalizeStatus(raw){
    const s = String(raw || '').trim().toUpperCase();
    if (!s) return 'OPEN';
    if (['CANCELLED','CANCELED','CANCELADA','CANCELADO'].includes(s)) return 'CANCELLED';
    if (['COMPLETED','COMPLETADA','FINALIZADA','FINALIZADO'].includes(s)) return 'COMPLETED';
    if (['OPEN','ABIERTA','ABIERTO'].includes(s)) return 'OPEN';
    if (['IN_PROGRESS','EN CURSO','PROCESANDO'].includes(s)) return 'IN_PROGRESS';
    return s;
  }

  function statusLabelEs(raw, kitchenSent){
    if (kitchenSent) return 'EN COCINA';
    const st = normalizeStatus(raw);
    if (st === 'CANCELLED') return 'CANCELADA';
    if (st === 'COMPLETED') return 'COMPLETADA';
    if (st === 'IN_PROGRESS') return 'EN CURSO';
    if (st === 'OPEN') return 'ABIERTA';
    return String(raw || st || '').trim();
  }

  function statusClass(raw, kitchenSent){
    if (kitchenSent) return 'st-progress';
    const st = normalizeStatus(raw);
    if (st === 'CANCELLED') return 'st-cancelled';
    if (st === 'COMPLETED') return 'st-completed';
    if (st === 'IN_PROGRESS') return 'st-progress';
    return 'st-open';
  }

  function renderStatusBadge(raw, kitchenSent){
    const label = statusLabelEs(raw, kitchenSent);
    const cls = statusClass(raw, kitchenSent);
    return '<span class="status-badge ' + cls + '">' + escHtml(label) + '</span>';
  }
 



  function formatMoney(n) {
    const num = Number(n || 0);
    if (!Number.isFinite(num)) return '-';
    return num.toLocaleString('es-AR', { minimumFractionDigits: 0 });
  }

  function fmt(d){
    try { return new Date(d).toLocaleString(); }
    catch { return '-'; }
  }

  // =========================
  // Render mensajes modal
  // =========================
  function renderMessages(list){
    const target = modalMsgs();
    if (!target) return;

    if(!Array.isArray(list) || !list.length){
      target.innerHTML = '<p class="muted">Sin mensajes para mostrar</p>';
      return;
    }

    function mediaHtml(m){
      const md = m && m.media;
      if (!md || !md.url) return '';
      const kind = String(md.kind || '').toLowerCase();
      const filename = escHtml(String(md.filename || 'archivo'));
      const url = escHtml(String(md.url || ''));
      const dl = url + (url.includes('?') ? '&' : '?') + 'download=1';

      if (kind === 'image') {
        return (
          '<div style="margin-top:6px">' +
            '<a href="'+url+'" target="_blank" rel="noopener">' +
              '<img src="'+url+'" alt="'+filename+'" style="max-width:320px;border-radius:10px;display:block"/>' +
            '</a>' +
            '<a href="'+dl+'" style="display:inline-block;margin-top:4px;font-size:11px;text-decoration:underline">Descargar</a>' +
          '</div>'
        );
      }
      if (kind === 'audio') {
        return (
          '<div style="margin-top:6px">' +
            '<audio controls src="'+url+'" style="max-width:340px;display:block"></audio>' +
            '<a href="'+dl+'" style="display:inline-block;margin-top:4px;font-size:11px;text-decoration:underline">Descargar</a>' +
          '</div>'
        );
      }
      if (kind === 'video') {
        return (
          '<div style="margin-top:6px">' +
            '<video controls src="'+url+'" style="max-width:340px;border-radius:10px;display:block"></video>' +
            '<a href="'+dl+'" style="display:inline-block;margin-top:4px;font-size:11px;text-decoration:underline">Descargar</a>' +
          '</div>'
        );
      }
      // documentos u otros
      return (
        '<div style="margin-top:6px">' +
          '<a href="'+dl+'" style="text-decoration:underline">📎 '+(filename || 'Archivo')+'</a>' +
        '</div>'
      );
    }

    target.innerHTML = list.map(m => (
      '<div class="msg role-'+escHtml(m.role)+'">'+
        '<small>['+new Date(m.createdAt).toLocaleString()+'] '+escHtml(m.role)+'</small>'+
        '<pre>'+escHtml(m.content||'')+'</pre>'+
        mediaHtml(m) +
      '</div>'
    )).join('');
  }

  // =========================
  // Estado manual UI
  // =========================
  function updateModalManualUI(){
    const badge = modalManualBadge();
    const btn = modalToggleManualBtn();
    if (!badge || !btn) return;

    if (modalManualOpen) {
      badge.textContent = 'Modo MANUAL: bot pausado';
      badge.classList.remove('badge-bot');
      badge.classList.add('badge-manual');
      btn.textContent = 'Liberar al bot';
    } else {
      badge.textContent = 'Modo BOT automático';
      badge.classList.remove('badge-manual');
      badge.classList.add('badge-bot');
      btn.textContent = 'Tomar chat (pausar bot)';
    }
  }

  async function verifyDelivered(convId){
    try{
      const r = await fetch('/api/admin/conversation-meta?convId=' + encodeURIComponent(convId));
      if(!r.ok) return null;
      const j = await r.json();
      return !!j.delivered;
    }catch{
      return null;
    }
  }

  async function verifyKitchen(convId){
    try{
      const r = await fetch('/api/admin/conversation-meta?convId=' + encodeURIComponent(convId));
      if(!r.ok) return null;
      const j = await r.json();
      return !!j.kitchenSent;
    }catch{
      return null;
    }
  }

  async function loadModalMeta(){
    if (!currentConvId) return;
    try {
      const r = await fetch('/api/admin/conversation-meta?convId=' + encodeURIComponent(currentConvId));
      if (!r.ok) return;
      const j = await r.json().catch(()=>null);
      modalManualOpen = !!(j && j.manualOpen);
      updateModalManualUI();
    } catch (e) {
      console.warn("loadModalMeta error", e);
    }
  }

  // ✅ Toggle tolerante (evita “error fantasma”)
  async function toggleModalManual(){
    if (!currentConvId) return;

    let j = null;
    let r = null;

    try {
      r = await fetch('/api/admin/conversation-manual', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ convId: currentConvId, manualOpen: !modalManualOpen })
      });

      j = await r.json().catch(()=>null);

      // Si el backend devolvió manualOpen booleano, es suficiente
      if (j && typeof j.manualOpen === "boolean") {
        modalManualOpen = !!j.manualOpen;
        updateModalManualUI();
        return;
      }

      if (!r.ok || !j?.ok) {
        alert('No se pudo cambiar el estado manual.');
        return;
      }

      modalManualOpen = !!j.manualOpen;
      updateModalManualUI();

    } catch (e) {
      console.error("toggleModalManual error", e);
      alert('No se pudo cambiar el estado manual.');
    }
  }

  // =========================
  // Enviar mensaje manual
  // =========================
  async function sendModalMessage(){
    if (!currentConvId) return;

    const ta = modalReplyText();
    const text = String(ta?.value || '').trim();
    if (!text) return;

    try {
      const r = await fetch('/api/admin/send-message', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ convId: currentConvId, text })
      });

      const j = await r.json().catch(()=>null);
      if (!r.ok || !j?.ok) {
        alert('Error al enviar el mensaje.');
        return;
      }

      if (ta) ta.value = '';

      // refrescar mensajes inmediato
      await refreshModalMessages(true);

    } catch (e) {
      console.error("sendModalMessage error", e);
      alert('Error al enviar el mensaje.');
    }
  }

  // =========================
  // Polling del modal (auto refresh)
  // =========================
  async function refreshModalMessages(forceScroll){
    if (!currentConvId) return;

    const target = modalMsgs();
    const wasNearBottom = target
      ? (target.scrollHeight - target.scrollTop - target.clientHeight) < 120
      : true;

    const r = await fetch('/api/logs/messages?convId=' + encodeURIComponent(currentConvId));
    if (!r.ok) return;

    const data = await r.json().catch(()=>[]);
    if (!Array.isArray(data)) return;

    const last = data[data.length - 1];
     const fp =
      String(data.length) + ':' +
      String((last && (last._id || last.createdAt)) || '');

    if (fp !== lastMsgsFingerprint) {
      lastMsgsFingerprint = fp;
      renderMessages(data);

      const target2 = modalMsgs();
      if (target2 && (forceScroll || wasNearBottom)) {
        target2.scrollTop = target2.scrollHeight;
      }
    }
  }

  function startModalPolling(){
    stopModalPolling();
    modalPollTimer = setInterval(async () => {
      try {
        await refreshModalMessages(false);
        await loadModalMeta();
      } catch (e) {
        console.warn("modal polling error", e);
      }
    }, 3000);
  }

  function stopModalPolling(){
    if (modalPollTimer) {
      clearInterval(modalPollTimer);
      modalPollTimer = null;
    }
  }

  // =========================
  // Modal open/close
  // =========================
  function closeModal(){
    stopModalPolling();
    currentConvId = null;
    lastMsgsFingerprint = "";
    modalRoot.style.display='none';
    modalRoot.setAttribute('aria-hidden','true');
  }

  modalCloseBtn.addEventListener('click', closeModal);
  modalRoot.addEventListener('click', (e)=>{ if(e.target===modalRoot) closeModal(); });

  async function openDetailModal(convId){
    try{
      currentConvId = convId;
      lastMsgsFingerprint = "";

      modalRoot.style.display = 'flex';
      modalRoot.setAttribute('aria-hidden','false');

      const msgsEl = modalMsgs();
      if (msgsEl) msgsEl.innerHTML = '<p class="muted">Cargando…</p>';

      await loadModalMeta();

      await refreshModalMessages(true);

      // imprimir desde el modal
      if (modalPrintBtn) {
        modalPrintBtn.onclick = () => openTicketModal(convId);
      }

      // ✅ empezar auto-actualización
      startModalPolling();

    } catch (e) {
      console.error('openDetailModal error:', e);
      const msgsEl = modalMsgs();
      if (msgsEl) msgsEl.innerHTML = '<p class="muted">Error al cargar.</p>';
    }
  }

  // =========================
  // Modal Pedido
  // =========================
  function closePedidoModal() {
    pedidoModalBackdrop.style.display = 'none';
    pedidoModalBackdrop.setAttribute('aria-hidden', 'true');
  }

  pedidoModalCloseBtn.addEventListener('click', closePedidoModal);
  pedidoModalBackdrop.addEventListener('click', (e)=>{ if(e.target===pedidoModalBackdrop) closePedidoModal(); });

  function renderPedidoDetail(data) {
    const items = Array.isArray(data.items) ? data.items : [];
    let html = '';

    html += '<p>';
    html += '<strong>Nombre:</strong> ' + escHtml(data.contactName || '-') + '<br/>';
    html += '<strong>Teléfono:</strong> ' + escHtml(data.waId || '-') + '<br/>';
    if (data.fechaEntrega || data.horaEntrega) {
      html += '<strong>Entrega para:</strong> ' +
        escHtml(data.fechaEntrega || '') + ' ' +
        escHtml(data.horaEntrega || '') + '<br/>';
    }
    html += '<strong>Modalidad:</strong> ' + escHtml(data.entrega || '-') + '<br/>';
    html += '<strong>Dirección:</strong> ' + escHtml(data.direccion || '-') + '<br/>';
    html += '</p>';

    if (!items.length) {
      html += '<p class="muted">No se encontraron ítems en este pedido.</p>';
      pedidoModalBody.innerHTML = html;
      return;
    }

    html += '<table style="width:100%;border-collapse:collapse;margin-top:8px">';
    html += '<thead><tr>' +
      '<th style="text-align:left;border-bottom:1px solid #ddd;padding:4px">Cant.</th>' +
      '<th style="text-align:left;border-bottom:1px solid #ddd;padding:4px">Producto</th>' +
      '<th style="text-align:right;border-bottom:1px solid #ddd;padding:4px">Unit.</th>' +
      '<th style="text-align:right;border-bottom:1px solid #ddd;padding:4px">Total</th>' +
      '</tr></thead><tbody>';

    items.forEach(it => {
      html += '<tr>';
      html += '<td style="padding:4px;border-bottom:1px solid #eee">' + escHtml(it.cantidad != null ? it.cantidad : '') + '</td>';
      html += '<td style="padding:4px;border-bottom:1px solid #eee">' + escHtml(it.descripcion || '') + '</td>';
      html += '<td style="padding:4px;border-bottom:1px solid #eee;text-align:right">' +
        (it.importe_unitario ? ('$ ' + formatMoney(it.importe_unitario)) : '-') +
        '</td>';
      html += '<td style="padding:4px;border-bottom:1px solid #eee;text-align:right">$ ' +
        formatMoney(it.total) +
        '</td>';
      html += '</tr>';
    });

    html += '</tbody></table>';
    html += '<p style="margin-top:8px;text-align:right"><strong>Total:</strong> $ ' + formatMoney(data.total) + '</p>';

    pedidoModalBody.innerHTML = html;
  }

  async function openPedidoModal(convId) {
    try {
      pedidoModalBody.innerHTML = '<p class="muted">Cargando…</p>';
      pedidoModalBackdrop.style.display = 'flex';
      pedidoModalBackdrop.setAttribute('aria-hidden', 'false');

      const r = await fetch('/api/logs/pedido?convId=' + encodeURIComponent(convId));
      if (!r.ok) {
        pedidoModalBody.innerHTML = '<p class="muted">No se encontró un pedido para esta conversación.</p>';
        return;
      }
      const data = await r.json();
      if (!data || !data.ok) {
        pedidoModalBody.innerHTML = '<p class="muted">No se encontró un pedido para esta conversación.</p>';
        return;
      }
      renderPedidoDetail(data);
    } catch (e) {
      console.error('Detalle pedido modal error:', e);
      pedidoModalBody.innerHTML = '<p class="muted">Error al cargar el pedido.</p>';
    }
  }

  // =========================
  // Ticket modal (iframe)
  // =========================
  function openTicketModal(conversationId) {
    const backdrop = document.getElementById('ticketModalBackdrop');
    const frame = document.getElementById('ticketFrame');
    if (frame) frame.src = '/admin/ticket/' + conversationId;
    if (backdrop) backdrop.style.display = 'flex';
  }

  function closeTicketModal() {
    const backdrop = document.getElementById('ticketModalBackdrop');
    const frame = document.getElementById('ticketFrame');
    if (frame) frame.src = 'about:blank';
    if (backdrop) backdrop.style.display = 'none';
  }

  function printTicket() {
    const frame = document.getElementById('ticketFrame');
    if (frame && frame.contentWindow) {
      frame.contentWindow.focus();
      frame.contentWindow.print();
    }
  }

  // Exponer para botones inline del HTML
  window.openTicketModal = openTicketModal;
  window.closeTicketModal = closeTicketModal;
  window.printTicket = printTicket;

  function formatActivity(raw){
    try {
      const d = new Date(raw);
      return {
        date: d.toLocaleDateString('es-AR'),
        time: d.toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit' })
      };
    } catch {
      return { date: '-', time: '' };
    }
  }

  function todayIso(){
    const now = new Date();
    const yyyy = now.getFullYear();
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const dd = String(now.getDate()).padStart(2, '0');
    return yyyy + '-' + mm + '-' + dd;
  }

  const ADMIN_FILTERS_STORAGE_KEY = 'adminConversationsFilters.v1';

  function saveAdminFilterState(){
    try {
      const payload = {
        q: String(document.getElementById('qFilter')?.value || ''),
        deliv: String(document.getElementById('delivFilter')?.value || 'pending'),
        status: String(document.getElementById('statusFilter')?.value || 'completed'),
        from: String(document.getElementById('dateFrom')?.value || ''),
        to: String(document.getElementById('dateTo')?.value || '')
      };
      localStorage.setItem(ADMIN_FILTERS_STORAGE_KEY, JSON.stringify(payload));
    } catch {}
  }

  function initAdminFilters(){
    const today = todayIso();
    const from = document.getElementById('dateFrom');
    const to = document.getElementById('dateTo');
    const q = document.getElementById('qFilter');
    const deliv = document.getElementById('delivFilter');
    const status = document.getElementById('statusFilter');

    let restored = false;
    try {
      const raw = localStorage.getItem(ADMIN_FILTERS_STORAGE_KEY);
      if (raw) {
        const saved = JSON.parse(raw) || {};
        if (q && typeof saved.q === 'string') q.value = saved.q;
        if (from && typeof saved.from === 'string') from.value = saved.from;
        if (to && typeof saved.to === 'string') to.value = saved.to;
        if (deliv) deliv.value = parseMultiFilterValue(saved.deliv, DELIVERY_FILTER_ALLOWED, ['pending']).join(',');
        if (status) status.value = parseMultiFilterValue(saved.status, STATUS_FILTER_ALLOWED, ['completed']).join(',');
        restored = true;
      }
    } catch {}

    if (!restored) {
      if (from && !from.value) from.value = today;
      if (to && !to.value) to.value = today;
      if (deliv && !String(deliv.value || '').trim()) deliv.value = 'pending';
      if (status && !String(status.value || '').trim()) status.value = 'completed';
    }
    refreshFilterButtonLabels();
  }

  function getTextFilterValue(){
    return String(document.getElementById('qFilter')?.value || '').trim().toLowerCase();
  }

  const DELIVERY_FILTER_ALLOWED = ['pending', 'kitchen', 'delivered'];
  const STATUS_FILTER_ALLOWED = ['completed', 'open', 'cancelled'];
  const DELIVERY_FILTER_LABELS = { pending: 'No entregadas', kitchen: 'En cocina', delivered: 'Entregadas' };
  const STATUS_FILTER_LABELS = { completed: 'Completadas', open: 'Abiertas', cancelled: 'Canceladas' };

  function parseMultiFilterValue(raw, allowed, fallback){
    const allowedSet = new Set(Array.isArray(allowed) ? allowed : []);
    const values = String(raw || '')
      .split(',')
      .map(v => String(v || '').trim())
      .filter(v => allowedSet.has(v));
    const ordered = (Array.isArray(allowed) ? allowed : []).filter(v => values.includes(v));
    if (ordered.length) return ordered;
    return Array.isArray(fallback) ? fallback.slice() : [];
  }

  function getDeliveryFilterValue(){
    return parseMultiFilterValue(document.getElementById('delivFilter')?.value, DELIVERY_FILTER_ALLOWED, ['pending']);
  }

  function getStatusFilterValue(){
    return parseMultiFilterValue(document.getElementById('statusFilter')?.value, STATUS_FILTER_ALLOWED, ['completed']);
  }

  function formatMultiFilterLabel(selected, labels, allowed, allLabel){
    const ordered = (Array.isArray(allowed) ? allowed : []).filter(v => (Array.isArray(selected) ? selected : []).includes(v));
    if (!ordered.length || ordered.length === (Array.isArray(allowed) ? allowed.length : 0)) return allLabel;
    if (ordered.length === 1) return labels[ordered[0]] || ordered[0];
    if (ordered.length === 2) return (labels[ordered[0]] || ordered[0]) + ' + ' + (labels[ordered[1]] || ordered[1]);
    return ordered.length + ' seleccionados';
  }

  function refreshFilterButtonLabels(){
    const delivBtnTxt = document.querySelector('#delivFilterBtn .filter-text');
    if (delivBtnTxt) delivBtnTxt.textContent = formatMultiFilterLabel(getDeliveryFilterValue(), DELIVERY_FILTER_LABELS, DELIVERY_FILTER_ALLOWED, 'Todas');
    const statusBtnTxt = document.querySelector('#statusFilterBtn .filter-text');
    if (statusBtnTxt) statusBtnTxt.textContent = formatMultiFilterLabel(getStatusFilterValue(), STATUS_FILTER_LABELS, STATUS_FILTER_ALLOWED, 'Todos');
  }

  function syncPopoverChecks(hiddenId){
    const hidden = document.getElementById(hiddenId);
    const pop = document.getElementById(hiddenId + 'Popover');
    if (!hidden || !pop) return;
    const allowed = hiddenId === 'delivFilter' ? DELIVERY_FILTER_ALLOWED : STATUS_FILTER_ALLOWED;
    const fallback = hiddenId === 'delivFilter' ? ['pending'] : ['completed'];
    const selected = parseMultiFilterValue(hidden.value, allowed, fallback);
    pop.querySelectorAll('input[type="checkbox"]').forEach(chk => { chk.checked = selected.includes(chk.value); });
  }

  function setHiddenMultiValue(hiddenId, values){
    const hidden = document.getElementById(hiddenId);
    if (!hidden) return;
    const allowed = hiddenId === 'delivFilter' ? DELIVERY_FILTER_ALLOWED : STATUS_FILTER_ALLOWED;
    const fallback = hiddenId === 'delivFilter' ? ['pending'] : ['completed'];
    const ordered = parseMultiFilterValue((Array.isArray(values) ? values : []).join(','), allowed, fallback);
    hidden.value = ordered.join(',');
    refreshFilterButtonLabels();
    saveAdminFilterState();
  }

  function closeAllFilterPopovers(){
    ['delivFilter', 'statusFilter'].forEach(id => {
      const pop = document.getElementById(id + 'Popover');
      const btn = document.getElementById(id + 'Btn');
      if (pop) pop.hidden = true;
      if (btn) btn.setAttribute('aria-expanded', 'false');
    });
  }

  function openFilterPopover(hiddenId){
    const pop = document.getElementById(hiddenId + 'Popover');
    const btn = document.getElementById(hiddenId + 'Btn');
    if (!pop || !btn) return;
    const willOpen = pop.hidden;
    closeAllFilterPopovers();
    if (!willOpen) return;
    syncPopoverChecks(hiddenId);
    pop.hidden = false;
    btn.setAttribute('aria-expanded', 'true');
  }

  function initAdminFilterPopovers(){
    ['delivFilter', 'statusFilter'].forEach(hiddenId => {
      const btn = document.getElementById(hiddenId + 'Btn');
      const pop = document.getElementById(hiddenId + 'Popover');
      if (!btn || !pop) return;
      btn.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        openFilterPopover(hiddenId);
      });
      pop.addEventListener('click', (e) => e.stopPropagation());
    });

    document.querySelectorAll('[data-filter-all]').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.preventDefault();
        const hiddenId = btn.getAttribute('data-filter-all');
        const allowed = hiddenId === 'delivFilter' ? DELIVERY_FILTER_ALLOWED : STATUS_FILTER_ALLOWED;
        setHiddenMultiValue(hiddenId, allowed);
        syncPopoverChecks(hiddenId);
      });
    });

    document.querySelectorAll('[data-filter-apply]').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.preventDefault();
        const hiddenId = btn.getAttribute('data-filter-apply');
        const pop = document.getElementById(hiddenId + 'Popover');
        if (!pop) return;
        const checked = Array.from(pop.querySelectorAll('input[type="checkbox"]:checked')).map(chk => chk.value);
        const fallback = hiddenId === 'delivFilter' ? ['pending'] : ['completed'];
        setHiddenMultiValue(hiddenId, checked.length ? checked : fallback);
        closeAllFilterPopovers();
        loadTable();
      });
    });

    document.addEventListener('click', () => closeAllFilterPopovers());
    refreshFilterButtonLabels();
  }

  function getDateRangeValue(){
    return {
      from: String(document.getElementById('dateFrom')?.value || '').trim(),
      to: String(document.getElementById('dateTo')?.value || '').trim()
    };
  }

  function convMatchesText(c, textFilter){
   if (!textFilter) return true;
    const productLines = Array.isArray(c?.products) ? c.products.join(' ') : '';
    const haystack = [
      c.waId,
      c.contactName,
      c.direccion,
      c.entregaLabel,
      c.fechaEntrega,
      c.horaEntrega,
      productLines
    ].map(v => String(v || '').toLowerCase()).join(' ');
    return haystack.includes(textFilter);
  }

  function getConversationStatusTags(c){
    const st = normalizeStatus(c?.status);
    const tags = [];
    if (st === 'COMPLETED') tags.push('completed');
    if (st === 'OPEN' || st === 'IN_PROGRESS') tags.push('open');
    if (st === 'CANCELLED') tags.push('cancelled');
    return tags;
  }

  function convMatchesStatus(c, statusFilter){
    const selected = Array.isArray(statusFilter) ? statusFilter : getStatusFilterValue();
    if (!selected.length || selected.length === STATUS_FILTER_ALLOWED.length) return true;
    const tags = getConversationStatusTags(c);
    return selected.some(tag => tags.includes(tag));
  }

  function convMatchesDelivered(c, deliveryFilter){
    const selected = Array.isArray(deliveryFilter) ? deliveryFilter : getDeliveryFilterValue();
    if (!selected.length || selected.length === DELIVERY_FILTER_ALLOWED.length) return true;
    const delivered = !!c?.delivered;
    const kitchen = !!c?.kitchenSent && !delivered;
    const pending = !delivered && !kitchen;
    return (pending && selected.includes('pending')) ||
           (kitchen && selected.includes('kitchen')) ||
           (delivered && selected.includes('delivered'));
  }

 function convDateKey(c){
    const raw = String(c?.fechaEntrega || '').trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    const m = raw.match(/^(\d{2})-(\d{2})-(\d{4})$/);
    if (m) return m[3] + '-' + m[2] + '-' + m[1];
    const last = c?.lastAt ? new Date(c.lastAt) : null;
    if (last && Number.isFinite(last.getTime())) {
      const yyyy = last.getFullYear();
      const mm = String(last.getMonth() + 1).padStart(2, '0');
      const dd = String(last.getDate()).padStart(2, '0');
      return yyyy + '-' + mm + '-' + dd;
    }
    return '';
  }

 function convMatchesDateRange(c, from, to){
    const fecha = convDateKey(c);
    if (!from && !to) return true;
    if (!fecha) return false;
    if (from && fecha < from) return false;
    if (to && fecha > to) return false;
    return true;
  }

  function convDateTimeSortValue(c){
    const fecha = /^\d{4}-\d{2}-\d{2}$/.test(String(c?.fechaEntrega || '').trim()) ? String(c.fechaEntrega).trim() : '';
    const hora = /^\d{2}:\d{2}$/.test(String(c?.horaEntrega || '').trim()) ? String(c.horaEntrega).trim() : '23:59';
    if (fecha) {
      const ms = Date.parse(fecha + 'T' + hora + ':00-03:00');
      if (Number.isFinite(ms)) return ms;
    }
    const fallback = Date.parse(c?.lastAt || '');
    return Number.isFinite(fallback) ? fallback : 0;
  }

  function statusPriorityForAdmin(raw, statusFilter, kitchenSent){
    const tags = [];
    const st = normalizeStatus(raw);
    if (kitchenSent) tags.push('kitchen');
    if (st === 'COMPLETED') tags.push('completed');
    if (st === 'OPEN') tags.push('open');
    if (st === 'IN_PROGRESS') tags.push('in_progress');
    if (st === 'CANCELLED') tags.push('cancelled');
    const preferred = ['kitchen', 'completed', 'open', 'in_progress', 'cancelled'];
    const selected = Array.isArray(statusFilter) && statusFilter.length
      ? preferred.filter(v => statusFilter.includes(v))
      : preferred;
    for (let i = 0; i < selected.length; i++) {
      if (tags.includes(selected[i])) return i;
    }
    return preferred.length + 1;
  }

  function entregaPriorityForAdmin(row){
    const mode = getEntregaMode(row);
    if (mode === 'retiro') return 0;
    if (mode === 'envio') return 1;
    return 2;
  }

  function sortAdminConversations(list, statusFilter){
    return (Array.isArray(list) ? list.slice() : []).sort((a, b) => {
      const statusDelta = statusPriorityForAdmin(a?.status, statusFilter, !!a?.kitchenSent) - statusPriorityForAdmin(b?.status, statusFilter, !!b?.kitchenSent);
      if (statusDelta !== 0) return statusDelta;

      const entregaDelta = entregaPriorityForAdmin(a) - entregaPriorityForAdmin(b);
      if (entregaDelta !== 0) return entregaDelta;

      const aWhen = convDateTimeSortValue(a);
      const bWhen = convDateTimeSortValue(b);
      if (aWhen !== bWhen) return aWhen - bWhen;
      const aLast = Date.parse(a?.lastAt || '');
      const bLast = Date.parse(b?.lastAt || '');
      return (Number.isFinite(bLast) ? bLast : 0) - (Number.isFinite(aLast) ? aLast : 0);
    });
  }

  function updateAdminSummary(list, allCount){
    const rows = Array.isArray(list) ? list : [];
    const total = rows.length;
    const deliveredFilter = getDeliveryFilterValue();
    const statusFilter = getStatusFilterValue();
    const range = getDateRangeValue();
    const hint = document.getElementById('tableHint');
    if (hint) {
      const deliveryLabel = formatMultiFilterLabel(deliveredFilter, DELIVERY_FILTER_LABELS, DELIVERY_FILTER_ALLOWED, 'Todas las entregas').toLowerCase();
      const statusLabel = formatMultiFilterLabel(statusFilter, STATUS_FILTER_LABELS, STATUS_FILTER_ALLOWED, 'Todos los estados').toLowerCase();
      const dateLabel = (range.from || range.to)
        ? ('fechas ' + (range.from || '...') + ' a ' + (range.to || '...'))
        : 'sin rango de fechas';
      const base = [deliveryLabel, statusLabel, dateLabel].join(' · ');

      hint.textContent = total
        ? (total + ' conversaciones visibles' + (Number.isFinite(allCount) ? (' de ' + allCount) : '') + ' · ' + base + '.')
        : ('No hay conversaciones para mostrar con los filtros actuales.');

    }
  }


  function parseQtyFromProductLine(line){
    const m = String(line || '').trim().match(/^(\d+(?:[\.,]\d+)?)\s*x\s+/i);
    if (!m) return 1;
    const n = Number(String(m[1]).replace(',', '.'));
    return Number.isFinite(n) && n > 0 ? n : 1;
  }

  function normalizeProductLine(line){
    return String(line || '').replace(/^(\d+(?:[\.,]\d+)?)\s*x\s+/i, '').trim();
  }

  function foldText(raw){
    return String(raw || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .trim();
  }

  function isDeliveryProductLine(line){
    const name = foldText(normalizeProductLine(line));
    return !!name && (
      name.includes('envio') ||
      name.includes('costo de envio') ||
      name.includes('delivery') ||
      /\\b\\d+\\s*-\\s*\\d+\\s*km\\b/.test(name)
    );
  }

  function rowHasPedido(row){
    const lines = Array.isArray(row?.products) ? row.products.filter(Boolean) : [];
    if (lines.length) return true;
    if (String(row?.fechaEntrega || '').trim() && String(row?.fechaEntrega || '').trim() !== '-') return true;
    if (String(row?.horaEntrega || '').trim() && String(row?.horaEntrega || '').trim() !== '-') return true;
    if (String(row?.entregaLabel || '').trim() && String(row?.entregaLabel || '').trim() !== '-') return true;
    if (String(row?.direccion || '').trim() && String(row?.direccion || '').trim() !== '-') return true;
    return false;
  }

  function isPendingConversation(row){
    if (!row || row.delivered) return false;
    if (normalizeStatus(row?.status) === 'CANCELLED') return false;
    return rowHasPedido(row);
  }

  function getEntregaMode(row){
    const label = foldText(row?.entregaLabel || '');
    if (label.includes('env')) return 'envio';
    if (label.includes('ret')) return 'retiro';
    const lines = Array.isArray(row?.products) ? row.products : [];
    if (lines.some(isDeliveryProductLine)) return 'envio';
    return '';
  }

  function getWeightedQty(line, kind){
    const name = foldText(normalizeProductLine(line));
    if (!name || isDeliveryProductLine(name)) return 0;
    const qty = parseQtyFromProductLine(line);
    if (kind === 'pollo') {
      if (!/\\bpollo\\b/.test(name)) return 0;
      if (/\\bmedio\\s+pollo\\b|\\b1\\/2\\s+pollo\\b/.test(name)) return qty * 0.5;
      if (/\\bcuarto\\s+de\\s+pollo\\b|\\b1\\/4\\s+pollo\\b/.test(name)) return qty * 0.25;
      return qty;
    }
    if (kind === 'papa') {
      if (!/\\bpapa(?:s)?\\b|\\bfrita(?:s)?\\b/.test(name)) return 0;
      const paraMatch = name.match(/\\bpara\\s+(\\d+)\\b/);
      const porciones = paraMatch ? Number(paraMatch[1]) : 0;
      if (porciones > 0) return qty * porciones;
      return qty;
    }
    return 0;
  }

  function sumProductQty(rows, kind){
    return (Array.isArray(rows) ? rows : []).reduce((acc, row) => {
      const pedidoItems = Array.isArray(row?.pedidoItems) ? row.pedidoItems : [];
      if (pedidoItems.length) {
        const itemSum = pedidoItems.reduce((sum, it) => {
          const desc = String(it?.descripcion || '').trim();
          const qty = Number(it?.cantidad || 0);
          if (!desc || !Number.isFinite(qty) || qty <= 0) return sum;
          return sum + getWeightedQty(String(qty).replace(/\.0+$/, '') + ' x ' + desc, kind);
        }, 0);
        return acc + itemSum;
      }
      const lines = Array.isArray(row?.products) ? row.products : [];
      return acc + lines.reduce((sum, line) => sum + getWeightedQty(line, kind), 0);
    }, 0);
  }

  function countByEntrega(rows, mode){
    return (Array.isArray(rows) ? rows : []).filter((row) => getEntregaMode(row) === mode).length;
  }

  function buildBarRows(items){
    const safeItems = (Array.isArray(items) ? items : []).filter(Boolean);
    const max = safeItems.reduce((m, item) => Math.max(m, Number(item?.value || 0)), 0) || 1;
    return safeItems.map((item) => {
      const value = Number(item?.value || 0);
      const width = Math.max(6, Math.round((value / max) * 100));
      return '<div class="bar-row">' +
        '<div class="bar-label">' + escHtml(item.label || '-') + '</div>' +
        '<div class="bar-track"><div class="bar-fill" style="width:' + width + '%"></div></div>' +
        '<div class="bar-value">' + escHtml(String(value)) + '</div>' +
      '</div>';
    }).join('');
  }

  function renderAdminStats(rows, allRows){
    const visible = Array.isArray(rows) ? rows : [];
    const loaded = Array.isArray(allRows) ? allRows : [];
    const pendingAll = loaded.filter(isPendingConversation);
    const completedCount = visible.filter(row => normalizeStatus(row?.status) === 'COMPLETED').length;
    const openCount = visible.filter(row => ['OPEN','IN_PROGRESS'].includes(normalizeStatus(row?.status))).length;
    const envioCount = countByEntrega(pendingAll, 'envio');
    const retiroCount = countByEntrega(pendingAll, 'retiro');
    const polloQty = sumProductQty(pendingAll, 'pollo');
    const papaQty = sumProductQty(pendingAll, 'papa');

    const statsGrid = document.getElementById('statsGrid');
    if (statsGrid) {
      const cards = [
        { value: pendingAll.length, label: 'Pendientes', meta: 'Total no entregado con pedido válido' },
        { value: String(polloQty).replace(/\.0+$/, ''), label: 'Pollos', meta: 'Unidades pendientes totales' },
        { value: String(papaQty).replace(/\.0+$/, ''), label: 'Papas', meta: 'Unidades pendientes totales' },
        { value: envioCount, label: 'A enviar', meta: 'Pendientes totales con envío' },
        { value: retiroCount, label: 'A retirar', meta: 'Pendientes totales para retiro' }
      ];
      statsGrid.innerHTML = cards.map((card) =>
        '<div class="kpi">' +
          '<div class="kpi-value">' + escHtml(String(card.value)) + '</div>' +
          '<div class="kpi-label">' + escHtml(card.label) + '</div>' +
          '<div class="kpi-meta">' + escHtml(card.meta) + '</div>' +
        '</div>'
      ).join('');
    }

    const statusBars = document.getElementById('statusBars');
    if (statusBars) {
      statusBars.innerHTML = buildBarRows([
        { label: 'Completadas', value: completedCount },
        { label: 'Abiertas', value: openCount },
        { label: 'Canceladas', value: visible.filter(row => normalizeStatus(row?.status) === 'CANCELLED').length }
      ]);
    }

    const deliveryBars = document.getElementById('deliveryBars');
    if (deliveryBars) {
      deliveryBars.innerHTML = buildBarRows([
        { label: 'Envío', value: envioCount },
        { label: 'Retiro', value: retiroCount },
        { label: 'Sin definir', value: pendingAll.filter(row => !getEntregaMode(row)).length }
      ]);
    }


    const statsHint = document.getElementById('statsHint');
    if (statsHint) {
      const now = new Date();
      const hhmm = now.toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit' });
      statsHint.textContent = 'Última actualización ' + hhmm + ' · estados visibles sobre lo filtrado · pendientes y entrega sobre el total cargado.';
    }
  }

  function setStatsCollapsed(collapsed){
    const panel = document.getElementById('statsPanel');
    const btn = document.getElementById('btnToggleStats');
    if (!panel || !btn) return;
    panel.classList.toggle('is-collapsed', !!collapsed);
    btn.textContent = collapsed ? 'Mostrar resumen' : 'Ocultar resumen';
    try { localStorage.setItem('adminStatsCollapsed', collapsed ? '1' : '0'); } catch {}
  }

  function initAdminUiState(){
    let collapsed = false;
    try { collapsed = localStorage.getItem('adminStatsCollapsed') === '1'; } catch {}
    setStatsCollapsed(collapsed);
    document.getElementById('btnToggleStats')?.addEventListener('click', () => {
      const next = !document.getElementById('statsPanel')?.classList.contains('is-collapsed');
      setStatsCollapsed(next);
    });
  }

  function renderEntregaPill(raw){
    const label = String(raw || '-').trim() || '-';
    const cls = label.toLowerCase().includes('env') ? 'delivery-pill is-envio' : (label.toLowerCase().includes('ret') ? 'delivery-pill is-retiro' : 'delivery-pill');
    return '<span class="' + cls + '">' + escHtml(label) + '</span>';
  }

  function renderDistanceBadge(raw){
    if (raw === undefined || raw === null || raw === '') return '<span class="distance-badge">-</span>';
    return '<span class="distance-badge">' + escHtml(raw) + '</span>';
  }

  function renderProductsCell(lines){
    const safeLines = Array.isArray(lines) ? lines.filter(Boolean) : [];
    if (!safeLines.length) return '<span class="cell-subtle">-</span>';
    return '<div class="cell-products">' + safeLines.map(line => '<div>' + escHtml(line) + '</div>').join('') + '</div>';
  }

  let adminTablePollTimer = null;
  let adminTableLoadDebounceTimer = null;
  let lastAdminTableFingerprint = "";
  let adminLastUserInteractionAt = 0;
  function markAdminInteraction(){
    adminLastUserInteractionAt = Date.now();
  }

  function adminUserIsInteracting(){
    return (Date.now() - adminLastUserInteractionAt) < 5000;
  }

  function buildAdminTableFingerprint(allRows, visibleRows){
    const all = Array.isArray(allRows) ? allRows : [];
    const visible = Array.isArray(visibleRows) ? visibleRows : [];
    return JSON.stringify({
      totalAll: all.length,
      totalVisible: visible.length,
      all: all.map(c => [
        c._id || '',
        c.status || '',
        c.lastAt || '',
        c.delivered ? 1 : 0,
        c.kitchenSent ? 1 : 0,
        c.fechaEntrega || '',
        c.horaEntrega || '',
        c.distanceKm ?? '',
        c.contactName || '',
        c.direccion || '',
        Array.isArray(c.products) ? c.products.join('¦') : ''
      ].join('|'))
    });
  }

  function debounceAdminTableLoad(ms = 250, opts = {}){
    if (adminTableLoadDebounceTimer) {
      clearTimeout(adminTableLoadDebounceTimer);
    }
    adminTableLoadDebounceTimer = setTimeout(() => {
      loadTable(opts);
    }, ms);
  }

  function startAdminTablePolling(){
    stopAdminTablePolling();
    adminTablePollTimer = setInterval(async () => {
      try {
        if (document.hidden) return;
        if (currentConvId) return;
        if (adminUserIsInteracting()) return;
        await loadTable({ soft: true, preserveScroll: true });
      } catch (e) {
        console.warn('admin table polling error', e);
      }
    }, 15000);
  }

  function stopAdminTablePolling(){
    if (adminTablePollTimer) {
      clearInterval(adminTablePollTimer);
      adminTablePollTimer = null;
    }
  }
  // =========================
  // Tabla conversaciones (ÚNICA versión)
  // =========================
  async function loadTable(opts = {}){
    const soft = !!opts.soft;
    const preserveScroll = !!opts.preserveScroll;
    const tableWrap = document.querySelector('.table-wrap');
    const prevScrollTop = (preserveScroll && tableWrap) ? tableWrap.scrollTop : 0;
    const prevScrollLeft = (preserveScroll && tableWrap) ? tableWrap.scrollLeft : 0;
    const tb = document.querySelector('#tbl tbody');
    try{
      const deliveredFilter = getDeliveryFilterValue();
      const statusFilter = getStatusFilterValue();
      const textFilter = getTextFilterValue();
      const range = getDateRangeValue();
      saveAdminFilterState();

      let url = '/api/logs/conversations?limit=200';
      if (deliveredFilter.length === 1 && deliveredFilter[0] === 'delivered') url += '&delivered=true';
      else if (deliveredFilter.length === 1 && deliveredFilter[0] === 'pending') url += '&delivered=false';

      if (tb && !soft) {
        tb.innerHTML = '<tr><td colspan="13" style="text-align:center;color:#64748b;padding:20px">Cargando conversaciones…</td></tr>';
      }

      const r = await fetch(url, { cache: 'no-store' });
      const data = await r.json().catch(()=>[]);
      if (!tb) return;
      const filtered = (Array.isArray(data) ? data : []).filter(c =>
        convMatchesDelivered(c, deliveredFilter) &&
        convMatchesText(c, textFilter) &&
        convMatchesStatus(c, statusFilter) &&
        convMatchesDateRange(c, range.from, range.to)
      );
      const rows = sortAdminConversations(filtered, statusFilter);
      const fingerprint = buildAdminTableFingerprint(Array.isArray(data) ? data : [], rows);

      if (soft && fingerprint === lastAdminTableFingerprint) {
        return;
      }
      lastAdminTableFingerprint = fingerprint;


      tb.innerHTML = '';
      updateAdminSummary(rows, Array.isArray(data) ? data.length : 0);
      renderAdminStats(rows, Array.isArray(data) ? data : []);

      for(const c of rows){
        const tr = document.createElement('tr');
        if (c.delivered) tr.classList.add('delivered-row');
        if (c.kitchenSent) tr.classList.add('kitchen-row');
        const act = formatActivity(c.lastAt);
        const direccion = String(c.direccion || '-').trim() || '-';
        const nombre = String(c.contactName || '-').trim() || '-';
        tr.innerHTML =
          '<td data-label="Actividad">' +
            '<span class="cell-strong">' + escHtml(act.date) + '</span>' +
            '<span class="cell-subtle">' + escHtml(act.time || '') + '</span>' +
          '</td>' +
          '<td data-label="Teléfono"><span class="cell-strong">' + escHtml(c.waId || '-') + '</span></td>' +
          '<td data-label="Nombre" title="' + escHtml(nombre) + '"><span class="cell-strong cell-ellipsis">' + escHtml(nombre) + '</span></td>' +
          '<td data-label="Productos">' + renderProductsCell(c.products) + '</td>' +
          '<td data-label="Entrega">' + renderEntregaPill(c.entregaLabel || '-') + '</td>' +
          '<td data-label="Dirección" title="' + escHtml(direccion) + '"><span class="cell-address">' + escHtml(direccion) + '</span></td>' +
          '<td data-label="Distancia">' + renderDistanceBadge(c.distanceKm) + '</td>' +
          '<td data-label="Día"><span class="cell-ellipsis">' + escHtml(c.fechaEntrega || '-') + '</span></td>' +
          '<td data-label="Hora"><span class="cell-strong">' + escHtml(c.horaEntrega || '-') + '</span></td>' +
          '<td data-label="Estado">' + renderStatusBadge(c.status, c.kitchenSent) + '</td>' +
          '<td data-label="Coc." style="text-align:center">' +
            '<input class="kitchenChk" type="checkbox" data-id="' + escHtml(c._id) + '" ' +
            (c.kitchenSent ? 'checked' : '') + ' title="Enviado a cocina" />' +
          '</td>' +
          '<td data-label="Ent." style="text-align:center">' +
            '<input class="delivChk" type="checkbox" data-id="' + escHtml(c._id) + '" ' +
            (c.delivered ? 'checked' : '') + ' title="Entregado" />' +
          '</td>' +
          '<td data-label="Acciones" style="overflow:visible">' +
            '<div class="actions">' +
              '<button class="btn btn-primary btn-detail btn-icon" data-conv="' + escHtml(c._id) + '" title="Ver conversación">👁</button>' +
              '<button class="btn btn-soft btn-icon" data-pedido="' + escHtml(c._id) + '" title="Ver pedido">🧾</button>' +
              '<button class="btn btn-soft btn-icon" data-print="' + escHtml(c._id) + '" title="Imprimir ticket">🖨️</button>' +
            '</div>' +
          '</td>';

        tb.appendChild(tr);
      }

      if (preserveScroll && tableWrap) {
        tableWrap.scrollTop = prevScrollTop;
        tableWrap.scrollLeft = prevScrollLeft;
      }

      tb.querySelectorAll('button[data-conv]').forEach(b=>{
        b.addEventListener('click',()=>openDetailModal(b.getAttribute('data-conv')));
      });
      tb.querySelectorAll('button[data-pedido]').forEach(b=>{
        b.addEventListener('click',()=>openPedidoModal(b.getAttribute('data-pedido')));
      });
      tb.querySelectorAll('button[data-print]').forEach(b=>{
        b.addEventListener('click',()=>openTicketModal(b.getAttribute('data-print')));
      });
      tb.querySelectorAll('input.kitchenChk').forEach(chk=>{
        chk.addEventListener('change', async()=>{
          const convId = chk.getAttribute('data-id');
          if (!convId) return;
          const flag = chk.checked;
          chk.disabled = true;
          try{
            const rr = await fetch('/api/admin/conversation-kitchen', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ convId, kitchenSent: flag })
            });
            let ok = rr.ok;
            if (ok) {
              const jj = await rr.json().catch(()=>({}));
              ok = !!jj.ok;
            } else {
              const actual = await verifyKitchen(convId);
              ok = (actual === flag);
            }
            if (!ok) throw new Error('not_updated');
            await loadTable({ soft: true, preserveScroll: true });
          }catch(e){
            chk.checked = !flag;
            alert('No se pudo actualizar el estado de cocina');
          }finally{
            chk.disabled = false;
          }
        });
      });
      tb.querySelectorAll('input.delivChk').forEach(chk=>{
        chk.addEventListener('change', async()=>{
          const convId = chk.getAttribute('data-id');
          if (!convId) return;
          const flag = chk.checked;
          chk.disabled = true;
          try{
            const rr = await fetch('/api/admin/conversation-delivered', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ convId, delivered: flag })
            });
            let ok = rr.ok;
            if (!ok) {
              const actual = await verifyDelivered(convId);
              ok = (actual === flag);
            }
            if (!ok) throw new Error('not_updated');

           await loadTable({ soft: true, preserveScroll: true });
          }catch(e){
            chk.checked = !flag;
            alert('No se pudo actualizar el estado de entrega');
          }finally{
            chk.disabled = false;
          }
        });
      });

     if(!rows.length){
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="13" style="text-align:center;color:#64748b;padding:26px">Sin conversaciones para mostrar</td>';
        tb.appendChild(tr);
      }

    }catch(e){
      console.error('loadTable error', e);
      renderAdminStats([]);
      if (tb) tb.innerHTML = '<tr><td colspan="13" style="text-align:center;color:#b91c1c;padding:20px">No se pudo cargar la tabla</td></tr>';
    }
  }

  

  // =========================
  // Bind general
  // =========================
  document.getElementById('dateFrom')?.addEventListener('change', ()=> loadTable({ soft: true, preserveScroll: true }));
  document.getElementById('dateTo')?.addEventListener('change', ()=> loadTable({ soft: true, preserveScroll: true }));
  document.getElementById('qFilter')?.addEventListener('input', ()=>{
    markAdminInteraction();
    debounceAdminTableLoad(250, { soft: true, preserveScroll: true });
  });

  
  document.getElementById('btnReload')?.addEventListener('click', ()=> loadTable());
  document.getElementById('btnToday')?.addEventListener('click', ()=>{
    markAdminInteraction();
    const today = todayIso();
    const from = document.getElementById('dateFrom');
    const to = document.getElementById('dateTo');
    if (from) from.value = today;
    if (to) to.value = today;
    loadTable({ soft: true, preserveScroll: true });
  });
  document.getElementById('btnClearDates')?.addEventListener('click', ()=>{
    markAdminInteraction();
    const from = document.getElementById('dateFrom');
    const to = document.getElementById('dateTo');
    if (from) from.value = '';
    if (to) to.value = '';
    loadTable({ soft: true, preserveScroll: true });
  });
  document.getElementById('qFilter')?.addEventListener('keydown', (e)=>{
    markAdminInteraction();

    if (e.key === 'Enter') {
      e.preventDefault();
       loadTable({ soft: true, preserveScroll: true });
    }
  });

  ['mousedown','keydown','touchstart','wheel','focusin'].forEach(evt => {
    document.addEventListener(evt, (e) => {
      const el = e.target;
      if (!el || !el.closest) return;
      if (
        el.closest('.toolbar-card') ||
        el.closest('.table-card') ||
        el.closest('#modalRoot') ||
        el.closest('#pedidoModalBackdrop')
      ) {
        markAdminInteraction();
      }
    }, true);
  });

  // Bind modal manual/chat
  document.addEventListener('DOMContentLoaded', () => {
    const tBtn = modalToggleManualBtn();
    const sBtn = modalSendBtn();
    const ta = modalReplyText();

    if (tBtn) tBtn.addEventListener('click', toggleModalManual);
    if (sBtn) sBtn.addEventListener('click', sendModalMessage);

    if (ta) {
      ta.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
          e.preventDefault();
          sendModalMessage();
        }
      });
    }
  });

  // Carga inicial + refresco tabla
  initAdminFilters();
  initAdminFilterPopovers();
  initAdminUiState();
  loadTable();
  startAdminTablePolling();
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) loadTable({ soft: true, preserveScroll: true });
  });


  </script>
</body>
</html>`;
    res.setHeader("content-type", "text/html; charset=utf-8");
    res.send(html);
  } catch (e) {
    console.error("GET /admin error:", e);
    res.status(500).send("Error interno");
  }
});


 // ---------- Ventana de detalle de conversación ----------
 app.get("/admin/conversation", async (req, res) => {
   try {
     const { convId, waId } = req.query;
     if (!convId && !waId) return res.status(400).send("convId o waId requerido");
     const qs = convId ? ("convId="+encodeURIComponent(convId)) : ("waId="+encodeURIComponent(waId));

     const convIdJs = convId ? String(convId).replace(/\\/g, "\\\\").replace(/'/g, "\\'") : "";
     const waIdJs = waId ? String(waId).replace(/\\/g, "\\\\").replace(/'/g, "\\'") : "";

     res.setHeader("content-type","text/html; charset=utf-8");
     res.end(`<!doctype html><html lang="es"><head>
       <meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
       <title>Detalle de conversación</title>
       <style>
         body{font-family:system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji"; margin:20px;}
         .msg{border:1px solid #ddd;border-radius:8px;padding:10px;margin-bottom:10px}
         .role-user{background:#f0fafc}
         .role-assistant{background:#f8f6ff}
         small{color:#666}
         pre{white-space:pre-wrap}
         .toolbar{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;align-items:center}
         .btn{padding:6px 10px; border:1px solid #333; background:#fff; border-radius:4px; cursor:pointer}
         .badge{padding:4px 8px;border-radius:999px;font-size:12px}
         .badge-bot{background:#e3f7e3;color:#145214}
         .badge-manual{background:#ffe4e1;color:#8b0000}
         #chatBox{margin-top:16px;border-top:1px solid #ddd;padding-top:10px;display:flex;flex-direction:column;gap:6px}
         #replyText{width:100%;min-height:70px;font-family:inherit;font-size:14px;padding:6px 8px}
         .chat-actions{display:flex;align-items:center;gap:8px;justify-content:flex-end}
         .muted{color:#666;font-size:12px}
       </style></head><body>
       <div class="toolbar">
         <button class="btn" onclick="window.print()">Imprimir</button>
         <button class="btn" onclick="location.reload()">Recargar</button>
         <span id="manualBadge" class="badge badge-bot">Cargando estado…</span>
         <span id="channelInfo" class="muted" style="margin-left:8px"></span>
         <button class="btn" id="toggleManualBtn">Tomar chat (pausar bot)</button>
       </div>
       <div id="root"></div>
       <div id="chatBox">
         <textarea id="replyText" placeholder="Escribí un mensaje para el cliente… (Ctrl/⌘+Enter para enviar)"></textarea>
         <div class="chat-actions">
           <button class="btn" id="sendBtn">Enviar</button>
           <span class="muted">El bot solo se pausa si el chat está en modo manual.</span>
         </div>
       </div>
       <script>
         const CONV_ID = '${convIdJs}';
         const WA_ID = '${waIdJs}';
         const QS = '${qs}';
         let meta = null;
         let manualOpen = false;

         async function loadMessages(){
           const r = await fetch('/api/logs/messages?' + QS);
          const data = await r.json();
           const root = document.getElementById('root');
           root.innerHTML='';
           for (const m of data){
             const d=document.createElement('div');
             d.className='msg role-'+m.role;
             const when = new Date(m.createdAt).toLocaleString();
             const small = document.createElement('small');
             small.textContent = '['+when+'] '+m.role;
             d.appendChild(small);

             const pre = document.createElement('pre');
             pre.textContent = (m.content||'');
             d.appendChild(pre);

                          // media
             try {
               const md = m.media;
               if (md && md.url) {
                 const kind = String(md.kind||'').toLowerCase();
                 const mime = String(md.mime||'').toLowerCase();
                 const filename = String(md.filename || 'Archivo').trim() || 'Archivo';
                 const fnameLower = filename.toLowerCase();
                 const url = String(md.url||'');
                 const dl = url + (url.includes('?') ? '&' : '?') + 'download=1';

                 const isImage = (kind === 'image') || mime.startsWith('image/');
                 const isAudio = (kind === 'audio') || mime.startsWith('audio/');
                 const isVideo = (kind === 'video') || mime.startsWith('video/');
                 const isPdf = (kind === 'pdf') || mime.includes('pdf') || fnameLower.endsWith('.pdf');

                 const links = document.createElement('div');
                 links.style.display = 'flex';
                 links.style.gap = '10px';
                 links.style.marginTop = '4px';
                 links.style.alignItems = 'center';

                 const open = document.createElement('a');
                 open.href = url;
                 open.textContent = 'Abrir';
                 open.target = '_blank';
                 open.rel = 'noopener';
                 open.style.display = 'inline-block';
                 open.style.fontSize = '12px';

                 const adl = document.createElement('a');
                 adl.href = dl;
                 adl.textContent = 'Descargar';
                 adl.style.display = 'inline-block';
                 adl.style.fontSize = '12px';

                 if (isImage) {
                   const a = document.createElement('a');
                   a.href = url; a.target = '_blank'; a.rel = 'noopener';
                   const img = document.createElement('img');
                   img.src = url;
                   img.style.maxWidth = '360px';
                   img.style.borderRadius = '10px';
                   img.style.display = 'block';
                   img.style.marginTop = '6px';
                   a.appendChild(img);
                   d.appendChild(a);

                   links.appendChild(adl);
                   d.appendChild(links);
                 } else if (isAudio) {
                   const audio = document.createElement('audio');
                   audio.controls = true;
                   audio.src = url;
                   audio.style.display = 'block';
                   audio.style.marginTop = '6px';
                   d.appendChild(audio);

                   links.appendChild(open);
                   links.appendChild(adl);
                   d.appendChild(links);
                 } else if (isVideo) {
                   const video = document.createElement('video');
                   video.controls = true;
                   video.src = url;
                   video.style.maxWidth = '360px';
                   video.style.borderRadius = '10px';
                   video.style.display = 'block';
                   video.style.marginTop = '6px';
                   d.appendChild(video);

                   links.appendChild(open);
                   links.appendChild(adl);
                   d.appendChild(links);
                 } else if (isPdf) {
                   const frame = document.createElement('iframe');
                   frame.src = url;
                   frame.style.width = '360px';
                   frame.style.maxWidth = '100%';
                   frame.style.height = '420px';
                   frame.style.border = '0';
                   frame.style.borderRadius = '10px';
                   frame.style.display = 'block';
                   frame.style.marginTop = '6px';
                   d.appendChild(frame);

                   links.appendChild(open);
                   links.appendChild(adl);
                   d.appendChild(links);
                 } else {
                   const a = document.createElement('a');
                   a.href = dl;
                   a.textContent = '📎 ' + filename;
                   a.style.display = 'inline-block';
                   a.style.marginTop = '6px';
                   d.appendChild(a);

                   links.appendChild(open);
                   links.appendChild(adl);
                   d.appendChild(links);
                 }
               }
             } catch {}

             root.appendChild(d);
           }
           if(!data.length){ root.innerHTML='<p class="muted">Sin mensajes para mostrar</p>'; }
           root.scrollTop = root.scrollHeight || 0;
         }

         function updateManualUI(){
           const badge = document.getElementById('manualBadge');
           const btn = document.getElementById('toggleManualBtn');
           if (!badge || !btn) return;
           if (manualOpen) {
             badge.textContent = 'Modo MANUAL: bot pausado';
             badge.classList.remove('badge-bot');
             badge.classList.add('badge-manual');
             btn.textContent = 'Liberar al bot';
           } else {
             badge.textContent = 'Modo BOT automático';
             badge.classList.remove('badge-manual');
             badge.classList.add('badge-bot');
             btn.textContent = 'Tomar chat (pausar bot)';
           }
         }

         async function loadMeta(){
           const url = new URL('/api/admin/conversation-meta', window.location.origin);
           if (CONV_ID) url.searchParams.set('convId', CONV_ID);
           else if (WA_ID) url.searchParams.set('waId', WA_ID);
           const r = await fetch(url.toString());
           if (!r.ok) return;
           meta = await r.json();
           manualOpen = !!(meta && meta.manualOpen);
           updateManualUI();
           const ch = (meta && (meta.displayPhoneNumber || meta.phoneNumberId))
             ? (meta.displayPhoneNumber || meta.phoneNumberId)
             : "";
           const chEl = document.getElementById('channelInfo');
           if (chEl) chEl.textContent = ch ? ('Canal: ' + ch) : '';
         }

         async function toggleManual(){
           if (!meta && !CONV_ID && !WA_ID) return;
           const payload = { manualOpen: !manualOpen };
           if (meta && meta.convId) payload.convId = meta.convId;
           else if (CONV_ID) payload.convId = CONV_ID;
           else if (WA_ID) payload.waId = WA_ID;

           const r = await fetch('/api/admin/conversation-manual', {
             method: 'POST',
             headers: { 'Content-Type': 'application/json' },
             body: JSON.stringify(payload)
           });
           const j = await r.json().catch(()=>({}));
           if (!r.ok || !j.ok) {
             alert('No se pudo cambiar el estado manual.');
             return;
           }
           manualOpen = !!j.manualOpen;
           updateManualUI();
         }

         async function sendMessage(){
           const ta = document.getElementById('replyText');
           const text = (ta.value || '').trim();
           if (!text) return;
           const payload = { text };
           if (meta && meta.convId) payload.convId = meta.convId;
           else if (CONV_ID) payload.convId = CONV_ID;
           else if (WA_ID) payload.waId = WA_ID;

           const r = await fetch('/api/admin/send-message', {
             method: 'POST',
             headers: { 'Content-Type': 'application/json' },
             body: JSON.stringify(payload)
           });
           const j = await r.json().catch(()=>({}));
           if (!r.ok || !j.ok) {
             alert('Error al enviar el mensaje.');
             return;
           }
           ta.value = '';
           await loadMessages();
         }

         document.getElementById('toggleManualBtn').addEventListener('click', toggleManual);
         document.getElementById('sendBtn').addEventListener('click', sendMessage);
         document.getElementById('replyText').addEventListener('keydown', function(e){
           if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
             e.preventDefault();
             sendMessage();
           }
         });

         loadMessages();
         loadMeta();
       </script>
     </body></html>`);
   } catch (e) {
     console.error("GET /admin/conversation error:", e);
     res.status(500).send("Error interno");
   }
 });

// ---------- Ventana separada de mensajes ----------
app.get("/admin/messages/:convId", async (req, res) => {
  try {
    const convId = String(req.params.convId || "").trim();
    if (!convId) return res.status(400).send("convId requerido");
    const msgs = await getConversationMessagesByConvId(convId, 1000);
    const html = `<!doctype html><html><head><meta charset="utf-8"/>
      <meta name="viewport" content="width=device-width, initial-scale=1"/>
      <title>Mensajes</title>
      <style>
        body{font-family:system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Helvetica, Arial; margin:16px}
        .msg{border:1px solid #ddd; border-radius:8px; padding:10px; margin-bottom:10px}
        .role-user{background:#f0fafc}.role-assistant{background:#f8f6ff}
        small{color:#666} pre{white-space:pre-wrap; margin:4px 0 0}
      </style></head><body>
      <h3>Mensajes</h3>
      ${msgs.map(m => `
        <div class="msg role-${m.role}">
          <small>[${new Date(m.ts || m.createdAt).toLocaleString()}] ${m.role}</small>
          <pre>${(m.content || "").replace(/</g,"&lt;")}</pre>
        </div>`).join("") || "<em>Sin mensajes</em>"}
      </body></html>`;
    res.setHeader("content-type", "text/html; charset=utf-8");
    res.send(html);
  } catch (e) {
    console.error("GET /admin/messages error:", e);
    res.status(500).send("Error interno");
 }
});







// ---------- Ticket imprimible (80mm) ----------
app.get("/admin/ticket/:convId", async (req, res) => {
  try {
    const convId = String(req.params.convId || "").trim();
    if (!convId) return res.status(400).send("convId requerido");
    const db = await getDb();
    const convObjectId = new ObjectId(convId);

    // Traemos la conversación para nombre / teléfono
    let pedido = null;
    let nombre = "";
    let waId = "";

    try {
      const conv = await db
        .collection("conversations")
        .findOne(withTenant({ _id: convObjectId }));
      waId = conv?.waId || "";
      nombre = conv?.contactName || "";
    } catch (e) {
      console.error("GET /admin/ticket conv error:", e?.message || e);
    }

    // 1) Intentar leer el pedido desde la colección orders (modo actual)
    try {
      const order = await db
        .collection("orders")
        .findOne(
          withTenant({ conversationId: convObjectId }),
          { sort: { createdAt: -1 } }
        );
      if (order && order.pedido) {
        pedido = order.pedido;
      }
    } catch (e) {
      console.error("GET /admin/ticket orders error:", e?.message || e);
    }

    // 2) Si por algún motivo no encontramos en orders,
    //    caemos al método viejo: buscar el JSON en los mensajes.
    if (!pedido) {
      const msgs = await getConversationMessagesByConvId(convId, 1000);
      for (let i = msgs.length - 1; i >= 0; i--) {
        const m = msgs[i];
        if (m.role !== "assistant") continue;
        const s = String(m.content || "").trim();
        try {
          const j = JSON.parse(s);
          if (j && j.Pedido && Array.isArray(j.Pedido.items)) {
            pedido = j.Pedido;
            break;
          }
        } catch {}
      }
    }

    const items = (pedido?.items || []).map((it) => ({
      desc: String(it.descripcion || "").trim(),
      qty: Number(it.cantidad || 0),
    }));

    const total = Number(pedido?.total_pedido || 0);
    const fecha = new Date().toLocaleString();

    // ===== Modalidad de entrega y dirección =====
    const entregaRaw = String(pedido?.Entrega || "").trim();
    let modalidadText = "-";
    let direccionText = "-";

    if (entregaRaw) {
      // Soportar valores nuevos ("domicilio"/"retiro") y viejos ("Envío (Moreno 2862)")
      if (/domicilio|env[ií]o|delivery/i.test(entregaRaw)) {
        modalidadText = "Envío";
      } else if (/retiro|retir/i.test(entregaRaw)) {
        modalidadText = "Retiro";
      } else {
        // fallback: mostramos lo que vino
        modalidadText = entregaRaw;
      }
    }

    // Dirección: primero miramos Pedido.Domicilio
    if (pedido?.Domicilio) {
      const dom = pedido.Domicilio;
      if (typeof dom === "string") {
        direccionText = dom;
      } else if (typeof dom === "object") {
        direccionText = String(
          dom.direccion || dom.calle || ""
        ).trim();
        if (!direccionText) {
          // último recurso: stringify para no perder info
          direccionText = JSON.stringify(dom);
        }
      }
    } else if (entregaRaw) {
      // Soportar formato viejo: "Envío (Moreno 2862)"
      const m = entregaRaw.match(/\((.+)\)/);
      if (m) direccionText = m[1].trim();
    }
    const html = `<!doctype html>
<html><head><meta charset="utf-8"/><title>Ticket</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
  @media print { .no-print{display:none} body{margin:0} }
  body{font-family: ui-monospace, Menlo, Consolas, monospace; width: 80mm; margin:0; padding:8px}
  h3{margin:0 0 8px 0; font-size:14px; text-align:center}
  .line{display:flex; justify-content:space-between; font-size:12px; margin:2px 0}
  .sep{border-top:1px dashed #000; margin:6px 0}
  .foot{font-size:11px; text-align:center; margin-top:8px}
  .btn{padding:6px 10px; border:1px solid #333; border-radius:6px; background:#fff; cursor:pointer}
  .warn{font-size:11px; margin-top:6px}
  </style></head>
<body>
  <h3>Comanda Cliente</h3>
    <div class="line"><span>Fecha</span><span>${fecha}</span></div>
   <div class="line"><span>Teléfono</span><span>${waId || "-"}</span></div>
   <div class="line"><span>Nombre</span><span>${(nombre||"-")}</span></div>
   <div class="line"><span>Entrega</span><span>${modalidadText}</span></div>
   <div class="line"><span>Dirección</span><span>${direccionText}</span></div>
   <div class="sep"></div>
  ${items.length ? items.map(i => `<div class="line"><span>${i.qty} x ${i.desc}</span><span></span></div>`).join("") : "<div class='line'><em>Sin ítems detectados</em></div>"}
  <div class="sep"></div>
  <div class="line"><strong>Total</strong><strong>$ ${total.toLocaleString("es-AR")}</strong></div>
  ${(items.some(x => /milanesa/i.test(x.desc)) ? `<div class="warn">* Las milanesas se pesan al entregar; el precio se informa al momento de la entrega.</div>` : ``)}
  <div class="foot">¡Gracias por tu compra!</div>
</body></html>`;
    res.setHeader("content-type", "text/html; charset=utf-8");
    res.send(html);
  } catch (e) {
    console.error("GET /admin/ticket error:", e);
    res.status(500).send("Error interno");
  }
});




// GET /api/products  → lista (activos por defecto; ?all=true para todos)
app.get("/api/products", async (req, res) => {
  try {
    const db = await getDb();
    const tenant = resolveTenantId(req);
    const q = req.query.all === "true" ? {} : { active: { $ne: false } };
    if (tenant) q.tenantId = tenant; else if (TENANT_ID) q.tenantId = TENANT_ID;
    const items = await db.collection("products")
      .find(q).sort({ createdAt: -1, descripcion: 1 }).toArray();
    res.json(items.map(it => ({ ...it, _id: String(it._id) })));
  } catch (e) {
    console.error("GET /api/products error:", e);
    res.status(500).json({ error: "internal" });
  }
});

// POST /api/products  → crear
app.post("/api/products", async (req, res) => {
  try {
    const db = await getDb();
    let { descripcion, tag, importe, cantidad, observacion, active } = req.body || {};
    descripcion = String(descripcion || "").trim();
    tag = String(tag || "").trim();
    observacion = String(observacion || "").trim();
    if (typeof active !== "boolean") active = !!active;
    let imp = null;
    if (typeof importe === "number") imp = importe;
    else if (typeof importe === "string") {
      const n = Number(importe.replace(/[^\d.,-]/g, "").replace(",", "."));
      imp = Number.isFinite(n) ? n : null;
    }
    // cantidad (stock/limite) opcional
    // - si viene vacío, no se guarda
    // - si viene numérico, se guarda como entero
    let qty = null;
    if (typeof cantidad === "number") {
      qty = Number.isFinite(cantidad) ? Math.trunc(cantidad) : null;
    } else if (typeof cantidad === "string") {
      const t = cantidad.trim();
      if (t) {
       // Solo entero (permitimos que el usuario cargue "10" o "10,0")
        const n = parseInt(t.replace(/[^\d-]/g, ""), 10);
        qty = Number.isFinite(n) ? n : null;
      }
    }

    if (!descripcion) return res.status(400).json({ error: "descripcion requerida" });
    const now = new Date();
    const tenant = resolveTenantId(req);
    const doc = { tenantId: (tenant || TENANT_ID || DEFAULT_TENANT_ID || null), descripcion, observacion, active, createdAt: now, updatedAt: now };
    if (tag) doc.tag = tag;
    if (qty !== null) doc.cantidad = qty;
    if (imp !== null) doc.importe = imp;
    const ins = await db.collection("products").insertOne(doc);
    res.json({ ok: true, _id: String(ins.insertedId) });
  } catch (e) {
    console.error("POST /api/products error:", e);
    res.status(500).json({ error: "internal" });
  }
});

// PUT /api/products/:id  → actualizar
app.put("/api/products/:id", async (req, res) => {
  try {
    const db = await getDb();
    const { id } = req.params;
    const upd = {};
    ["descripcion","tag","observacion","active","importe","cantidad"].forEach(k => {
      if (req.body[k] !== undefined) upd[k] = req.body[k];
    });
    if (upd.tag !== undefined) {
      upd.tag = String(upd.tag || "").trim();
    }
    if (upd.importe !== undefined && typeof upd.importe === "string") {
      const n = Number(upd.importe.replace(/[^\d.,-]/g, "").replace(",", "."));
      upd.importe = Number.isFinite(n) ? n : undefined;
    }
    // cantidad: permitir "" para limpiar (null)
    if (upd.cantidad !== undefined) {
      if (typeof upd.cantidad === "string") {
        const t = upd.cantidad.trim();
        if (!t) {
          upd.cantidad = null;
        } else {
          const n = parseInt(t.replace(/[^\d-]/g, ""), 10);
          upd.cantidad = Number.isFinite(n) ? n : null;
        }
      } else if (typeof upd.cantidad === "number") {
        upd.cantidad = Number.isFinite(upd.cantidad) ? Math.trunc(upd.cantidad) : null;
      } else {
        upd.cantidad = null;
      }
    }
    if (Object.keys(upd).length === 0) return res.status(400).json({ error: "no_fields" });
    upd.updatedAt = new Date();
    const tenant = resolveTenantId(req);
    const filter = { _id: new ObjectId(String(id)) };
    if (tenant) filter.tenantId = tenant; else if (TENANT_ID) filter.tenantId = TENANT_ID;
    const result = await db.collection("products").updateOne(filter, { $set: upd });
    if (!result.matchedCount) return res.status(404).json({ error: "not_found" });
    res.json({ ok: true });
  } catch (e) {
    console.error("PUT /api/products/:id error:", e);
    res.status(500).json({ error: "internal" });
  }
});

// DELETE /api/products/:id  → eliminar
app.delete("/api/products/:id", async (req, res) => {
  try {
    const db = await getDb();
    const { id } = req.params;
    const tenant = resolveTenantId(req);
    const filter = { _id: new ObjectId(String(id)) };
    if (tenant) filter.tenantId = tenant; else if (TENANT_ID) filter.tenantId = TENANT_ID;
    const result = await db.collection("products").deleteOne(filter);
    if (!result.deletedCount) return res.status(404).json({ error: "not_found" });
    res.json({ ok: true });
  } catch (e) {
    console.error("DELETE /api/products/:id error:", e);
    res.status(500).json({ error: "internal" });
  }
});

// POST /api/products/:id/inactivate  → inactivar
app.post("/api/products/:id/inactivate", async (req, res) => {
  try {
    const db = await getDb();
    const { id } = req.params;
    const tenant = resolveTenantId(req);
    const filter = { _id: new ObjectId(String(id)) };
    if (tenant) filter.tenantId = tenant; else if (TENANT_ID) filter.tenantId = TENANT_ID;
    const result = await db.collection("products").updateOne(filter, { $set: { active: false, updatedAt: new Date() } });
    if (!result.matchedCount) return res.status(404).json({ error: "not_found" });
    res.json({ ok: true });
  } catch (e) {
    console.error("POST /api/products/:id/inactivate error:", e);
    res.status(500).json({ error: "internal" });
  }
});

// POST /api/products/:id/reactivate  → reactivar
app.post("/api/products/:id/reactivate", async (req, res) => {
  try {
    const db = await getDb();
    const { id } = req.params;
    const tenant = resolveTenantId(req);
    const filter = { _id: new ObjectId(String(id)) };
    if (tenant) filter.tenantId = tenant; else if (TENANT_ID) filter.tenantId = TENANT_ID;
    const result = await db.collection("products").updateOne(filter, { $set: { active: true, updatedAt: new Date() } });
    if (!result.matchedCount) return res.status(404).json({ error: "not_found" });
    res.json({ ok: true });
  } catch (e) {
    console.error("POST /api/products/:id/reactivate error:", e);
    res.status(500).json({ error: "internal" });
  }
});

// GET /productos  → UI HTML mejorada para administrar
app.get("/productos", async (req, res) => {
  try {
    const db = await getDb();
    const verTodos = req.query.all === "true";
    const tenant = resolveTenantId(req);
    const filtro = verTodos ? {} : { active: { $ne: false } };
    if (tenant) filtro.tenantId = tenant; else if (TENANT_ID) filtro.tenantId = TENANT_ID;
    const productos = await db.collection("products").find(filtro).sort({ active: -1, descripcion: 1, createdAt: -1 }).toArray();

    const escAttr = (v) => String(v ?? "").replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
    const escText = (v) => String(v ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;");
    const initialRows = productos.map(p => `<tr data-id="${p._id}">
        <td class="col-desc"><input class="descripcion" type="text" value="${escAttr(p.descripcion || "")}" placeholder="Descripción del producto" /></td>
        <td class="col-tag"><input class="tag" type="text" value="${escAttr(p.tag || "")}" placeholder="Tag" /></td>
        <td class="col-price"><input class="importe" type="number" step="0.01" value="${escAttr(p.importe ?? "")}" placeholder="0" /></td>
        <td class="col-qty"><input class="cantidad" type="number" step="1" value="${escAttr(p.cantidad ?? "")}" placeholder="0" /></td>
        <td class="col-obs"><textarea class="observacion" placeholder="Observaciones, categoría, presentación...">${escText(p.observacion || "")}</textarea></td>
       <td class="col-active">
          <label class="switch">
            <input class="active" type="checkbox" ${p.active !== false ? "checked" : ""} />
            <span></span>
          </label>
        </td>
        <td class="col-actions">
          <div class="actions-stack">
            <button class="save btn btn-primary" type="button">Guardar</button>
            <button class="del btn btn-danger" type="button">Eliminar</button>
            <button class="toggle btn btn-soft" type="button">${p.active !== false ? "Inactivar" : "Reactivar"}</button>
          </div>
        </td>
      </tr>`).join("");

    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.end(`<!doctype html><html><head><meta charset="utf-8" /><title>Productos</title>
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <style>
        :root{
          --bg:#f6f8fc;
          --card:#ffffff;
          --line:#d9e2ef;
          --line-strong:#c7d3e5;
          --text:#0b1f44;
          --muted:#5f7598;
          --primary:#153e75;
          --primary-2:#1f5aa8;
          --danger:#b42318;
          --danger-bg:#fff2f0;
          --soft:#eff4fb;
          --success:#0c7a43;
          --success-bg:#ecfdf3;
          --shadow:0 12px 26px rgba(16,24,40,.08);
          --radius:18px;
        }
        *{box-sizing:border-box}
        html,body{margin:0;padding:0;background:transparent;color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif}
        body{padding:14px 16px 20px;overflow-x:hidden}
        .page{width:100%;max-width:none;margin:0 auto}
        .hero{display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;margin-bottom:18px}
        .hero h1{margin:0;font-size:22px;line-height:1.1}
        .hero p{margin:6px 0 0;color:var(--muted);font-size:14px}
        .hero-side{display:flex;gap:8px;flex-wrap:wrap}
        .chip{display:inline-flex;align-items:center;gap:6px;padding:8px 12px;border-radius:999px;background:#fff;border:1px solid var(--line);font-size:12px;font-weight:700;color:var(--primary)}
        .toolbar{background:var(--card);border:1px solid var(--line);border-radius:var(--radius);padding:14px;box-shadow:var(--shadow);display:flex;gap:12px;align-items:center;justify-content:space-between;flex-wrap:wrap;margin-bottom:16px}
        .search{position:relative;flex:1 1 360px;min-width:260px}
        .search input{width:100%;height:46px;border-radius:14px;border:1px solid var(--line-strong);padding:0 16px;background:#fff;font-size:14px;outline:none;color:var(--text)}
        .search input:focus{border-color:#88a7d4;box-shadow:0 0 0 4px rgba(31,90,168,.08)}
        .toolbar-actions{display:flex;gap:10px;flex-wrap:wrap}
        .btn{appearance:none;border:1px solid var(--line-strong);background:#fff;color:var(--text);border-radius:12px;padding:10px 14px;font-weight:700;font-size:13px;cursor:pointer;transition:.18s ease;line-height:1}
        .btn:hover{transform:translateY(-1px);box-shadow:0 8px 18px rgba(16,24,40,.08)}
        .btn-primary{background:var(--primary);border-color:var(--primary);color:#fff}
        .btn-primary:hover{background:var(--primary-2);border-color:var(--primary-2)}
        .btn-soft{background:var(--soft);border-color:var(--line);color:var(--primary)}
        .btn-danger{background:var(--danger-bg);border-color:#f5c3bd;color:var(--danger)}
        .table-card{background:var(--card);border:1px solid var(--line);border-radius:22px;box-shadow:var(--shadow);overflow:hidden}
        .table-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap;padding:18px 18px 12px;border-bottom:1px solid var(--line)}
        .table-head h2{margin:0;font-size:16px}
        .table-head p{margin:4px 0 0;color:var(--muted);font-size:13px}
        .table-wrap{overflow-x:hidden;overflow-y:auto}
        table{width:100%;border-collapse:separate;border-spacing:0;table-layout:fixed;min-width:0}
        thead th{position:sticky;top:0;z-index:1;background:#f8fbff;color:#36527b;font-size:11px;letter-spacing:.04em;text-transform:uppercase;padding:10px 8px;border-bottom:1px solid var(--line);text-align:left}
        tbody td{padding:10px 8px;border-bottom:1px solid #edf2f7;vertical-align:top;background:#fff}
        tbody tr:hover td{background:#fbfdff}
        tbody tr:last-child td{border-bottom:none}
        .col-desc{width:24%}
        .col-price{width:11%}
        .col-qty{width:10%}
        .col-obs{width:27%}
        .col-active{width:8%;text-align:center}
        .col-actions{width:20%}
        input[type=text],input[type=number],textarea{
          width:100%;border:1px solid var(--line-strong);border-radius:12px;background:#fff;padding:9px 10px;font-size:13px;color:var(--text);outline:none;transition:.18s ease
        }
        input[type=text]:focus,input[type=number]:focus,textarea:focus{
          border-color:#88a7d4;box-shadow:0 0 0 4px rgba(31,90,168,.08)
        }
        textarea{min-height:74px;resize:vertical;line-height:1.35}
        input[type=number]{text-align:right}
        .switch{position:relative;display:inline-flex;align-items:center;width:50px;height:30px}
        .switch input{position:absolute;opacity:0;pointer-events:none}
        .switch span{display:block;width:50px;height:30px;border-radius:999px;background:#d6dfeb;border:1px solid #c7d3e5;position:relative;transition:.18s ease}
        .switch span:before{content:"";position:absolute;left:3px;top:3px;width:22px;height:22px;border-radius:50%;background:#fff;box-shadow:0 2px 6px rgba(16,24,40,.14);transition:.18s ease}
        .switch input:checked + span{background:#dff7e9;border-color:#9bdfbb}
        .switch input:checked + span:before{left:23px;background:#0c7a43}
        .actions-stack{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:6px;align-items:start}
        .actions-stack .btn{width:100%;justify-content:center;padding:8px 8px;font-size:12px;white-space:nowrap}
        .actions-stack .btn .ic{font-size:13px;line-height:1}
        .empty-row td{padding:32px 20px;text-align:center;color:var(--muted);font-weight:600}
        .row-draft td{background:#fffcf1}
        @media (max-width: 1100px){
          .col-desc{width:20%}
          .col-tag{width:12%}
          .col-price{width:10%}
          .col-qty{width:9%}
          .col-obs{width:20%}
          .col-active{width:8%}
          .col-actions{width:21%}
          .actions-stack{grid-template-columns:1fr}
        }
        @media (max-width: 960px){
          body{padding:12px}
          .table-card{border-radius:18px}
          .table-wrap{overflow:auto}
          table{min-width:1080px}
          .actions-stack{grid-template-columns:1fr}
        }
      </style></head><body>
      <div class="page">
        <div class="hero">
          <div>
            <h1>Productos</h1>
            <p>Administrá el catálogo del tenant, activá o desactivá artículos y actualizá importes sin tocar la lógica.</p>
          </div>
          <div class="hero-side">
            <span class="chip" id="metaMode">${verTodos ? "Mostrando todos" : "Solo activos"}</span>
            <span class="chip" id="metaCount">0 productos</span>
          </div>
        </div>

        <div class="toolbar">
          <label class="search">
            <input id="searchInput" type="text" placeholder="Buscar por descripción, tag, observación, importe o cantidad..." />
          </label>
          <div class="toolbar-actions">
            <a class="btn btn-soft" href="/productos${verTodos ? "" : "?all=true"}">${verTodos ? "Ver solo activos" : "Ver todos"}</a>
            <button id="btnAdd" class="btn btn-primary" type="button">Nuevo producto</button>
            <button id="btnReload" class="btn" type="button">Recargar</button>
          </div>
        </div>

        <section class="table-card">
          <div class="table-head">
            <div>
              <h2>Listado</h2>
              <p id="tableHint">Editá directamente sobre la tabla y guardá cada fila cuando termines.</p>
            </div>
          </div>
          <div class="table-wrap">
            <table id="tbl">
              <thead>
              <tr>
                <th class="col-desc">Descripción</th>
                <th class="col-tag">Tag</th>
                <th class="col-price">Importe</th>
                <th class="col-qty">Cantidad máx.</th>
                <th class="col-obs">Observación</th>
                <th class="col-active">Activo</th>
                <th class="col-actions">Acciones</th>
              </tr>
              </thead>
               <tbody id="productRows">${initialRows || `<tr class="empty-row"><td colspan="7">No hay productos para mostrar.</td></tr>`}</tbody>
            </table>
          </div>
        </section>
      </div>

      <template id="row-tpl"><tr data-id="" data-draft="1" class="row-draft">
        <td class="col-desc"><input class="descripcion" type="text" placeholder="Descripción del producto" /></td>
        <td class="col-tag"><input class="tag" type="text" placeholder="Tag" /></td>
        <td class="col-price"><input class="importe" type="number" step="0.01" placeholder="0" /></td>
        <td class="col-qty"><input class="cantidad" type="number" step="1" placeholder="0" /></td>
        <td class="col-obs"><textarea class="observacion" placeholder="Observaciones, categoría, presentación..."></textarea></td>
        <td class="col-active">
          <label class="switch">
            <input class="active" type="checkbox" checked />
            <span></span>
          </label>
        </td>
        <td class="col-actions">
          <div class="actions-stack">
            <button class="save btn btn-primary" type="button" title="Guardar" aria-label="Guardar"><span class="ic">💾</span></button>
            <button class="del btn btn-danger" type="button" title="Eliminar" aria-label="Eliminar"><span class="ic">🗑️</span></button>
            <button class="toggle btn btn-soft" type="button" title="Inactivar / Reactivar" aria-label="Inactivar o reactivar"><span class="ic">⏸️</span></button>
          </div>
        </td>
      </tr></template>

      <script>
        function q(s,c){return (c||document).querySelector(s)}
        function all(s,c){return Array.from((c||document).querySelectorAll(s))}
        async function j(url,opts){
          const r=await fetch(url,opts||{});
          if(!r.ok)throw new Error('HTTP '+r.status);
          const ct=r.headers.get('content-type')||'';
          return ct.includes('application/json')?r.json():r.text();
        }

        function setRowValues(tr,it){
          tr.dataset.id=it && it._id ? String(it._id) : '';
          tr.dataset.draft = it && it._id ? '' : '1';
          tr.classList.toggle('row-draft', !(it && it._id));
          q('.descripcion',tr).value=it && it.descripcion ? it.descripcion : '';
          q('.tag',tr).value=it && it.tag ? it.tag : '';
          q('.importe',tr).value=(it && (typeof it.importe==='number' || it.importe)) ? it.importe : '';
          q('.cantidad',tr).value=(it && (typeof it.cantidad==='number' || it.cantidad)) ? it.cantidad : '';
          q('.observacion',tr).value=it && it.observacion ? it.observacion : '';
          q('.active',tr).checked=!(it && it.active===false);
          q('.toggle',tr).innerHTML = '<span class="ic">' + (!(it && it.active===false) ? '⏸️' : '▶️') + '</span>';
        }

        function visibleRows(){
          return all('#productRows tr').filter(tr => !tr.classList.contains('empty-row'));
        }

        function updateMeta(){
          const term=(q('#searchInput').value||'').trim().toLowerCase();
          let total=0, visibles=0, activos=0;
          visibleRows().forEach(tr=>{
            total++;
            const haystack=[
              q('.descripcion',tr)?.value||'',
              q('.tag',tr)?.value||'',
              q('.observacion',tr)?.value||'',
              q('.importe',tr)?.value||'',
              q('.cantidad',tr)?.value||''
            ].join(' ').toLowerCase();
            const show=!term || haystack.includes(term);
            tr.style.display=show?'':'none';
            if(show) visibles++;
            if(q('.active',tr)?.checked) activos++;
          });
          q('#metaCount').textContent = visibles + ' / ' + total + ' visibles';
          q('#tableHint').textContent = total
            ? (term ? 'Filtrando ' + visibles + ' de ' + total + ' productos.' : 'Editá directamente sobre la tabla y guardá cada fila cuando termines.')
            : 'No hay productos para mostrar.';
        }

        function bindRow(tr){
          q('.save',tr).addEventListener('click',()=>saveRow(tr));
          q('.del',tr).addEventListener('click',()=>deleteRow(tr));
          q('.toggle',tr).addEventListener('click',()=>toggleRow(tr));
          all('input,textarea',tr).forEach(el=>el.addEventListener('input',updateMeta));
          q('.active',tr).addEventListener('change',()=>{
            q('.toggle',tr).innerHTML = '<span class="ic">' + (q('.active',tr).checked ? '⏸️' : '▶️') + '</span>';
            updateMeta();
          });
        }

        function showEmptyIfNeeded(){
          const tb=q('#productRows');
          const rows=visibleRows();
          const currentEmpty=q('.empty-row',tb);
          if(!rows.length){
            if(!currentEmpty){
              const tr=document.createElement('tr');
              tr.className='empty-row';
              tr.innerHTML='<td colspan="7">No hay productos para mostrar.</td>';
              tb.appendChild(tr);
            }
          }else if(currentEmpty){
            currentEmpty.remove();
          }
        }

        async function reload(){
          const url=new URL(location.href);
          const allFlag=url.searchParams.get('all')==='true';
          const data=await j('/api/products'+(allFlag?'?all=true':''));
          const tb=q('#productRows');
          tb.innerHTML='';
          if(Array.isArray(data) && data.length){
            data.forEach(it=>{
              const tr=q('#row-tpl').content.firstElementChild.cloneNode(true);
              setRowValues(tr,it);
              bindRow(tr);
              tb.appendChild(tr);
            });
          }
          showEmptyIfNeeded();
          updateMeta();
        }

        async function saveRow(tr){
          const payload={
            descripcion:q('.descripcion',tr).value.trim(),
            tag:q('.tag',tr).value.trim(),
            importe:q('.importe',tr).value.trim(),
            cantidad:q('.cantidad',tr).value.trim(),
            observacion:q('.observacion',tr).value.trim(),
            active:q('.active',tr).checked
          };
          if(!payload.descripcion){ alert('Descripción requerida'); return; }
          const id=tr.dataset.id;
          if(id){
            await j('/api/products/'+encodeURIComponent(id),{
              method:'PUT',
              headers:{'Content-Type':'application/json'},
              body:JSON.stringify(payload)
            });
          }else{
            await j('/api/products',{
              method:'POST',
              headers:{'Content-Type':'application/json'},
              body:JSON.stringify(payload)
            });
          }
          await reload();
        }

        async function deleteRow(tr){
          const id=tr.dataset.id;
          if(!id){ tr.remove(); showEmptyIfNeeded(); updateMeta(); return; }
          if(!confirm('¿Eliminar definitivamente este producto?')) return;
          await j('/api/products/'+encodeURIComponent(id),{method:'DELETE'});
          await reload();
        }

        async function toggleRow(tr){
          const id=tr.dataset.id;
          if(!id){ alert('Primero guardá el nuevo producto.'); return; }
          const active=q('.active',tr).checked;
          const path=active
            ? ('/api/products/'+encodeURIComponent(id)+'/inactivate')
            : ('/api/products/'+encodeURIComponent(id)+'/reactivate');
          await j(path,{method:'POST'});
          await reload();
        }

        q('#btnReload').addEventListener('click',reload);
        q('#btnAdd').addEventListener('click',()=>{
          const tr=q('#row-tpl').content.firstElementChild.cloneNode(true);
          bindRow(tr);
          q('#productRows').prepend(tr);
          showEmptyIfNeeded();
          updateMeta();
          q('.descripcion',tr).focus();
        });
        q('#searchInput').addEventListener('input',updateMeta);

        all('#productRows tr').forEach(tr=>{
          if(!tr.classList.contains('empty-row')) bindRow(tr);
        });
        showEmptyIfNeeded();
        updateMeta();
      </script></body></html>`);
  } catch (e) {
    console.error("/productos error:", e);
    res.status(500).send("internal");
  }
});


// ================== Horarios de atención (UI L-V) ==================
// Página visual para cargar horarios de lunes a domingo (hasta 2 franjas por día)
app.get("/horarios", async (req, res) => {
  try {
    const tenant = resolveTenantId(req);
    const db = await getDb();
    const _id = `store_hours:${tenant}`;
    const doc = (await db.collection("settings").findOne({ _id })) || {};
    const hours = doc.hours || {};
    const hoursJson = JSON.stringify(hours).replace(/</g, "\u003c");

    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.end(`<!doctype html><html><head><meta charset="utf-8" />
      <title>Horarios de atención (${tenant})</title>
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <style>
        :root{
          --bg:#f4f7fb;
          --card:#ffffff;
          --text:#0f172a;
          --muted:#51627f;
          --line:#d9e3f0;
          --line-strong:#bfd0e3;
          --primary:#1f5aa8;
          --primary-2:#184985;
          --soft:#eef4fb;
          --success:#0c7a43;
          --success-bg:#dff7e9;
          --shadow:0 12px 26px rgba(16,24,40,.08);
          --radius:18px;
        }
        *{box-sizing:border-box}
        html,body{margin:0;padding:0;background:transparent;color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif}
        body{padding:14px 16px 20px;overflow-x:hidden}
        .page{width:100%;max-width:none;margin:0 auto}
        .hero{display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;margin-bottom:18px}
        .hero h1{margin:0;font-size:22px;line-height:1.1}
        .hero p{margin:6px 0 0;color:var(--muted);font-size:14px}
        .hero-side{display:flex;gap:8px;flex-wrap:wrap}
        .chip{display:inline-flex;align-items:center;gap:6px;padding:8px 12px;border-radius:999px;background:#fff;border:1px solid var(--line);font-size:12px;font-weight:700;color:var(--primary)}
        .toolbar{background:var(--card);border:1px solid var(--line);border-radius:var(--radius);padding:14px;box-shadow:var(--shadow);display:flex;gap:12px;align-items:end;justify-content:space-between;flex-wrap:wrap;margin-bottom:16px}
        .toolbar-left{display:flex;gap:12px;flex-wrap:wrap;align-items:end;flex:1 1 420px}
        .field{display:flex;flex-direction:column;gap:6px;min-width:220px;flex:1 1 260px}
        .field.small{flex:0 0 220px;min-width:220px}
        .field label{font-size:12px;font-weight:800;letter-spacing:.04em;text-transform:uppercase;color:#466189}
        .field input{width:100%;height:46px;border-radius:14px;border:1px solid var(--line-strong);padding:0 14px;background:#fff;font-size:14px;outline:none;color:var(--text)}
        .field input:focus{border-color:#88a7d4;box-shadow:0 0 0 4px rgba(31,90,168,.08)}
        .toolbar-actions{display:flex;gap:10px;flex-wrap:wrap}
        .btn{appearance:none;border:1px solid var(--line-strong);background:#fff;color:var(--text);border-radius:12px;padding:10px 14px;font-weight:700;font-size:13px;cursor:pointer;transition:.18s ease;line-height:1}
        .btn:hover{transform:translateY(-1px);box-shadow:0 8px 18px rgba(16,24,40,.08)}
        .btn-primary{background:var(--primary);border-color:var(--primary);color:#fff}
        .btn-primary:hover{background:var(--primary-2);border-color:var(--primary-2)}
        .btn-soft{background:var(--soft);border-color:var(--line);color:var(--primary)}
        .grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
        .day-card{background:var(--card);border:1px solid var(--line);border-radius:22px;box-shadow:var(--shadow);overflow:hidden}
        .day-head{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:16px 18px;border-bottom:1px solid var(--line)}
        .day-title{margin:0;font-size:18px;line-height:1.1}
        .day-sub{margin:4px 0 0;color:var(--muted);font-size:12px}
        .day-body{padding:16px 18px 18px;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
        .range{background:#f8fbff;border:1px solid var(--line);border-radius:16px;padding:12px}
        .range h3{margin:0 0 10px;font-size:13px;color:#36527b;letter-spacing:.04em;text-transform:uppercase}
        .time-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
        .time-field{display:flex;flex-direction:column;gap:6px}
        .time-field label{font-size:11px;font-weight:800;letter-spacing:.04em;text-transform:uppercase;color:#466189}
        input[type=time]{width:100%;height:42px;border-radius:12px;border:1px solid var(--line-strong);padding:0 10px;background:#fff;font-size:14px;color:var(--text);outline:none}
        input[type=time]:focus{border-color:#88a7d4;box-shadow:0 0 0 4px rgba(31,90,168,.08)}
        .switch-row{display:flex;align-items:center;gap:10px;white-space:nowrap}
        .switch-label{font-size:13px;font-weight:700;color:var(--muted)}
        .switch{position:relative;display:inline-flex;align-items:center;width:50px;height:30px}
        .switch input{position:absolute;opacity:0;pointer-events:none}
        .switch span{display:block;width:50px;height:30px;border-radius:999px;background:#d6dfeb;border:1px solid #c7d3e5;position:relative;transition:.18s ease}
        .switch span:before{content:"";position:absolute;left:3px;top:3px;width:22px;height:22px;border-radius:50%;background:#fff;box-shadow:0 2px 6px rgba(16,24,40,.14);transition:.18s ease}
        .switch input:checked + span{background:var(--success-bg);border-color:#9bdfbb}
        .switch input:checked + span:before{left:23px;background:var(--success)}
        .day-card.is-off .range{opacity:.55}
        .day-card.is-off .day-title{color:#6b7b94}
        .hint-card{margin-top:16px;background:var(--card);border:1px solid var(--line);border-radius:18px;padding:14px 16px;color:var(--muted);font-size:13px;box-shadow:var(--shadow)}
        .toast{position:fixed;right:18px;bottom:18px;background:#0f172a;color:#fff;padding:12px 14px;border-radius:14px;box-shadow:0 14px 30px rgba(15,23,42,.22);font-size:13px;font-weight:700;opacity:0;transform:translateY(10px);pointer-events:none;transition:.18s ease;z-index:50}
        .toast.show{opacity:1;transform:translateY(0)}
        @media (max-width: 980px){
          body{padding:12px}
          .grid{grid-template-columns:1fr}
        }
        @media (max-width: 680px){
          .day-body,.time-grid{grid-template-columns:1fr}
          .field.small,.field{min-width:0;flex:1 1 100%}
          .toolbar-actions{width:100%}
          .toolbar-actions .btn{flex:1 1 0}
        }
      </style></head><body>
      <div class="page">
        <div class="hero">
          <div>
            <h1>Horarios de atención</h1>
            <p>Configurá la disponibilidad del tenant y definí hasta dos franjas por día sin depender de una tabla ancha.</p>
          </div>
          <div class="hero-side">
            <span class="chip">Lunes a domingo</span>
            <span class="chip" id="metaEnabled">0 días activos</span>
          </div>
        </div>

        <div class="toolbar">
          <div class="toolbar-left">
            <div class="field small">
              <label for="tenant">Tenant</label>
              <input id="tenant" type="text" value="${tenant.replace(/"/g,'&quot;')}" />
            </div>
          </div>
          <div class="toolbar-actions">
            <button id="btnReload" class="btn btn-soft" type="button">Recargar</button>
            <button id="btnSave" class="btn btn-primary" type="button">Guardar horarios</button>
          </div>
        </div>

        <section class="grid" id="hoursGrid">
          ${STORE_HOURS_DAYS.map((d, idx) => `
          <article class="day-card" data-day="${d.key}">
            <div class="day-head">
              <div>
                <h2 class="day-title">${d.label}</h2>
                <p class="day-sub">${idx < 5 ? 'Jornada configurable' : 'Disponibilidad especial de fin de semana'}</p>
              </div>
              <div class="switch-row">
                <span class="switch-label">Habilitado</span>
                <label class="switch">
                  <input type="checkbox" class="enabled" />
                  <span></span>
                </label>
              </div>
            </div>
            <div class="day-body">
              <section class="range">
                <h3>Franja 1</h3>
                <div class="time-grid">
                  <div class="time-field">
                    <label>Desde</label>
                    <input type="time" class="from1" />
                  </div>
                  <div class="time-field">
                    <label>Hasta</label>
                    <input type="time" class="to1" />
                  </div>
                </div>
              </section>
              <section class="range">
                <h3>Franja 2</h3>
                <div class="time-grid">
                  <div class="time-field">
                    <label>Desde</label>
                    <input type="time" class="from2" />
                  </div>
                  <div class="time-field">
                    <label>Hasta</label>
                    <input type="time" class="to2" />
                  </div>
                </div>
              </section>
            </div>
          </article>`).join("")}
        </section>

        <div class="hint-card">
          Dejá un día deshabilitado o sin horarios para que no pueda seleccionarse en nuevos pedidos. Los cambios se guardan en el backend y se usan para validar disponibilidad.
        </div>
      </div>
      <div id="toast" class="toast" aria-live="polite"></div>
      <script>
        const DAYS = ${JSON.stringify(STORE_HOURS_DAYS).replace(/</g,"\u003c")};

        function q(s,c){return (c||document).querySelector(s)}
        function all(s,c){return Array.from((c||document).querySelectorAll(s))}

        function showToast(msg){
          const t = q('#toast');
          t.textContent = msg || '';
          t.classList.add('show');
          clearTimeout(showToast._tm);
          showToast._tm = setTimeout(()=>t.classList.remove('show'), 1800);
        }

        function setDayState(card, enabled){
          card.classList.toggle('is-off', !enabled);
        }

        function updateMeta(){
          const enabledCount = all('.day-card .enabled').filter(el=>el.checked).length;
          q('#metaEnabled').textContent = enabledCount + ' día' + (enabledCount === 1 ? '' : 's') + ' activos';
          all('.day-card').forEach(card => setDayState(card, q('.enabled', card).checked));
        }

        function setForm(data){
          DAYS.forEach(d => {
            const row = document.querySelector('[data-day="'+d.key+'"]');
            if (!row) return;
            const ranges = Array.isArray(data[d.key]) ? data[d.key] : [];
            const r1 = ranges[0] || {};
            const r2 = ranges[1] || {};
            row.querySelector('.enabled').checked = ranges.length > 0;
            row.querySelector('.from1').value = r1.from || "";
            row.querySelector('.to1').value   = r1.to   || "";
            row.querySelector('.from2').value = r2.from || "";
            row.querySelector('.to2').value   = r2.to   || "";
          });
          updateMeta();
        }

        function collectForm(){
          const out = {};
          DAYS.forEach(d => {
            const row = document.querySelector('[data-day="'+d.key+'"]');
            if (!row) return;
            const enabled = row.querySelector('.enabled').checked;
            const f1 = row.querySelector('.from1').value;
            const t1 = row.querySelector('.to1').value;
            const f2 = row.querySelector('.from2').value;
            const t2 = row.querySelector('.to2').value;
            if (!enabled) return;
            const ranges = [];
            if (f1 && t1) ranges.push({ from:f1, to:t1 });
            if (f2 && t2) ranges.push({ from:f2, to:t2 });
            if (ranges.length) out[d.key] = ranges;
          });
          return out;
        }

        async function reloadHours(){
          const t = document.getElementById('tenant').value || '';
          const r = await fetch('/api/hours?tenant=' + encodeURIComponent(t));
          if (!r.ok) { alert('Error recargando horarios'); return; }
          const j = await r.json();
          setForm(j.hours || {});
          showToast('Horarios recargados');
        }

        async function saveHours(){
          const t = document.getElementById('tenant').value || '';
          const hours = collectForm();
          const r = await fetch('/api/hours', {
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ tenantId:t, hours })
          });
          if (!r.ok) { alert('Error al guardar'); return; }
          const j = await r.json();
          setForm(j.hours || {});
          showToast('Horarios guardados ✅');
        }

        document.getElementById('btnReload').addEventListener('click', reloadHours);
        document.getElementById('btnSave').addEventListener('click', saveHours);
        all('.enabled').forEach(el => el.addEventListener('change', updateMeta));
        all('input[type=time]').forEach(el => el.addEventListener('change', updateMeta));

        // Inicializar con lo que vino del servidor
        setForm(${hoursJson});
      </script></body></html>`);
  } catch (e) {
    console.error("/horarios error:", e);
    res.status(500).send("internal");
  }
});



// Behavior UI
app.get("/comportamiento", async (req, res) => {
  try {
    const tenant = resolveTenantId(req);
    const cfg = await loadBehaviorConfigFromMongo(tenant);
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.end(`<!doctype html><html><head><meta charset="utf-8" />
      <title>Comportamiento del Bot (${tenant})</title>
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <style>body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:24px;max-width:960px}
      textarea{width:100%;min-height:360px;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:14px}
      .row{display:flex;gap:8px;align-items:center}.hint{color:#666;font-size:12px}.tag{padding:2px 6px;border:1px solid #ccc;border-radius:4px;font-size:12px}
      input[type=text]{padding:6px 8px}</style></head><body>
      <h1>Comportamiento del Bot</h1>
      <div class="row">
        <label>Tenant:&nbsp;<input id="tenant" type="text" value="${tenant}" /></label>
        <button id="btnReload">Recargar</button>
        <button id="btnSave">Guardar</button>
      </div>
      <div class="row" style="margin-top:8px">
        <label>Modo de historial:&nbsp;
          <select id="historyMode">
            <option value="standard">standard (completo)</option>
            <option value="minimal">minimal (solo user + assistant Pedido)</option>
          </select>
        </label>
      </div>
      <p></p><textarea id="txt" placeholder="Escribí aquí el comportamiento para este tenant..."></textarea>
      <script>
        async function load(){
          const t = document.getElementById('tenant').value || '';
          const r=await fetch('/api/behavior?tenant='+encodeURIComponent(t));
          const j=await r.json();
          document.getElementById('txt').value=j.text||'';
          document.getElementById('historyMode').value = (j.history_mode || 'standard');
        }
        async function save(){
          const t=document.getElementById('tenant').value||'';
          const v=document.getElementById('txt').value||'';
          const m=document.getElementById('historyMode').value||'standard';
          const r=await fetch('/api/behavior?tenant='+encodeURIComponent(t),{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({text:v,tenantId:t,history_mode:m})
          });
          alert(r.ok?'Guardado ✅':'Error al guardar');
        }
        document.getElementById('btnSave').addEventListener('click',save);
        document.getElementById('btnReload').addEventListener('click',load);
        document.addEventListener('DOMContentLoaded', () => {
          document.getElementById('historyMode').value='${(cfg.history_mode || 'standard').replace(/"/g,'&quot;')}';
        });
        load();
      </script></body></html>`);
  } catch (e) { console.error("/comportamiento error:", e); res.status(500).send("internal"); }
});


// ===================================================================
// ===============          Canales (WA/OpenAI)        ===============
// ===================================================================
// UI simple (sin sidebar) para administrar tenant_channels.
// Se usa dentro de /ui/canales (appShell) para evitar duplicar el menú.
app.get("/canales", async (req, res) => {
  try {
    const tenant = resolveTenantId(req);
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.end(`<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Canales (${tenant})</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:24px;max-width:1100px}
    h1{margin:0 0 8px}
    .muted{color:#666;font-size:13px}
    .row{display:flex;gap:14px;flex-wrap:wrap}
    .card{border:1px solid #ddd;border-radius:10px;padding:14px;flex:1;min-width:320px}
    label{display:block;font-size:12px;color:#333;margin:10px 0 6px}
    input{width:100%;padding:10px 12px;border:1px solid #ccc;border-radius:8px}
    button{padding:10px 14px;border:0;border-radius:8px;background:#0f766e;color:#fff;font-weight:600;cursor:pointer}
    button.secondary{background:#e5e7eb;color:#111827}
    table{width:100%;border-collapse:collapse;margin-top:10px}
    th,td{border-bottom:1px solid #eee;padding:10px;text-align:left;font-size:13px;vertical-align:top}
    th{font-size:12px;color:#374151}
    .pill{display:inline-block;padding:2px 8px;border:1px solid #cbd5e1;border-radius:999px;font-size:12px;color:#0f172a;background:#f8fafc}
    .actions{display:flex;gap:8px;flex-wrap:wrap}
    .msg{margin:10px 0;padding:10px 12px;border-radius:8px}
    .msg.ok{background:#ecfdf5;border:1px solid #a7f3d0;color:#065f46}
    .msg.err{background:#fef2f2;border:1px solid #fecaca;color:#991b1b}
    code{background:#f3f4f6;padding:2px 6px;border-radius:6px}
  </style>
</head>
<body>
  <h1>Canales (WhatsApp / Instagram / OpenAI)</h1>
  <div class="muted">Configurá por <b>tenantId</b> un canal de WhatsApp o Instagram y la API key de OpenAI (colección <code>tenant_channels</code>). Podés marcar un canal como <b>Default</b> por tenant.</div>

  <div id="msg"></div>

  <div class="row" style="margin-top:14px">
    <div class="card">
      <h3 style="margin:0 0 8px">Crear / actualizar</h3>
      <form id="f">
        <label>TenantId</label>
        <input name="tenantId" value="${String(tenant||'').replace(/"/g,'&quot;')}" placeholder="default"/>

        <label>Tipo de canal</label>
        <select name="channelType" id="channelType" style="width:100%;padding:10px 12px;border:1px solid #ccc;border-radius:8px">
          <option value="whatsapp">WhatsApp</option>
          <option value="instagram">Instagram</option>
        </select>

        <div id="waFields">
          <label>Phone Number ID (Meta)</label>
          <input name="phoneNumberId" placeholder="1234567890"/>

          <label>Display phone (opcional)</label>
          <input name="displayPhoneNumber" placeholder="+54 9 ..."/>

          <label>WhatsApp Token</label>
          <input name="whatsappToken" placeholder="EAAG..." />

          <label>Verify Token</label>
          <input name="verifyToken" placeholder="mi-token-verificacion" />
        </div>

        <div id="igFields" style="display:none">
          <label>Instagram Account ID</label>
          <input name="instagramAccountId" placeholder="1784..." />

          <label>Facebook Page ID vinculada</label>
          <input name="instagramPageId" placeholder="1234567890" />

          <label>Instagram Access Token</label>
          <input name="instagramAccessToken" placeholder="EAAG..." />
        </div>

        <label>OpenAI API Key</label>
        <input name="openaiApiKey" placeholder="sk-..." />

        <label>Espera antes de procesar (ms)</label>
        <input name="messageDebounceMs" placeholder="0 = sin espera (ej: 1200)" />
        <div class="muted">Si el cliente manda varios textos seguidos, se juntan y se envían a ChatGPT como uno solo, separados por coma.</div>

        <label style="display:flex;gap:10px;align-items:center;margin-top:12px">
          <input type="checkbox" name="isDefault" value="1" style="width:auto"/>
          <span>Canal default (por tenant)</span>
        </label>
       <div class="actions" style="margin-top:12px">
          <button type="submit">Guardar</button>
          <button type="button" class="secondary" id="btnClear">Limpiar</button>
          <button type="button" class="secondary" id="btnReload">Actualizar</button>
        </div>
      </form>
    </div>

    <div class="card">
      <h3 style="margin:0 0 8px">Canales cargados</h3>
      <div class="muted">Tip: hacé click en “Editar” para cargar los campos.</div>
      <div style="overflow:auto">
        <table>
          <thead>
            <tr>
              <th>Tenant</th>
              <th>Tipo</th>
              <th>Canal ID</th>
              <th>Display</th>
              <th>Default</th>
              <th>Updated</th>
              <th></th>
            </tr>
          </thead>
          <tbody id="tbody">
            <tr><td colspan="7" class="muted">Cargando...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

<script>
(function(){
  const msgEl = document.getElementById('msg');
  const form = document.getElementById('f');
  const tbody = document.getElementById('tbody');
  const btnClear = document.getElementById('btnClear');
  const btnReload = document.getElementById('btnReload');

  function esc(s){
    return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }

  function setMsg(type, text){
    if(!text){ msgEl.innerHTML=''; return; }
    msgEl.innerHTML = '<div class="msg '+(type==='ok'?'ok':'err')+'">'+esc(text)+'</div>';
  }

  async function load(){
    setMsg('', '');
    tbody.innerHTML = '<tr><td colspan="7" class="muted">Cargando...</td></tr>';

    let items = [];
    try {
      const tenantId = (form.tenantId.value||'').trim();
      const qs = tenantId ? ('?tenantId='+encodeURIComponent(tenantId)) : '';

      const r = await fetch('/api/tenant-channels'+qs, {
        headers: { 'Accept':'application/json' },
        credentials: 'same-origin',
      });

      if(!r.ok){
        const jErr = await r.json().catch(()=>null);
        const msg = (jErr && (jErr.error || jErr.message)) ? (jErr.error || jErr.message) : ('HTTP ' + r.status);
        tbody.innerHTML = '<tr><td colspan="7">No se pudo cargar: '+esc(msg)+'</td></tr>';
        return;
      }

      const ct = String(r.headers.get('content-type') || '').toLowerCase();
      if(!ct.includes('application/json')){
        tbody.innerHTML = '<tr><td colspan="7">No se pudo cargar (respuesta no JSON). Probable sesión vencida/redirección a login. Refrescá o volvé a iniciar sesión.</td></tr>';
        return;
      }

      const j = await r.json();
      items = Array.isArray(j.items) ? j.items : [];
      if(!items.length){
        tbody.innerHTML = '<tr><td colspan="7" class="muted">No hay canales cargados.</td></tr>';
        return;
      }

      tbody.innerHTML = items.map(it => {
        const def = (it && it.isDefault) ? '✅' : '';
        const channelId = (it.channelType === 'instagram')
          ? (it.instagramAccountId || '')
          : (it.phoneNumberId || '');
        const btnDefault = it.isDefault ? '' :
          '<button type="button" class="secondary" data-make-default="1" data-tenant="'+esc(it.tenantId||'')+'" data-type="'+esc(it.channelType||'whatsapp')+'" data-phone="'+esc(it.phoneNumberId||'')+'" data-ig="'+esc(it.instagramAccountId||'')+'">Hacer default</button>';

        return '<tr>'+
          '<td><span class="pill">'+esc(it.tenantId||'')+'</span></td>'+
          '<td>'+esc(it.channelType||'whatsapp')+'</td>'+
          '<td>'+esc(channelId)+'</td>'+
          '<td>'+esc(it.displayPhoneNumber || it.instagramPageId || '')+'</td>'+
          '<td>'+def+'</td>'+
          '<td class="muted">'+esc(it.updatedAt||it.createdAt||'')+'</td>'+
          '<td class="actions">'+
            '<button type="button" class="secondary" data-edit="'+esc(it._id)+'">Editar</button>'+
            btnDefault +
          '</td>'+
        '</tr>';
      }).join('');

      // Editar
      tbody.querySelectorAll('button[data-edit]').forEach(btn=>{
        btn.addEventListener('click', ()=>{
          const id = btn.getAttribute('data-edit');
          const it = items.find(x => String(x._id)===String(id));
          if(!it) return;

          form.tenantId.value = it.tenantId || '';
          if (form.channelType) form.channelType.value = it.channelType || 'whatsapp';
          form.phoneNumberId.value = it.phoneNumberId || '';
          form.displayPhoneNumber.value = it.displayPhoneNumber || '';
          form.instagramAccountId.value = it.instagramAccountId || '';
          form.instagramPageId.value = it.instagramPageId || '';

          // secretos pueden venir enmascarados si no sos superadmin
          form.whatsappToken.value = (it.whatsappToken && it.whatsappToken !== '********') ? it.whatsappToken : '';
          form.instagramAccessToken.value = (it.instagramAccessToken && it.instagramAccessToken !== '********') ? it.instagramAccessToken : '';
          form.verifyToken.value   = (it.verifyToken && it.verifyToken !== '********') ? it.verifyToken : '';
          form.openaiApiKey.value  = (it.openaiApiKey && it.openaiApiKey !== '********') ? it.openaiApiKey : '';
          if (form.messageDebounceMs) form.messageDebounceMs.value = String(it.messageDebounceMs ?? '');
          const cb = form.querySelector('input[name="isDefault"]');
          if (cb) cb.checked = !!it.isDefault;
          toggleChannelFields();

          window.scrollTo({ top: 0, behavior: 'smooth' });
        });
      });

      // Hacer default
      tbody.querySelectorAll('button[data-make-default]').forEach(btn=>{
        btn.addEventListener('click', async ()=>{
          const t = btn.getAttribute('data-tenant') || '';
          const type = btn.getAttribute('data-type') || 'whatsapp';
          const p = btn.getAttribute('data-phone') || '';
          const ig = btn.getAttribute('data-ig') || '';
          if(!t || (type === 'instagram' ? !ig : !p)) return;

          setMsg('', '');
          const data = new URLSearchParams();
          data.set('tenantId', t);
          data.set('channelType', type);
          if (type === 'instagram') data.set('instagramAccountId', ig);
          else data.set('phoneNumberId', p);
          data.set('isDefault', '1');

          const rr = await fetch('/api/tenant-channels', {
            method: 'POST',
            headers: { 'Content-Type':'application/x-www-form-urlencoded' },
            credentials: 'same-origin',
            body: data
          });

          const jj = await rr.json().catch(()=>null);
          if(!rr.ok){
            setMsg('err', (jj && jj.error) ? jj.error : 'Error seteando default.');
            return;
          }
          setMsg('ok', 'Default actualizado ✅');
          await load();
        });
      });

    } catch (e) {
      console.error('[canales] load error:', e);
      tbody.innerHTML = '<tr><td colspan="7">Error cargando canales: '+esc(e?.message || String(e))+'</td></tr>';
    }
  }

  // Guardar (upsert)
  form.addEventListener('submit', async (e)=>{
    e.preventDefault();
    setMsg('', '');
    const data = new URLSearchParams(new FormData(form));

    const r = await fetch('/api/tenant-channels', {
      method: 'POST',
      headers: { 'Content-Type':'application/x-www-form-urlencoded' },
      credentials: 'same-origin',
      body: data
    });

    const j = await r.json().catch(()=>null);
    if(!r.ok){
      setMsg('err', (j && j.error) ? j.error : 'Error guardando.');
      return;
    }
    setMsg('ok', 'Guardado OK.');
    await load();
  });

  function toggleChannelFields(){
    const type = (form.channelType && form.channelType.value) || 'whatsapp';
    const wa = document.getElementById('waFields');
    const ig = document.getElementById('igFields');
    if (wa) wa.style.display = type === 'instagram' ? 'none' : '';
    if (ig) ig.style.display = type === 'instagram' ? '' : 'none';
  }

  if (form.channelType) {
    form.channelType.addEventListener('change', toggleChannelFields);
    toggleChannelFields();
  }

  btnClear.addEventListener('click', ()=>{
    const keepTenant = form.tenantId.value;
    form.reset();
    form.tenantId.value = keepTenant;
    if (form.channelType) form.channelType.value = 'whatsapp';
    toggleChannelFields();
    setMsg('', '');
  });

  btnReload.addEventListener('click', load);

  load();
})();
</script>

</body>
</html>`);
  } catch (e) {
    console.error("GET /canales error:", e?.message || e);
    res.status(500).send("Error");
  }
});

app.get("/api/behavior", async (req, res) => {
  try {
    const tenant = resolveTenantId(req);
    const cfg = await loadBehaviorConfigFromMongo(tenant);
    res.json({ source: "mongo", tenant, text: cfg.text, history_mode: cfg.history_mode });
  } catch {
    res.status(500).json({ error: "internal" });
  }
});

app.post("/api/behavior", async (req, res) => {
  try {
    const tenant = (req.body?.tenantId || resolveTenantId(req)).toString().trim();
    const text = String(req.body?.text || "").trim();
    const history_mode = String(req.body?.history_mode || "").trim() || "standard";
    const db = await require("./db").getDb();
    const _id = `behavior:${tenant}`;
    await db.collection("settings").updateOne(
      { _id },
      { $set: { text, history_mode, tenantId: tenant, updatedAt: new Date() } },
      { upsert: true }
    );
    invalidateBehaviorCache(tenant);
    res.json({ ok: true, tenant, history_mode });
  } catch (e) {
    console.error("POST /api/behavior error:", e);
    res.status(500).json({ error: "internal" });
  }
});

app.post("/api/behavior/refresh-cache", async (req, res) => {
  try {
    const tenant = resolveTenantId(req);
    invalidateBehaviorCache(tenant);
    res.json({ ok: true, tenant, cache: "invalidated" });
  } catch (e) { console.error("refresh-cache error:", e); res.status(500).json({ error: "internal" }); }
});

// ===================================================================
// ===============      Horarios de atención (L-V)     ================
// ===================================================================
// Permite guardar y leer los horarios disponibles de lunes a viernes.
// Cada día puede tener hasta dos franjas horarias [{ from, to }, ...]
// Formato de hora: "HH:MM" (24h).

function normalizeHoursPayload(raw) {
  const days = ["monday", "tuesday", "wednesday", "thursday", "friday","saturday","sunday"];
  const out = {};
  const isHHMM = (v) => /^([01]\d|2[0-3]):[0-5]\d$/.test(String(v || "").trim());

  for (const d of days) {
    const ranges = Array.isArray(raw?.[d]) ? raw[d] : [];
    const normRanges = [];

    for (const r of ranges) {
      if (!r) continue;
      // Soportar tanto { from, to } como { desde, hasta }
      const from = String(r.from ?? r.desde ?? "").trim();
      const to   = String(r.to   ?? r.hasta ?? "").trim();

      // Validar formato y que from < to
      if (!isHHMM(from) || !isHHMM(to)) continue;
      if (from >= to) continue;

      normRanges.push({ from, to });
      // Máximo 2 franjas por día
      if (normRanges.length >= 2) break;
    }

    if (normRanges.length) {
      out[d] = normRanges;
    }
  }

  return out;
}

// GET /api/hours  → devuelve horarios configurados para el tenant actual
app.get("/api/hours", async (req, res) => {
  try {
    const tenant = resolveTenantId(req);
    const db = await getDb();
    const _id = `store_hours:${tenant}`;
    const doc = (await db.collection("settings").findOne({ _id })) || {};

    res.json({
      ok: true,
      tenant,
      hours: doc.hours || {},
      updatedAt: doc.updatedAt || null,
    });
  } catch (e) {
    console.error("GET /api/hours error:", e);
    res.status(500).json({ error: "internal" });
  }
});

// POST /api/hours  → guarda horarios (sobrescribe los existentes para ese tenant)
app.post("/api/hours", async (req, res) => {
  try {
    const tenant = (req.body?.tenantId || resolveTenantId(req)).toString().trim();
    const hours = normalizeHoursPayload(req.body?.hours || req.body || {});

    const db = await getDb();
    const _id = `store_hours:${tenant}`;
    await db.collection("settings").updateOne(
      { _id },
      { $set: { hours, tenantId: tenant, updatedAt: new Date() } },
      { upsert: true }
    );

    res.json({ ok: true, tenant, hours });
  } catch (e) {
    console.error("POST /api/hours error:", e);
    res.status(500).json({ error: "internal" });
  }
});






// Webhook Verify (GET)
// Retrocompatible: acepta VERIFY_TOKEN de .env como siempre,
// y además acepta cualquier verifyToken guardado en tenant_channels.
app.get("/webhook", async (req, res) => {
  const mode = req.query["hub.mode"];
  const token = String(req.query["hub.verify_token"] || "").trim();
  const challenge = req.query["hub.challenge"];

  if (!mode || mode !== "subscribe" || !token) return res.sendStatus(403);

  // 1) modo legacy (solo .env)
  if (token && token === String(VERIFY_TOKEN || "").trim()) {
    return res.status(200).send(challenge);
  }

  // 2) modo multi-tenant (DB)
  try {
    const rt = await findAnyByVerifyToken(token);
    if (rt) return res.status(200).send(challenge);
  } catch (e) {
    console.error("[webhook] verify db error:", e?.message || e);
  }

  return res.sendStatus(403);
});

// Webhook Entrante (POST)
app.post("/webhook", async (req, res) => {
  try {
    if (process.env.WHATSAPP_APP_SECRET && !isValidSignature(req)) {
      if (process.env.NODE_ENV === "production") return res.sendStatus(403);
      console.warn("⚠️ Webhook: firma inválida (ignorada en dev).");
    }
// ✅ PARSEO CORRECTO DEL PAYLOAD WHATSAPP

    const entry0 = req.body?.entry?.[0] || {};
    const change = entry0?.changes?.[0];
    const value  = change?.value;
    const messagingEvt = entry0?.messaging?.[0] || null;
const phoneNumberIdInbound =
  value?.metadata?.phone_number_id ||
  value?.metadata?.phoneNumberId ||
  value?.metadata?.phone_number ||
  null;

const instagramAccountIdInbound =
  value?.metadata?.instagram_account_id ||
  value?.metadata?.instagram_accountId ||
  value?.metadata?.page_id ||
  value?.metadata?.pageId ||
  value?.recipient?.id ||
  messagingEvt?.recipient?.id ||
  null;

// Runtime por canal desde Mongo.
// Si no existe, cae a .env (retrocompatible para WhatsApp).
let runtime = null;
let channelType = "whatsapp";
let msg = value?.messages?.[0];   // mensaje entrante (texto/audio/etc.)
const status = value?.statuses?.[0];   // (se ignora para persistencia)

if (!msg && messagingEvt?.message) {
  channelType = "instagram";
  const attachments = Array.isArray(messagingEvt.message.attachments) ? messagingEvt.message.attachments : [];
  const firstAttachment = attachments[0] || null;
  msg = {
    from: messagingEvt.sender?.id || "",
    text: messagingEvt.message.text ? { body: messagingEvt.message.text } : undefined,
    type: messagingEvt.message.text ? "text" : (firstAttachment?.type || "text"),
    instagram_mid: messagingEvt.message.mid || null,
    attachments,
  };
  if (!msg.text?.body && firstAttachment) {
    msg.text = { body: `[adjunto instagram: ${firstAttachment.type || "archivo"}]` };
  }
} else if (String(value?.messaging_product || "").trim().toLowerCase() === "instagram") {
  channelType = "instagram";
}

try {
  if (channelType === "instagram") runtime = await getRuntimeByInstagramAccountId(instagramAccountIdInbound);
  else runtime = await getRuntimeByPhoneNumberId(phoneNumberIdInbound);
} catch {}
const tenant = String(runtime?.tenantId || DEFAULT_TENANT_ID || TENANT_ID || "default").trim();

const channelOpts = {
  channelType,
  whatsappToken: runtime?.whatsappToken || null,
  phoneNumberId: runtime?.phoneNumberId || phoneNumberIdInbound || null,
  instagramAccountId: runtime?.instagramAccountId || instagramAccountIdInbound || null,
  instagramPageId: runtime?.instagramPageId || null,
  instagramAccessToken: runtime?.instagramAccessToken || null,
};

const aiOpts = {
  tenantId: tenant,
  openaiApiKey: runtime?.openaiApiKey || null
};

    if (!msg) {
      console.warn("[webhook] evento sin messages; se ignora");
      return res.sendStatus(200);
    }
    const from = msg.from;
    const sessionFrom = channelType === "instagram" ? `instagram:${from}` : from;
    let text   = (msg.text?.body || "").trim();
    const msgType = msg.type;
    let inboundLocation = null;

    // Normalización del texto según tipo de mensaje
    if (msg.type === "text" && msg.text?.body) {
      text = msg.text.body;
    } else if (msg.type === "audio" && msg.audio?.id) {
      try {
        const info = await getMediaInfo(msg.audio.id, channelOpts);
        const buf = await downloadMediaBuffer(info.url, channelOpts);
        const id = putInCache(buf, info.mime_type || "audio/ogg");
        const publicAudioUrl = `${req.protocol}://${req.get("host")}/cache/audio/${id}`;
        const tr = await transcribeAudioExternal({ publicAudioUrl, buffer: buf, mime: info.mime_type, ...aiOpts });
        text = String(tr?.text || "").trim() || "(audio sin texto)";
        // enriquecemos meta para admin/UI
        msg.__media = { kind: "audio", cacheId: id, publicUrl: publicAudioUrl, mime: info.mime_type || "audio/ogg" };
      } catch (e) {
        console.error("Audio/transcripción:", e.message);
        text = "(no se pudo transcribir el audio)";
      }
     } else if (msg.type === "image" && msg.image?.id) {
      try {
        const info = await getMediaInfo(msg.image.id, channelOpts);
        const buf = await downloadMediaBuffer(info.url, channelOpts);
        const id = putInCache(buf, info.mime_type || "image/jpeg");
        const publicImageUrl = `${req.protocol}://${req.get("host")}/cache/media/${id}`;

        const img = await analyzeImageExternal({
          publicImageUrl,
          mime: info.mime_type,
          purpose: "payment-proof",
          ...aiOpts
        });

        // Texto que alimenta al modelo conversacional
        text = img?.userText || "[imagen recibida]";

        // enriquecemos meta para admin/debug
        msg.__media = { cacheId: id, publicUrl: publicImageUrl, mime: info.mime_type, analysis: img?.json || null };
      } catch (e) {
        console.error("Imagen/análisis:", e?.message || e);
        text = "[imagen recibida]";
      }
    }

    else if (msg.type === "document" && msg.document?.id) {
      // Documento/archivo (no analizamos; solo registramos para el panel)
      const fn = String(msg.document?.filename || msg.document?.file_name || "archivo").trim() || "archivo";
      const cap = String(msg.document?.caption || "").trim();
      text = cap ? cap : `[archivo: ${fn}]`;
      if (cap) text += `\n[archivo: ${fn}]`;
      msg.__media = { kind: "document", filename: fn, mime: msg.document?.mime_type || null };
    } else if (msg.type === "video" && msg.video?.id) {
      const cap = String(msg.video?.caption || "").trim();
      text = cap ? cap : "[video]";
      msg.__media = { kind: "video", mime: msg.video?.mime_type || null };
    } else if (msg.type === "sticker" && msg.sticker?.id) {
      text = "[sticker]";
      msg.__media = { kind: "sticker", mime: msg.sticker?.mime_type || null };
    }

    else if (msg.type === "location" && msg.location) {
      // Ubicación compartida: intentamos reverse geocoding para obtener una dirección aproximada.
      const lat = Number(msg.location?.latitude);
      const lon = Number(msg.location?.longitude);
      const name = String(msg.location?.name || "").trim();
      const addr = String(msg.location?.address || "").trim();

      let formatted = addr;
      let rev = null;
      try {
        if (!formatted && Number.isFinite(lat) && Number.isFinite(lon)) {
          rev = await reverseGeocode(lat, lon);
          formatted = String(rev?.formatted_address || "").trim();
        }
      } catch {}

      inboundLocation = {
        lat: Number.isFinite(lat) ? lat : null,
        lon: Number.isFinite(lon) ? lon : null,
        name: name || null,
        address: addr || null,
        formatted_address: formatted || null,
        rev: rev || null,
      };
      msg.__location = inboundLocation;

      const link = (Number.isFinite(lat) && Number.isFinite(lon))
        ? `https://www.google.com/maps?q=${lat},${lon}`
        : "";

      // Texto que alimenta al modelo conversacional
      text = [
        "📍 El usuario compartió su ubicación.",
        (Number.isFinite(lat) && Number.isFinite(lon)) ? `Coordenadas: ${lat}, ${lon}.` : null,
        formatted ? `Dirección aproximada: ${formatted}.` : null,
        link ? `Link: ${link}` : null,
      ].filter(Boolean).join("\n");
    }


        // Asegurar conversación y guardar mensaje de usuario
    let conv = null;
     try {
       // Guardamos el canal/telefono por el que entró el mensaje para poder verlo en Admin UI
       conv = await upsertConversation(from, {
         channelType,
         phoneNumberId: channelOpts?.phoneNumberId || null,
         displayPhoneNumber: runtime?.displayPhoneNumber || null,
         instagramAccountId: channelOpts?.instagramAccountId || null,
         instagramPageId: channelOpts?.instagramPageId || null,
       }, tenant);
     } catch (e) { console.error("upsertConversation:", e?.message); }
     // Guardamos phoneNumberId para poder responder/operar por el mismo canal luego (admin, etc.)
 
    const convId = conv?._id;

   
console.log("[convId] "+ convId);

    // ✅ Si se creó una conversación nueva, reseteamos historial del LLM
    // para que un nuevo pedido no arrastre contexto del pedido anterior.
    if (convId) syncSessionConversation(tenant, sessionFrom, convId);

    if (convId && String(msg?.id || "").trim()) {
      try {
        const db = await getDb();
        const dupInbound = await db.collection("messages").findOne(
          withTenant({ conversationId: new ObjectId(String(convId)), "meta.raw.id": String(msg.id).trim() }, tenant),
          { projection: { _id: 1 } }
        );
        if (dupInbound) {
          console.log("[webhook] duplicate inbound already processed:", String(msg.id).trim());
          return res.sendStatus(200);
        }
      } catch (e) {
        console.warn("[webhook] duplicate inbound check error:", e?.message || e);
      }
    }


       if (convId) {
      console.log("[messages] about to save USER message", { convId, from, type: msg.type, textPreview: String(text).slice(0,80) });
      try {
        await saveMessageDoc({
          tenantId: tenant,
          conversationId: convId,
          waId: from,
          role: "user",
          content: text,
          type: msg.type || "text",
           meta: { raw: msg, media: msg.__media || null, location: msg.__location || null, channelType }
        });
      } catch (e) { console.error("saveMessage(user):", e?.message); }
    }

    // 🧑‍💻 Si la conversación está en modo manual, no respondemos automáticamente
    if (conv && conv.manualOpen) {
      console.log("[webhook] conversación en modo manualOpen=true; se omite respuesta automática.");
      return res.sendStatus(200);
   }

    // ================== Debounce configurable (solo mensajes text) ==================
// - Retrocompatible: si messageDebounceMs=0 => no cambia nada
// - Si >0: junta varios mensajes text y llama al LLM una sola vez
const debounceMs = clampInt(runtime?.messageDebounceMs ?? runtime?.debounceMs ?? 0, 0, 5000);
const debounceKey = convId
  ? `${tenant}:${channelType}:${channelOpts?.phoneNumberId || channelOpts?.instagramAccountId || "env"}:${convId}`
  : `${tenant}:${channelType}:${channelOpts?.phoneNumberId || channelOpts?.instagramAccountId || "env"}:${from}`;

// Si entra un mensaje NO-text (ej. location), cancelamos cualquier tanda
// pendiente de texto para que no se procese una dirección vieja después.
if (debounceMs > 0 && msg.type !== "text") {
  const pending = pendingTextBatches.get(debounceKey);
  if (pending) {
    pending.cancelled = true;
    pendingTextBatches.delete(debounceKey);
    console.log(`[debounce] lote cancelado por mensaje ${msg.type} key=${debounceKey}`);
  }
}

if (debounceMs > 0 && msg.type === "text") {
  // key estable: convId si existe; si no, tenant+canal+from
  const key = debounceKey;
  let batch = pendingTextBatches.get(key);
  if (!batch) {
    batch = { texts: [], leader: false, createdAt: Date.now(), cancelled: false };
    pendingTextBatches.set(key, batch);
  }

  // guardamos el texto de este mensaje
  const t = String(text || "").trim();
  if (t) {
    // dedupe simple: no agregar si es igual al último
    const last = batch.texts.length ? batch.texts[batch.texts.length - 1] : null;
    if (last !== t) batch.texts.push(t);
  }

  // si ya hay otro request “líder” esperando, este request termina acá
  if (batch.leader) {
    return res.sendStatus(200);
  }

  // este request se convierte en líder: espera y luego procesa todo junto
  batch.leader = true;
  await sleep(debounceMs);

  // recuperar lote final y liberarlo
  const finalBatch = pendingTextBatches.get(key);
  pendingTextBatches.delete(key);

  if (!finalBatch || finalBatch.cancelled) {
    console.log(`[debounce] líder cancelado key=${key}`);
    return res.sendStatus(200);
  }

  const parts = Array.isArray(finalBatch?.texts) ? finalBatch.texts : [];

  // dedupe extra (caso típico: "hola" al inicio y repetido al final)
  if (parts.length >= 2 && parts[0] === parts[parts.length - 1]) {
    parts.pop();
  }

  // limpiar vacíos
  const clean = parts.map(x => String(x || "").trim()).filter(Boolean);

  // unir como pidió el usuario: separados por coma
  // ejemplo: "hola", "quiero 2", "y 1" => "hola, quiero 2, y 1"
  text = clean.join(", ");

  if (!text) {
    console.log(`[debounce] lote vacío, no se procesa key=${key}`);
    return res.sendStatus(200);
  }
}

    const runKey = buildProcessingRunKey(tenant, `${channelType}:${channelOpts?.phoneNumberId || channelOpts?.instagramAccountId || "env"}`, convId, from);

    await withConversationRunLock(runKey, async ({ runSeq, isStale }) => {
      if (isStale()) {
        console.log(`[webhook][stale-run] abort before processing key=${runKey} seq=${runSeq}`);
        return;
      }

      // Si el mensaje NO es solo un cierre de cortesía, limpiamos el flag de sesión terminada
      if (!isPoliteClosingMessage(text)) {
        clearEndedFlag(tenant, sessionFrom);
      }
      if (hasActiveEndedFlag(tenant, sessionFrom)) {
        if (isPoliteClosingMessage(text)) {
          if (isStale()) {
            console.log(`[webhook][stale-run] polite close dropped key=${runKey} seq=${runSeq}`);
            return;
          }
          await require("./logic").sendChannelMessage(
            from,
            "¡Gracias! 😊 Cuando quieras hacemos otro pedido.",
            channelOpts
          );
          return;
        }
      }

          // ⚡ Fast-path: si el usuario confirma explícitamente, cerramos sin llamar al modelo
      // ⚡ Fast-path: aceptar también “sí/si” como confirmación explícita,
      // además de las variantes de “confirmar”.
       const userConfirms = isExplicitUserConfirmation(text);
      if (userConfirms) {
        // Tomamos último snapshot si existe
        let snapshot = null;
        try { snapshot = JSON.parse(require("./logic").__proto__ ? "{}" : "{}"); } catch {}
        // En minimal guardamos snapshot siempre; si no lo tenés a mano, seguimos y dejamos que el modelo lo complete
      }
      const gptReply = await getGPTReply(tenant, sessionFrom, text, aiOpts);
      if (isStale()) {
        console.log(`[webhook][stale-run] drop GPT reply key=${runKey} seq=${runSeq}`);
        return;
      }
   
  
    // También dispara si el usuario pide "total" o está en fase de confirmar
   const wantsDetail = /\b(detalle|detall|resumen|desglose|total|confirm(a|o|ar))\b/i
      .test(String(text || ""));


    let responseText = "Perdón, hubo un error. ¿Podés repetir?";
    let estado = null;
    let pedido = null;
    const prevPedidoSnapshot = convId ? await loadLastPedidoSnapshot(tenant, convId) : null;

    try {
      const parsed = JSON.parse(gptReply);
      estado = parsed.estado;
      pedido = mergePedidoState(prevPedidoSnapshot, parsed.Pedido || { items: [] });

      // 📍 Si el mensaje entrante fue una ubicación, la volcamos al Pedido.
      // Esto permite que el flujo siga aunque el usuario no escriba una dirección textual.
      try {
        if (inboundLocation && pedido && typeof pedido === "object") {
          if (!String(pedido.Entrega || "").trim()) {
            pedido.Entrega = "domicilio";
          }
          const dom0 = (typeof pedido.Domicilio === "string")
            ? { direccion: pedido.Domicilio }
            : (pedido.Domicilio || {});
          pedido.Domicilio = dom0;

          if (Number.isFinite(Number(inboundLocation.lat)) && Number.isFinite(Number(inboundLocation.lon))) {
            pedido.Domicilio.lat = Number(inboundLocation.lat);
            pedido.Domicilio.lon = Number(inboundLocation.lon);
          }

          const addr = String(
            inboundLocation.formatted_address ||
            inboundLocation.address ||
            inboundLocation.name ||
            ""
          ).trim();
          if (addr && !String(pedido.Domicilio.direccion || "").trim()) {
            pedido.Domicilio.direccion = addr;
          }
          if (!String(pedido.Domicilio.direccion || "").trim() &&
              Number.isFinite(Number(pedido.Domicilio.lat)) &&
              Number.isFinite(Number(pedido.Domicilio.lon))) {
            pedido.Domicilio.direccion = `Ubicación compartida (${pedido.Domicilio.lat}, ${pedido.Domicilio.lon})`;
          }

          const rev = inboundLocation.rev || null;
          if (rev && typeof rev === "object") {
            if (rev.street && !pedido.Domicilio.calle) pedido.Domicilio.calle = rev.street;
            if (rev.street_number && !pedido.Domicilio.numero) pedido.Domicilio.numero = rev.street_number;
            if (rev.barrio && !pedido.Domicilio.barrio) pedido.Domicilio.barrio = rev.barrio;
            if (rev.ciudad && !pedido.Domicilio.ciudad && !pedido.Domicilio.localidad) pedido.Domicilio.ciudad = rev.ciudad;
            if (rev.provincia && !pedido.Domicilio.provincia) pedido.Domicilio.provincia = rev.provincia;
            if (rev.cp && !pedido.Domicilio.cp) pedido.Domicilio.cp = rev.cp;
          }
        }
      } catch {}
      // 💰 Hidratar precios desde catálogo ANTES de recalcular (evita “Pollo entero @ 0”)
      try { pedido = await hydratePricesFromCatalog(pedido, tenant || null); } catch {}
      // 🚚 Asegurar ítem Envío con geocoding/distancia (awaitable, sin race)
      try { pedido = await ensureEnvioSmart(pedido, tenant || null); } catch {}

      // 🧽 Normalización defensiva: si el modelo puso la HORA en `Entrega`, corrige campos.
      if (pedido && typeof pedido.Entrega === "string" && /^\d{1,2}:\d{2}$/.test(pedido.Entrega)) {
        const hhmm = pedido.Entrega.length === 4 ? ("0" + pedido.Entrega) : pedido.Entrega;
        pedido.Hora = pedido.Hora || hhmm;
        // Si `Entrega` no es "domicilio" ni "retiro", dejalo vacío para que no bloquee isPedidoCompleto
        if (!/^(domicilio|retiro)$/i.test(pedido.Entrega)) pedido.Entrega = "";
      }

      const { pedidoCorr, mismatch, hasItems } = recalcAndDetectMismatch(pedido);
      pedido = pedidoCorr;

      const originalResponseText = coalesceResponse(parsed.response, pedido);

      // ✅ Si el modelo devuelve {"error":"..."} lo tratamos como MENSAJE AL USUARIO (no fatal):
      if (typeof parsed?.error === "string" && parsed.error.trim()) {
        responseText = parsed.error.trim();
      } else if (mismatch && hasItems) {
        // 🔒 La corrección de importes la resuelve el backend en silencio.
        // No volvemos a consultar al modelo porque eso altera el flujo visible,
        // puede repetir preguntas ya respondidas y ensucia el historial con prompts internos.
        console.log("[fix] importes corregidos por backend; se preserva el response original del asistente");
        responseText = originalResponseText;
      } else {
        responseText = originalResponseText;
      }
    } catch (e) {
      console.error("Error al parsear/corregir JSON:", e.message);
    }

    // ==============================
    // ✅ Guarda dura #1: si el pedido quedó como DOMICILIO pero no hay dirección válida,
    // forzar pregunta de dirección y evitar resumen/confirmación.
    // ==============================
    try {
      if (pedidoIsDomicilio(pedido) && !pedidoHasAddress(pedido)) {
        estado = "IN_PROGRESS";
        pedido = stripEnvioItemsFromPedido(pedido || {});
        try {
          const { pedidoCorr } = recalcAndDetectMismatch(pedido);
          pedido = pedidoCorr;
        } catch {}
        responseText = "¿A qué dirección te lo enviamos? 😊";
      }
    } catch (e) {
      console.warn("[delivery] Guarda de dirección obligatoria falló:", e?.message || e);
    }

    // ✅ Validar día y horario del pedido contra los horarios configurados del local
    try {
      // Normalizar fecha/hora usando fecha_pedido/hora_pedido como fuente de verdad.
      if (pedido && typeof pedido === "object") {
        pedido = normalizePedidoDateTimeFields(pedido);
      }

      if (pedido && typeof pedido === "object" && pedido.Fecha && pedido.Hora) {
        const db = await getDb();
        const hoursDocId = `store_hours:${tenant}`;
        const docHours = await db.collection("settings").findOne({ _id: hoursDocId });
        const hoursCfg = docHours?.hours || null;

        if (hoursCfg) {
          const schedCheck = validatePedidoSchedule(pedido, hoursCfg);
          if (!schedCheck.ok) {
            // Si la fecha/hora no es válida, sobreescribimos la respuesta textual
            // para que el usuario elija un nuevo horario dentro de las franjas.
            responseText = schedCheck.msg;
            // Si ya estaba en COMPLETED, lo bajamos a IN_PROGRESS para que siga el flujo
            if (estado === "COMPLETED") {
              estado = "IN_PROGRESS";
            }
          }
        }
      }
    } catch (e) {
      console.error("[hours] Error al validar fecha/hora de Pedido:", e?.message || e);
    }
     /*// Guardar respuesta del asistente:
     // 1) el TEXTO que se envía al cliente
     // 2) un SNAPSHOT JSON con el Pedido ya corregido, para que /admin pueda leerlo
     if (convId) {
       try {
         console.log("[messages] about to save ASSISTANT message", { convId: String(convId), from, len: String(responseText||"").length });
         await saveMessageDoc({
           tenantId: tenant,
           conversationId: convId,
           waId: from,
           role: "assistant",
           content: String(responseText || ""),
           type: "text",
          meta: { model: "gpt" }
         });
       } catch (e) {
         console.error("saveMessage(assistant text):", e?.message);
       }
 
       // Preparar el snapshot JSON a persistir (usa el pedido ya armado en este handler)
       try {
         const snap = {
           response: typeof responseText === "string" ? responseText : "",
           estado: typeof estado === "string" ? estado : "IN_PROGRESS",
           Pedido: pedido && typeof pedido === "object"
             ? pedido
             : { Entrega: "", Domicilio: {}, items: [], total_pedido: 0 }
         };
         const assistantSnapshot = JSON.stringify(snap);
         await saveMessageDoc({
           tenantId: tenant,
           conversationId: convId,
           waId: from,
           role: "assistant",
           content: assistantSnapshot,
           type: "json",
           meta: { model: "gpt", kind: "pedido-snapshot" }
         });
       } catch (e) {
         console.error("saveMessage(assistant json):", e?.message);
       }
     }
*/
    try {
      let finalBody = String(responseText ?? "").trim();
      const pagoEsTransferencia = /^transferencia$/i.test(String(pedido?.Pago || "").trim());
      const showTotalInSummary = !!(wantsDetail || pagoEsTransferencia);


      // 🛡️ Si el modelo solo respondió algo muy corto tipo
      // "tu pedido queda así" sin detallar productos/total,
      // generamos un resumen completo desde backend.
      if (
        finalBody &&
        /queda\s+as[ií]/i.test(finalBody) &&
        finalBody.length < 80 &&
        pedido &&
        Array.isArray(pedido.items) &&
        pedido.items.length > 0
      ) {
        // Usamos el resumen estándar del backend (sin ítem de envío)
        responseText = buildBackendSummary(pedido, { showTotal: showTotalInSummary });
        finalBody = String(responseText || "").trim();
      }

      if (!finalBody) {
        // No forzar resumen a menos que lo pidan explícitamente
        if (wantsDetail && pedido && Array.isArray(pedido.items) && pedido.items.length > 0) {
          responseText = buildBackendSummary(pedido, {
            showEnvio: wantsDetail,
            showTotal: showTotalInSummary
          });
        } else {
          // Texto neutro si ya hay contexto; saludo solo si no lo hay
          responseText = coalesceResponse("", pedido);
        }
      }
    } catch {}


    /*// 🚚 Visibilidad de "Envío": sólo en total/resumen/confirmación
    // (o cuando wantsDetail=true). En resúmenes parciales lo ocultamos.
    try {
      const text = String(responseText || "");
      const showsTotals = /\btotal\s*:?\s*\d/i.test(text);
      const isConfirmation = /¿\s*confirm/i.test(text) || /\u00BF\s*confirm/i.test(text) || /¿Confirmas\?/i.test(text);
      const explicitResumen = /resumen del pedido/i.test(text);
      const allowShipping = wantsDetail || showsTotals || isConfirmation || explicitResumen;
      if (!allowShipping) {
        // Remover líneas que muestren "Envío ..." (con o sin viñetas)
        responseText = text
          .split(/\r?\n/)
          .filter(line =>
            !/^\s*[-•*]\s*Env[ií]o\b/i.test(line) &&  // • Envío ...
            !/^\s*Env[ií]o\b/i.test(line)            // Envío ...
          )
          .join("\n")
          .replace(/\n{3,}/g, "\n\n")
          .trim();
      }
    } catch {}*/


    // 🔎 Leyenda de milanesas: mostrarla SOLO en resumen/total/confirmar.
    // Si el modelo generó un resumen aunque el usuario no haya pedido "total/resumen",
    // lo detectamos por el contenido del responseText.
    try {
      const hasMilanesas = (pedido?.items || []).some(i =>
        String(i?.descripcion || "").toLowerCase().includes("milanesa")
      );
      // ¿El texto "parece" un resumen?
      const looksLikeSummary = wantsDetail || /\b(resumen del pedido|total\s*:|\btotal\b|¿\s*confirm|¿confirmas|\u00BF\s*confirm)/i.test(String(responseText || ""));

      if (!hasMilanesas) {
        // Limpia cualquier rastro de la leyenda si no hay milanesas
        responseText = String(responseText || "")
          .replace(/\*?\s*Las milanesas se pesan al entregar; el precio se informa al momento de la entrega\.\s*\*?/i, "")
          .replace(/\n{3,}/g, "\n\n")
          .trim();
      } else {
        if (looksLikeSummary) {
          // Asegurar que la leyenda esté presente en resúmenes/totales/confirmaciones
          const hasLegend = /\bse pesan al entregar\b/i.test(String(responseText || ""));
          if (!hasLegend) {
            responseText = `${String(responseText || "").trim()}\n\n*Las milanesas se pesan al entregar; el precio se informa al momento de la entrega.*`;
          }
        } else {
          // No es resumen → quitar la leyenda si el modelo la hubiera puesto
          responseText = String(responseText || "")
            .replace(/\*?\s*Las milanesas se pesan al entregar; el precio se informa al momento de la entrega\.\s*\*?/i, "")
            .replace(/\n{3,}/g, "\n\n")
            .trim();
        }
      }
    } catch {}

    /*await require("./logic").sendWhatsAppMessage(from, responseText);
    // persistir respuesta del asistente
    if (convId) {
      try {
        await saveMessageDoc({
          tenantId: tenant,
          conversationId: convId,
          waId: from,
          role: "assistant",
          content: responseText,
          type: "text"
        });
      } catch (e) { console.error("saveMessage(assistant):", e?.message); }
    }*/

    //await require("./logic").sendWhatsAppMessage(from, responseText);
    // ⚠️ No persistimos aquí para evitar duplicados.
    // El guardado del mensaje del asistente (texto) y del snapshot JSON
    // se realiza más abajo en un único bloque.
    // 1) Enviar EXACTAMENTE el texto final (post-fallback/normalizaciones)
    //await require("./logic").sendWhatsAppMessage(from, responseText);
        // 1) Enviar EXACTAMENTE el texto final (post-fallback/normalizaciones)
    //    ⚠️ Garantía: nunca mandar vacío a WhatsApp


     let geoAddressWarning = "";
    let geoShouldPrependToSummary = false;

    // ==============================
    // ✅ Validación de dirección exacta (Google Maps)
    // Si Google Maps no la encuentra de forma exacta, NO borramos la dirección:
    // la conservamos tal cual la mandó el cliente, la dejamos registrada
    // para búsqueda manual del operador y evitamos usar coords/distancia/envío.
    // ==============================
    try {
      if (pedido?.Entrega?.toLowerCase() === "domicilio" && pedido?.Domicilio) {
        const dom = (typeof pedido.Domicilio === "string")
          ? { direccion: pedido.Domicilio }
          : (pedido.Domicilio || {});
        pedido.Domicilio = dom;


        const originalAddress = String(
          dom.direccion ||
          [dom.calle, dom.numero].filter(Boolean).join(" ") ||
          ""
        ).trim();
        const geoManualPending = !!dom.geo_manual_pending;
        const geoManualPendingAddress = String(dom.geo_manual_pending_address || "").trim();
        const samePendingAddress =
          geoManualPending &&
          geoManualPendingAddress &&
          geoManualPendingAddress === originalAddress;
        // Si ya tenemos coordenadas (ubicación compartida), NO forzamos geocoding exacto.
        const hasCoords = Number.isFinite(Number(dom.lat)) && Number.isFinite(Number(dom.lon));

        const addrParts = [
          dom.direccion,
          [dom.calle, dom.numero].filter(Boolean).join(" "),
          dom.barrio,
          dom.ciudad || dom.localidad,
          dom.provincia,
          dom.cp
        ].filter(Boolean);
        const address = addrParts.join(", ").trim();
        if (address && !hasCoords && !samePendingAddress) {
          const DEF_CITY = process.env.DEFAULT_CITY || "Venado Tuerto";
          const DEF_PROVINCE = process.env.DEFAULT_PROVINCE || "Santa Fe";
          const DEF_COUNTRY = process.env.DEFAULT_COUNTRY || "Argentina";
          const addressFinal = /,/.test(address)
            ? address
            : [address, DEF_CITY, DEF_PROVINCE, DEF_COUNTRY].filter(Boolean).join(", ");

          const geo = await geocodeAddress(addressFinal);
          const exact = Boolean(geo && geo.exact);

          if (!exact) {
            // Evitar cierre por pedido "completo" y evitar envío/distance incorrectos
            estado = "IN_PROGRESS";
            pedido.distancia_km = null;
            if (pedido.Domicilio && typeof pedido.Domicilio === "object") {
              delete pedido.Domicilio.lat;
              delete pedido.Domicilio.lon;
              pedido.Domicilio.direccion = originalAddress || String(dom.direccion || "").trim();
              pedido.Domicilio.geo_manual_pending = true;
              pedido.Domicilio.geo_manual_pending_address = pedido.Domicilio.direccion;

            }

            // Reemplazar cualquier envío previo por el envío más caro (fallback Infinity)
            if (!Array.isArray(pedido.items)) pedido.items = [];
            pedido.items = pedido.items.filter(i => !/env[ií]o/i.test(String(i?.descripcion || "")));
            try {
              const db = await getDb();
              const envioProd = await pickEnvioProductByDistance(db, tenant || null, Infinity);
              if (envioProd) {
                pedido.items.push({
                  id: envioProd._id || envioProd.id || 0,
                  descripcion: envioProd.descripcion,
                  cantidad: 1,
                  importe_unitario: Number(envioProd.importe || 0),
                  total: Number(envioProd.importe || 0),
                });
              }
            } catch (e) {
              console.warn("[geo] no se pudo aplicar envío fallback Infinity:", e?.message || e);
            }

            // Recalcular total con el envío fallback ya insertado
            try {
              const { pedidoCorr } = recalcAndDetectMismatch(pedido);
              pedido = pedidoCorr;
            } catch {}

            geoAddressWarning =
              `📍 Google Maps no encontró esa dirección, pero la registré igualmente como:\n*${pedido?.Domicilio?.direccion || originalAddress || address}*\n\n` +
              `La vamos a dejar guardada para que el operador la busque manualmente.`;
              geoShouldPrependToSummary = true;
 
            const nextStep = nextRequiredQuestionFromPedido(pedido);
            responseText = nextStep
              ? `${geoAddressWarning}\n\n${nextStep}`
              : geoAddressWarning;
          } else if (pedido.Domicilio && typeof pedido.Domicilio === "object") {
            delete pedido.Domicilio.geo_manual_pending;
            delete pedido.Domicilio.geo_manual_pending_address;
          }
        }
      }
    } catch (e) {
      console.warn("[geo] Validación de dirección exacta falló:", e?.message || e);
    }





    // ==============================
    // ✅ Guardas backend imprescindibles:
    // 1) domicilio sin dirección → pedir dirección
    // 2) forma de pago solo con domicilio + dirección
    // 3) no preguntar condimento de pollo sin pollo real en Pedido.items
    // 4) no repreguntar tipo de milanesa si ya quedó resuelto
    // 5) bloquear resumen/confirmación/COMPLETED si faltan datos obligatorios
    // ==============================
    try {
      const guardRes = applyCriticalPedidoGuards({ pedido, responseText, estado, currentText: text });
      pedido = guardRes.pedido;
      responseText = guardRes.responseText;
      estado = guardRes.estado;
      if (Array.isArray(guardRes.guardHits) && guardRes.guardHits.length) {
        console.log("[guard] backend:", guardRes.guardHits.join(", "));
      }
    } catch (e) {
      console.warn("[guard] applyCriticalPedidoGuards error:", e?.message || e);
    }

    // ==============================
    // ✅ Confirmación obligatoria antes de cerrar:
    // si el pedido ya está completo y NO es transferencia,
    // jamás cerramos sin una confirmación explícita del usuario.
    // ==============================
    try {
      const pedidoListoParaCerrar = pedidoHasRequiredFieldsForClose(pedido);
      const pagoEsTransferencia = /^transferencia$/i.test(String(pedido?.Pago || "").trim());
      const userConfirmedNow = isExplicitUserConfirmation(text);

      if (pedidoListoParaCerrar && !pagoEsTransferencia && !userConfirmedNow) {
        estado = "IN_PROGRESS";
        const summaryText = buildBackendSummary(pedido, {
          showEnvio: wantsDetail,
          showTotal: wantsDetail || pagoEsTransferencia
        });
        responseText = (geoShouldPrependToSummary && geoAddressWarning)
          ? `${geoAddressWarning}\n\n${summaryText}`
          : summaryText;
      }
    } catch (e) {
      console.warn("[confirm] no se pudo forzar confirmación previa:", e?.message || e);
    }
    // ==============================
    // ✅ Si el pedido es a domicilio y ya tiene dirección,
    // forzar que el resumen / confirmación / mensaje final
    // muestre: "Modalidad: Envío ({dirección})"
    // ==============================
    try {
      const entrega = String(pedido?.Entrega || "").trim().toLowerCase();
      const domDir = String(
        (typeof pedido?.Domicilio === "string")
          ? pedido.Domicilio
          : (pedido?.Domicilio?.direccion || "")
      ).trim();

      if (entrega === "domicilio" && domDir) {
        const txt = String(responseText || "");
        const looksLikeSummaryOrFinal =
          /(?:\btu pedido es\b|\bresumen\b|\bpedido ha sido confirmado\b|¿\s*confirm|\bmodalidad\s*:|\bhora\s*:|\bnombre(?: y apellido)?\s*:)/i.test(txt);

        if (looksLikeSummaryOrFinal) {
          const desiredLine = `🚚 Modalidad: Envío (${domDir})`;

          if (/\bmodalidad\s*:/i.test(txt)) {
            responseText = txt.replace(
              /(🚚\s*)?Modalidad\s*:\s*(?:domicilio|env[ií]o)(?:\s*\([^\n)]*\))?/i,
              desiredLine
            );
          } else if (!new RegExp(`Envío\\s*\\(${domDir.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\)`, "i").test(txt)) {
            if (/🕒\s*Hora\s*:/i.test(txt)) {
              responseText = txt.replace(/(🕒\s*Hora\s*:)/i, `${desiredLine}\n$1`);
            } else if (/👤\s*(?:Nombre|Nombre y apellido)\s*:/i.test(txt)) {
              responseText = txt.replace(/(👤\s*(?:Nombre|Nombre y apellido)\s*:)/i, `${desiredLine}\n$1`);
            } else {
              responseText = `${txt.trim()}\n${desiredLine}`.trim();
            }
          }
        }
      }
    } catch (e) {
      console.warn("[delivery] no se pudo forzar dirección visible en response:", e?.message || e);
    }
 
    const responseTextSafe = String(responseText || "").trim()
      || (wantsDetail && pedido && Array.isArray(pedido.items) && pedido.items.length
          ? buildBackendSummary(pedido, {
              showEnvio: wantsDetail,
              showTotal: wantsDetail || /^transferencia$/i.test(String(pedido?.Pago || "").trim())
            })
          : "Perfecto, sigo acá. ¿Querés confirmar o cambiar algo?");

    // ==============================
    // ✅ Determinar el estado final ANTES de enviar/persistir texto
    // para que el mensaje visible y el estado guardado queden sincronizados.
    // ==============================
    const _tNormPre = String(text || "")
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "");
    const userWantsCancelRaw =
      /\b(cancel|anul)(ar|o|a|e|en|ado|ada)?\b/.test(_tNormPre) ||
      /\bdar de baja\b/.test(_tNormPre);
    const userCancelNeg =
      /\bno\s+(quiero\s+)?cancel/.test(_tNormPre) ||
      /\bno\s+(quiero\s+)?anul/.test(_tNormPre);
    const userCancelled = !!(userWantsCancelRaw && !userCancelNeg);

    const userConfirmsFast = isExplicitUserConfirmation(text);
    const pagoEsTransferenciaFinal = /^transferencia$/i.test(String(pedido?.Pago || "").trim());
   const willComplete = !!(!pagoEsTransferenciaFinal && userConfirmsFast && isPedidoCompleto(pedido));
    const closeStatus =
      (userCancelled || estado === "CANCELLED")
        ? "CANCELLED"
        : (willComplete ? "COMPLETED" : null);
    const finalEstado = closeStatus || (typeof estado === "string" ? estado : "IN_PROGRESS");

    let outboundText = String(responseTextSafe || "").trim();
    if (closeStatus === "COMPLETED") {
      const hasConfirmedPhrase =
        /\b(qued[oó]\s+confirmado|pedido\s+confirmado)\b/i.test(outboundText);
      const stillAsksConfirmation =
        /¿\s*confirm(amos|ás|as)\b/i.test(outboundText);

      if (!hasConfirmedPhrase || stillAsksConfirmation) {
        outboundText = buildBackendSummary(pedido, {
          showEnvio: wantsDetail,
          showTotal: wantsDetail || pagoEsTransferenciaFinal,
          askConfirmation: false,
          intro: "Perfecto, tu pedido quedó confirmado ✅\n\n🧾 Resumen del pedido:"
        });
      }
    }
    try {
      setAssistantPedidoSnapshot(tenant, sessionFrom, pedido, finalEstado);
      replaceLastAssistantHistory(tenant, sessionFrom, JSON.stringify({
        response: outboundText,
        estado: finalEstado,
        Pedido: (pedido && typeof pedido === "object")
          ? pedido
          : { Entrega: "", Domicilio: {}, items: [], total_pedido: 0 }
      }));
    } catch (e) {
      console.warn("[history] no se pudo reemplazar la última respuesta del asistente:", e?.message || e);
    }

    if (isStale()) {
      console.log(`[webhook][stale-run] abort before send key=${runKey} seq=${runSeq}`);
      return;
    }
     await require("./logic").sendChannelMessage(from, outboundText, channelOpts);
    if (isStale()) {
      console.log(`[webhook][stale-run] abort after send before persist key=${runKey} seq=${runSeq}`);
      return;
    }
   
    
    
    
    // 2) Guardar ahora el mismo texto y el snapshot JSON (mismo estado/pedido finales)




    if (convId) {
      try {
        await saveMessageDoc({
          tenantId: tenant,
          conversationId: convId,
          waId: from,
          role: "assistant",
          content: String(outboundText || ""),
          type: "text",
          meta: { model: "gpt" }
        });
      } catch (e) {
        console.error("saveMessage(assistant text final):", e?.message);
      }
      try {
        const snap = {
          response: typeof outboundText === "string" ? outboundText : "",
          estado: finalEstado,
          Pedido: (pedido && typeof pedido === "object")
            ? pedido
            : { Entrega: "", Domicilio: {}, items: [], total_pedido: 0 }
        };
        await saveMessageDoc({
          tenantId: tenant,
          conversationId: convId,
          waId: from,
          role: "assistant",
          content: JSON.stringify(snap),
          type: "json",
          meta: { model: "gpt", kind: "pedido-snapshot" }
        });
      } catch (e) {
        console.error("saveMessage(assistant json final):", e?.message);
      }
    }



    if (isStale()) {
      console.log(`[webhook][stale-run] abort before post-send side effects key=${runKey} seq=${runSeq}`);
      return;
    }

    try {
     // 🔹 Distancia + geocoding + Envío dinámico
      let distKm = null;
      if (pedido?.Entrega?.toLowerCase() === "domicilio" && pedido?.Domicilio) {
        const store = getStoreCoords();
        if (store) {
          let { lat, lon } = pedido.Domicilio;

          // Geocodificamos si faltan coords
          if (!(typeof lat === "number" && typeof lon === "number")) {
            const addrParts = [
              pedido.Domicilio.direccion,
              [pedido.Domicilio.calle, pedido.Domicilio.numero].filter(Boolean).join(" "),
              pedido.Domicilio.barrio,
              pedido.Domicilio.ciudad || pedido.Domicilio.localidad,
              pedido.Domicilio.provincia,
              pedido.Domicilio.cp
            ].filter(Boolean);
            const address = addrParts.join(", ").trim();
            if (address) {
              // ➕ Si el usuario solo escribió "Moreno 2862", agregamos localidad por defecto
              const DEF_CITY = process.env.DEFAULT_CITY || "Venado Tuerto";
              const DEF_PROVINCE = process.env.DEFAULT_PROVINCE || "Santa Fe";
              const DEF_COUNTRY = process.env.DEFAULT_COUNTRY || "Argentina";
              const addressFinal = /,/.test(address) ? address : [address, DEF_CITY, DEF_PROVINCE, DEF_COUNTRY].filter(Boolean).join(", ");
              console.log(`[geo] Direccion compilada='${addressFinal}'`);
             
              const geo = await geocodeAddress(addressFinal);
              if (geo && geo.exact) {
                lat = geo.lat; lon = geo.lon;
                pedido.Domicilio.lat = lat;
                pedido.Domicilio.lon = lon;
                console.log(`[geo] OK lat=${lat}, lon=${lon}`);
              } else {
                
                const reason = geo ? `inexacto (partial=${geo.partial_match}, type=${geo.location_type})` : "sin resultado";
                console.warn(`[geo] Geocoding ${reason}. No uso coords para distancia/envío.`);
 
              }
            }
          }

          // Si ya tenemos coords → calcular distancia
          if (typeof lat === "number" && typeof lon === "number") {
            distKm = calcularDistanciaKm(store.lat, store.lon, lat, lon);
            pedido.distancia_km = distKm;
            console.log(`📍 Distancia calculada al domicilio: ${distKm} km`);

            // Buscar producto de Envío según km
            const db = await getDb();
            const envioProd = await pickEnvioProductByDistance(db, tenant || null, distKm);
            if (envioProd && typeof envioProd.importe === "number") {
                 console.log(`[envio] Seleccionado por distancia: '${envioProd.descripcion}' @ ${envioProd.importe}`);
            
              const idx = (pedido.items || []).findIndex(i =>
                String(i.descripcion || "").toLowerCase().includes("envio")
              );
              if (idx >= 0) {
                 const cantidad = Number(pedido.items[idx].cantidad || 1);
                const prevImporte = Number(pedido.items[idx].importe_unitario || 0);
                const prevDesc = String(pedido.items[idx].descripcion || "");

                // ✅ Actualizar TODO: id, descripción, unitario y total
                pedido.items[idx].id = envioProd._id || pedido.items[idx].id || 0;
                pedido.items[idx].descripcion = envioProd.descripcion;
                pedido.items[idx].importe_unitario = Number(envioProd.importe);
                pedido.items[idx].total = cantidad * Number(envioProd.importe);

                const changed = (prevImporte !== Number(envioProd.importe)) || (prevDesc !== envioProd.descripcion);
                if (changed) console.log(`[envio] Ajustado item existente: '${prevDesc}' @ ${prevImporte} -> '${envioProd.descripcion}' @ ${envioProd.importe}`);
                } else {
                // 🆕 No existía el item Envío: lo insertamos ahora
                (pedido.items ||= []).push({
                  id: envioProd._id || 0,
                  descripcion: envioProd.descripcion,
                  cantidad: 1,
                  importe_unitario: Number(envioProd.importe),
                  total: Number(envioProd.importe),
                });
                console.log(`[envio] Insertado item envío: '${envioProd.descripcion}' @ ${envioProd.importe}`);
            
              }
              // Recalcular total localmente
              let totalCalc = 0;
              (pedido.items || []).forEach(it => {
                const cant = Number(String(it.cantidad).replace(/[^\d.-]/g,'')) || 0;
                const unit = Number(String(it.importe_unitario).replace(/[^\d.-]/g,'')) || 0;
                it.total = cant * unit;
                totalCalc += it.total;
              });
              pedido.total_pedido = totalCalc;

              // 🔔 (opcional pero útil): si hubo cambio de envío, avisar al cliente con total correcto
            /*  try {
                const totalStr = (Number(pedido.total_pedido)||0).toLocaleString("es-AR");
                await require("./logic").sendWhatsAppMessage(
                  from,
                  `Actualicé el envío según tu dirección (${distKm} km): ${envioProd.descripcion}. Total: $${totalStr}.`
                );
              } catch (e) {
                console.warn("[envio] No se pudo notificar ajuste de envío:", e?.message);
              }*/

            }
          }
        }
      }
   // 🔹 Mantener snapshot del asistente
setAssistantPedidoSnapshot(tenant, sessionFrom, pedido, estado);

 
	

// 🔹 Persistir pedido definitivo en MongoDB (upsert por conversationId) cuando está COMPLETED
if (willComplete && pedido && convId) {
  try {
    const db = await getDb();
    const nowOrder = new Date();
    const convObjectId = new ObjectId(String(convId));
    const filter = { conversationId: convObjectId, ...(tenant ? { tenantId: tenant } : {}) };

    await db.collection("orders").updateOne(
      filter,
      {
        $set: {
          tenantId: (tenant || null),
          from,
          conversationId: convObjectId,
          pedido,
          estado: "COMPLETED",
          status: "COMPLETED",
          distancia_km: typeof pedido?.distancia_km === "number" ? pedido.distancia_km : (distKm ?? null),
          updatedAt: nowOrder,
        },
        $setOnInsert: { createdAt: nowOrder }
      },
      { upsert: true }
    );
  } catch (e) {
    console.error("[orders] error upsert COMPLETED:", e?.message || e);
  }
}
    } catch {}
    try {
	  
	      if (closeStatus) {
	        await closeConversation(convId, closeStatus);
          
	        // 🔄 Mantener estado del pedido en orders (si existe) / crear si no existe
	        try {
	          const db = await getDb();
	          const nowOrder = new Date();
	          const convObjectId = new ObjectId(String(convId));
	          await db.collection("orders").updateOne(
	            { conversationId: convObjectId, ...(tenant ? { tenantId: tenant } : {}) },
	            {
	              $set: {
	                tenantId: (tenant || null),
	                from,
	                conversationId: convObjectId,
	                ...(pedido ? { pedido } : {}),
	                estado: closeStatus,
	                status: closeStatus,
	                updatedAt: nowOrder,
	              },
	              $setOnInsert: { createdAt: nowOrder }
	            },
	            { upsert: true }
	          );
	        } catch (e) {
	          console.error("[orders] error upsert closeStatus:", e?.message || e);
	        }

	        // 🧹 limpiar sesión en memoria para que el próximo msg empiece conversación nueva
	        markSessionEnded(tenant, sessionFrom);
	      }
    } catch {}
    });

    res.sendStatus(200);
  } catch (e) {
    console.error("POST /webhook error:", e?.message || e);
    res.sendStatus(500);
  }
});

/*const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Servidor corriendo en http://localhost:${PORT}`);
});

*/

// --- reemplazar desde aquí ---
const PORT = process.env.PORT || 3000;

if (require.main === module) {
  const server = app.listen(PORT, () => {
    console.log(`🚀 Servidor corriendo en http://localhost:${PORT}`);
  });

  // Cierre prolijo en deployment (Render/Heroku envían SIGTERM)
  process.on('SIGTERM', () => {
    console.log('SIGTERM recibido. Cerrando server...');
    server.close(() => process.exit(0));
  });
}

// Exportar la app para que otros módulos (tests/otros entrypoints) puedan importarla sin abrir un puerto.
module.exports = app;
// --- reemplazar hasta aquí ---
