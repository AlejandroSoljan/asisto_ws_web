/**
 * watcher_client.js
 * - Lee un Google Sheet publicado como CSV
 * - Toma la fila cuyo ClienteId == MI_CLIENTE_ID
 * - Si actualiza == "S":
 *    - si cambió Version: git fetch --tags + checkout tag + npm ci
 *    - siempre asegura ecosystem.local.config.js con ENV (api/smtp/etc.)
 *    - pm2 startOrRestart con --update-env
 *
 * Variables de entorno requeridas:
 *  - SHEET_URL     : URL CSV publicado (pub?output=csv)
 *  - MI_CLIENTE_ID : ClienteId (ej "1")
 *  - REPO_DIR      : carpeta donde está clonado el repo (ej "C:\\Asisto")
 *
 * Opcionales:
 *  - POLL_SECONDS  : default 60
 *  - PM2_NAME      : default "asisto-<ClienteId>"
 */

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const SHEET_URL = process.env.SHEET_URL;
const MI_CLIENTE_ID = String(process.env.MI_CLIENTE_ID || "").trim();
const REPO_DIR = process.env.REPO_DIR;
const POLL_SECONDS = Number(process.env.POLL_SECONDS || 60);
const PM2_NAME = process.env.PM2_NAME || `asisto-${MI_CLIENTE_ID}`;

if (!SHEET_URL) throw new Error("Falta SHEET_URL (URL CSV publicado del Google Sheet).");
if (!MI_CLIENTE_ID) throw new Error("Falta MI_CLIENTE_ID (ej: 1).");
if (!REPO_DIR) throw new Error("Falta REPO_DIR (ej: C:\\Asisto).");

const STATE_FILE = path.join(REPO_DIR, `.watch_state_cliente_${MI_CLIENTE_ID}.json`);
const ECOSYSTEM_FILE = path.join(REPO_DIR, `ecosystem.local.config.js`);

// ---------- Helpers ----------
function run(cmd, cwd = REPO_DIR) {
  console.log(`[cmd] ${cmd}`);
  execSync(cmd, { stdio: "inherit", cwd });
}

async function fetchText(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} leyendo Sheet`);
  return await res.text();
}

// CSV robusto: comillas, comas dentro de campos, "" escapado
function parseCsvRobusto(text) {
  const lines = text.split(/\r?\n/).filter(l => l.length > 0);
  if (lines.length === 0) return [];
  const header = parseCsvLine(lines[0]).map(s => s.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = parseCsvLine(lines[i]);
    const row = {};
    header.forEach((h, idx) => row[h] = (cols[idx] ?? "").trim());
    rows.push(row);
  }
  return rows;
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ",") {
        out.push(cur);
        cur = "";
      } else {
        cur += ch;
      }
    }
  }
  out.push(cur);
  return out;
}

function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return { lastTag: null, lastHash: null };
    return JSON.parse(fs.readFileSync(STATE_FILE, "utf-8"));
  } catch {
    return { lastTag: null, lastHash: null };
  }
}

function saveState(st) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(st, null, 2), "utf-8");
}

function normalizeActualiza(x) {
  return String(x || "").trim().toUpperCase();
}

// Tu sheet trae Version como "1.23". En git tags suele ser "v1.23" o "1.23".
// Probamos ambos.
function candidateTags(version) {
  const v = String(version || "").trim();
  if (!v) return [];
  if (v.startsWith("v")) return [v, v.replace(/^v/, "")];
  return [v, `v${v}`];
}

function computeRowHash(row) {
  // hash simple: JSON string estable de campos que nos importan para ENV
  // Si querés incluir más columnas, agregalas acá.
  const payload = {
    ClienteId: row.ClienteId,
    Nombre: row.Nombre,
    Script: row.Script,
    Version: row.Version,
    api: row.api,
    smtp: row.smtp,
    usr_smtp: row.usr_smtp,
    pass_smtp: row.pass_smtp,
    puerto_smtp: row.puerto_smtp,
    email_saliente: row.email_saliente,
    email_envio: row.email_envio,
    actualiza: row.actualiza,
    Script_tl: row.Script_tl,
  };
  return JSON.stringify(payload);
}

function writeEcosystem(row) {
  const clientId = `cliente-${row.ClienteId}`;

  // Sesión por cliente (NO se pisa)
  const sessionPath = path.join(REPO_DIR, "sessions", clientId);

  // Si en el futuro agregás columnas headless/chrome_path/port, el watcher las pasa también
  const env = {
    CLIENT_ID: clientId,
    SESSION_PATH: sessionPath,

    API_URL: row.api || "",

    SMTP_HOST: row.smtp || "",
    SMTP_USER: row.usr_smtp || "",
    SMTP_PASS: row.pass_smtp || "",
    SMTP_PORT: row.puerto_smtp || "",
    EMAIL_SALIENTE: row.email_saliente || "",
    EMAIL_ENVIO: row.email_envio || "",
  };

  fs.mkdirSync(sessionPath, { recursive: true });

  const content = `module.exports = {
  apps: [
    {
      name: "${PM2_NAME}",
      script: "app_asisto.js",
      cwd: ${JSON.stringify(REPO_DIR)},
      interpreter: "node",
      autorestart: true,
      watch: false,
      time: true,
      env: ${JSON.stringify(env, null, 2)}
    }
  ]
};`;

  fs.writeFileSync(ECOSYSTEM_FILE, content, "utf-8");
}

function ensureGitRepo() {
  if (!fs.existsSync(REPO_DIR)) throw new Error(`REPO_DIR no existe: ${REPO_DIR}`);
  if (!fs.existsSync(path.join(REPO_DIR, ".git"))) {
    throw new Error(`REPO_DIR no parece repo git (falta .git): ${REPO_DIR}`);
  }
}

function checkoutTag(tag) {
  // trae tags y cambia versión
  run(`git fetch --tags --force`);
  run(`git checkout ${tag}`);
  run(`npm ci`);
}

function pm2Apply() {
  // startOrRestart aplica cambios a un proceso existente o lo crea
  run(`pm2 startOrRestart "${ECOSYSTEM_FILE}" --update-env`);
  run(`pm2 save`);
}

// ---------- Main loop ----------
async function tick() {
  ensureGitRepo();

  const csv = await fetchText(SHEET_URL);
  const rows = parseCsvRobusto(csv);

  const row = rows.find(r => String(r.ClienteId || "").trim() === MI_CLIENTE_ID);
  if (!row) {
    console.log(`No hay fila para ClienteId=${MI_CLIENTE_ID}`);
    return;
  }

  const actualiza = normalizeActualiza(row.actualiza);
  const version = String(row.Version || "").trim();

  console.log(`ClienteId=${row.ClienteId} Version=${version} actualiza=${actualiza}`);

  if (actualiza !== "S") return;
  if (!version) return;

  const st = loadState();

  // Determinar tag real
  let chosenTag = null;
  for (const t of candidateTags(version)) {
    try {
      // git rev-parse verifica si existe el tag/commit
      execSync(`git rev-parse -q --verify "refs/tags/${t}"`, { cwd: REPO_DIR, stdio: "ignore" });
      chosenTag = t;
      break;
    } catch {
      // no existe ese tag
    }
  }

  // Si no encuentra tag, intentamos igual con el primero (por si es branch)
  if (!chosenTag) chosenTag = candidateTags(version)[0];

  const rowHash = computeRowHash(row);

  // Si cambió versión: checkout + npm ci
  if (st.lastTag !== chosenTag) {
    console.log(`Cambio de versión: ${st.lastTag || "(none)"} -> ${chosenTag}`);
    checkoutTag(chosenTag);
  }

  // Si cambió config o versión (o primera vez): reescribir ecosystem + restart pm2
  if (st.lastHash !== rowHash || st.lastTag !== chosenTag) {
    console.log(`Aplicando configuración PM2/env (hash cambió=${st.lastHash !== rowHash})`);
    writeEcosystem(row);
    pm2Apply();
    saveState({ lastTag: chosenTag, lastHash: rowHash });
  } else {
    console.log("Sin cambios.");
  }
}

(async function main() {
  console.log("Watcher iniciado...");
  await tick();
  setInterval(() => tick().catch(e => console.error("tick error:", e)), POLL_SECONDS * 1000);
})().catch(e => {
  console.error(e);
  process.exit(1);
});
