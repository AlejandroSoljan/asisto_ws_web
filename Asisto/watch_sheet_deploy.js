/**
 * watch_sheet_deploy.js
 * - Lee Google Sheet (CSV publicado)
 * - Para cada cliente: asegura release(tag) y reinicia PM2 apuntando a esa versión
 */

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const SHEET_CSV_URL = process.env.SHEET_CSV_URL;
if (!SHEET_CSV_URL) {
  console.error("Falta SHEET_CSV_URL (URL CSV publicado).");
  process.exit(1);
}

const BASE = process.env.ASISTO_BASE || "/srv/asisto";
const REPO_DIR = path.join(BASE, "repo");
const RELEASES_DIR = path.join(BASE, "releases");
const SESSIONS_DIR = path.join(BASE, "sessions");

const POLL_SECONDS = Number(process.env.POLL_SECONDS || 60);

function sh(cmd, opts = {}) {
  return execSync(cmd, { stdio: "pipe", encoding: "utf-8", ...opts }).trim();
}

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

async function fetchText(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.text();
}

function parseCSV(text) {
  const lines = text.split(/\r?\n/).filter(Boolean);
  const header = lines.shift().split(",").map(s => s.trim());
  return lines.map(line => {
    const cols = line.split(",").map(s => s.trim());
    const row = {};
    header.forEach((h, i) => row[h] = cols[i] ?? "");
    return row;
  });
}

function ensureBase() {
  fs.mkdirSync(BASE, { recursive: true });
  fs.mkdirSync(RELEASES_DIR, { recursive: true });
  fs.mkdirSync(SESSIONS_DIR, { recursive: true });
  if (!fs.existsSync(path.join(REPO_DIR, ".git"))) {
    throw new Error(`No existe repo en ${REPO_DIR}. Clonalo ahí (git clone ... ${REPO_DIR}).`);
  }
}

function gitFetchTags() {
  sh(`git -C ${REPO_DIR} fetch --tags --force`);
}

function ensureRelease(tag) {
  const releaseDir = path.join(RELEASES_DIR, tag);

  if (!fs.existsSync(releaseDir)) {
    fs.mkdirSync(releaseDir, { recursive: true });
    log(`Checkout ${tag} -> ${releaseDir}`);
    sh(`git -C ${REPO_DIR} --work-tree=${releaseDir} checkout ${tag} -- .`);

    log(`npm ci en ${releaseDir}`);
    sh(`npm -C ${releaseDir} ci --omit=dev`);
  }

  return releaseDir;
}

function pm2Ensure(clientId, releaseDir, headless) {
  const pm2Name = `asisto-${clientId}`;
  const entry = path.join(releaseDir, "app_asisto.js");

  const sessionPath = path.join(SESSIONS_DIR, clientId);

  // env para ese proceso
  const env = {
    CLIENT_ID: clientId,
    SESSION_PATH: sessionPath,
    HEADLESS: String(headless ?? ""), // si querés leerlo en tu script
  };

  // Crear dir sesión
  fs.mkdirSync(sessionPath, { recursive: true });

  // Si existe: restart con cwd correcto y env actualizado
  try {
    sh(`pm2 describe ${pm2Name}`);
    log(`pm2 restart ${pm2Name}`);
    sh(`pm2 restart ${pm2Name} --update-env`, { env: { ...process.env, ...env } });
  } catch {
    log(`pm2 start ${pm2Name}`);
    sh(`pm2 start ${entry} --name ${pm2Name} --cwd ${releaseDir}`, { env: { ...process.env, ...env } });
  }
}

function loadRuntime() {
  const f = path.join(BASE, "runtime.json");
  if (!fs.existsSync(f)) return {};
  try { return JSON.parse(fs.readFileSync(f, "utf-8")); } catch { return {}; }
}

function saveRuntime(rt) {
  const f = path.join(BASE, "runtime.json");
  fs.writeFileSync(f, JSON.stringify(rt, null, 2));
}

(async function main() {
  ensureBase();
  gitFetchTags();

  let runtime = loadRuntime();

  while (true) {
    try {
      const csv = await fetchText(SHEET_CSV_URL);
      const rows = parseCSV(csv);

      // rt shape: { [client_id]: { app_tag, headless, updated_at } }
      for (const r of rows) {
        const clientId = (r.client_id || "").trim();
        const appTag = (r.app_tag || "").trim();
        const enabled = String(r.enabled || "").toUpperCase().trim();
        const headless = String(r.headless || "").toUpperCase().trim();

        if (!clientId || !appTag) continue;
        if (enabled === "FALSE" || enabled === "0" || enabled === "NO") continue;

        const wantHeadless = (headless === "TRUE" || headless === "1" || headless === "YES") ? true : false;

        const cur = runtime[clientId]?.app_tag || null;
        const curH = runtime[clientId]?.headless ?? null;

        if (cur !== appTag || curH !== wantHeadless) {
          log(`Cambio: ${clientId} ${cur || "(none)"} -> ${appTag} (headless=${wantHeadless})`);

          const releaseDir = ensureRelease(appTag);
          pm2Ensure(clientId, releaseDir, wantHeadless);

          runtime[clientId] = {
            app_tag: appTag,
            headless: wantHeadless,
            updated_at: new Date().toISOString()
          };
          saveRuntime(runtime);
        }
      }
    } catch (e) {
      log(`ERROR: ${e.message}`);
    }

    await new Promise(r => setTimeout(r, POLL_SECONDS * 1000));
  }
})().catch(e => {
  console.error(e);
  process.exit(1);
});
