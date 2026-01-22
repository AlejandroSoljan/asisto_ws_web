const path = require("path");
const fs = require("fs");

const REPO_DIR = process.env.REPO_DIR || process.cwd();
const SESSIONS_BASE = process.env.SESSIONS_BASE || path.join(REPO_DIR, "sessions");
const CLIENTS_FILE = process.env.CLIENTS_FILE || path.join(REPO_DIR, "clients.json");

// Si usás releases por versión:
const RELEASES_BASE = process.env.RELEASES_BASE || path.join(REPO_DIR, "releases");
// Si NO usás releases y todo corre desde el mismo repo, poné: const useReleases = false;
const useReleases = true;

function loadClients() {
  if (!fs.existsSync(CLIENTS_FILE)) return [];
  return JSON.parse(fs.readFileSync(CLIENTS_FILE, "utf8"));
}

const clients = loadClients()
  .filter(c => String(c.actualiza || "").toUpperCase() === "S");

module.exports = {
  apps: clients.map(c => {
    const clientId = `cliente-${c.ClienteId}`;
    const version = String(c.Version || "main").trim();

    const cwd = useReleases
      ? path.join(RELEASES_BASE, version)
      : REPO_DIR;

    const script = path.join(cwd, "app_asisto.js");

    return {
      name: `asisto-${clientId}`,
      script,
      cwd,
      interpreter: "node",
      // Ajustá según tu server
      autorestart: true,
      watch: false,
      max_restarts: 20,
      time: true,

      env: {
        CLIENT_ID: clientId,
        SESSION_PATH: path.join(SESSIONS_BASE, clientId),

        // overrides que agregamos al app_asisto.js
        API_URL: c.api || "",

        SMTP_HOST: c.smtp || "",
        SMTP_USER: c.usr_smtp || "",
        SMTP_PASS: c.pass_smtp || "",
        SMTP_PORT: c.puerto_smtp || "",
        EMAIL_SALIENTE: c.email_saliente || "",
        EMAIL_ENVIO: c.email_envio || "",

        // opcionales si los agregás en el sheet:
        // HEADLESS: (c.headless || "false"),
        // CHROME_PATH: (c.chrome_path || "")
      }
    };
  })
};
