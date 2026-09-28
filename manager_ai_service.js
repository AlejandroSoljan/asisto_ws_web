'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

function bool(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'boolean') return value;
  return ['1', 'true', 'yes', 'si', 'sí', 'on'].includes(String(value).trim().toLowerCase());
}

function parseDocumentIntent(text) {
  const raw = String(text || '').trim();
  const normalized = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const asksToSend = /\b(mand(?:a|as|ame|anos|eme)|envi(?:a|as|ame|anos|eme)|necesito|quiero|comparti(?:me|nos)|pas(?:a|as|ame))\b/.test(normalized);
  const kind = /\b(recibo|recibos|pago|pagos)\b/.test(normalized)
    ? 'receipt'
    : /\b(factura|facturas|comprobante|comprobantes)\b/.test(normalized)
      ? 'sale'
      : '';
  if (!asksToSend || !kind) return null;
  const numberMatch = normalized.match(/\b(?:n(?:ro|umero)?\.?\s*)?(\d{1,5})\s*[-/]\s*(\d{1,10})\b/i);
  const plainNumber = !numberMatch ? normalized.match(/\b(?:n(?:ro|umero)?\.?\s*)(\d{3,10})\b/i) : null;
  return {
    kind,
    pointOfSale: numberMatch ? numberMatch[1] : '',
    number: numberMatch ? numberMatch[2] : (plainNumber ? plainNumber[1] : ''),
    latest: /\b(ultima|ultimo|mas reciente|reciente)\b/.test(normalized)
  };
}

function execPowerShell(script, args, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const child = spawn('powershell.exe', ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', script, ...args], {
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe']
    });
    let stdout = '';
    let stderr = '';
    const timer = setTimeout(() => {
      try { child.kill(); } catch {}
      reject(new Error('manager_ai_timeout'));
    }, Math.max(5000, Number(timeoutMs) || 30000));
    child.stdout.on('data', chunk => { stdout += chunk.toString(); });
    child.stderr.on('data', chunk => { stderr += chunk.toString(); });
    child.on('error', reject);
    child.on('close', code => {
      clearTimeout(timer);
      if (code !== 0) return reject(new Error((stderr || stdout || `powershell_exit_${code}`).trim()));
      resolve(stdout.trim());
    });
  });
}

function documentId(kind, row) {
  if (kind === 'receipt') return `${row.ptodeventa || ''}-${row.nro || ''}`;
  return `${row.ptodeventa || ''}-${row.nrotransaccion || ''}`;
}

function selectDocuments(intent, lookup) {
  const finance = lookup?.client?.finanzas || {};
  const rows = intent.kind === 'receipt' ? (finance.recibos || []) : (finance.facturas || []);
  let selected = rows;
  if (intent.pointOfSale && intent.number) {
    selected = rows.filter(row => String(row.ptodeventa || '').replace(/^0+/, '') === String(intent.pointOfSale).replace(/^0+/, '') &&
      String(intent.kind === 'receipt' ? row.nro : row.nrotransaccion).replace(/^0+/, '') === String(intent.number).replace(/^0+/, ''));
  } else if (intent.number) {
    selected = rows.filter(row => String(intent.kind === 'receipt' ? row.nro : row.nrotransaccion).replace(/^0+/, '') === String(intent.number).replace(/^0+/, ''));
  } else if (intent.latest) {
    selected = rows.slice(0, 1);
  }
  return selected;
}

function listMessage(kind, rows) {
  const noun = kind === 'receipt' ? 'recibo' : 'factura';
  const lines = rows.slice(0, 8).map(row => {
    const id = documentId(kind, row);
    const date = row.fecha ? String(row.fecha).slice(0, 10) : '';
    const amount = row.importe == null ? '' : ` - $ ${Number(row.importe).toLocaleString('es-AR')}`;
    return `• ${id}${date ? ` del ${date}` : ''}${amount}`;
  });
  return `Encontré varias opciones. Decime el número de ${noun} que necesitás:\n${lines.join('\n')}`;
}

async function handleManagerDocumentRequest(options) {
  const cfg = options.config || {};
  const enabled = bool(cfg.manager_ai_enabled ?? cfg.manager_ia_habilitada ?? cfg.wweb_ai_manager_enabled, false);
  if (!enabled) return { handled: false, reason: 'disabled' };
  const intent = parseDocumentIntent(options.text);
  if (!intent) return { handled: false, reason: 'not_document_intent' };
  if (!bool(cfg.manager_document_send_enabled ?? cfg.manager_envio_documentos_habilitado, false)) {
    await options.sendText('Entendí que necesitás un documento, pero el envío automático todavía no está habilitado.');
    return { handled: true, reason: 'send_disabled' };
  }

  const bridgeFolder = path.resolve(String(cfg.manager_ai_bridge_folder || path.join(__dirname, 'manager-ai')));
  const lookupScript = path.join(bridgeFolder, 'lookup_client.ps1');
  const generateScript = path.join(bridgeFolder, 'generate_document.ps1');
  const managerFolder = String(cfg.manager_folder || cfg.carpeta_manager || '').trim();
  const dsn = String(cfg.manager_odbc_dsn || cfg.dsn || '').trim();
  if (!managerFolder || !dsn || !fs.existsSync(lookupScript) || !fs.existsSync(generateScript)) {
    throw new Error('manager_ai_configuration_incomplete');
  }

  const days = Math.max(1, Math.min(1095, Number(cfg.manager_document_lookup_days || 365) || 365));
  const until = new Date();
  const since = new Date(until.getTime() - days * 86400000);
  const ymd = value => value.toISOString().slice(0, 10);
  const rawLookup = await execPowerShell(lookupScript, ['-Phone', String(options.phone), '-FromDate', ymd(since), '-ToDate', ymd(until), '-DsnName', dsn], 30000);
  const lookup = JSON.parse(rawLookup || '{}');
  if (!lookup.found) {
    await options.sendText('No encontré tu teléfono asociado a un cliente de Manager. Si querés, indicame tu razón social o CUIT para que lo revise una persona.');
    return { handled: true, reason: 'client_not_found' };
  }
  if (lookup.ambiguous) {
    await options.sendText('Encontré más de un cliente asociado a este teléfono. Para evitar enviarte un documento incorrecto, indicame tu razón social o CUIT.');
    return { handled: true, reason: 'ambiguous_client' };
  }
  const matches = selectDocuments(intent, lookup);
  if (!matches.length) {
    await options.sendText(`No encontré ${intent.kind === 'receipt' ? 'ese recibo' : 'esa factura'} en el período consultado.`);
    return { handled: true, reason: 'document_not_found' };
  }
  if (matches.length > 1) {
    await options.sendText(listMessage(intent.kind, matches));
    return { handled: true, reason: 'document_selection_required', count: matches.length };
  }

  const row = matches[0];
  const id = documentId(intent.kind, row);
  const output = path.join(os.tmpdir(), `asisto-${String(options.tenantId || 'tenant')}-${intent.kind}-${id}-${Date.now()}.pdf`);
  const args = ['-Kind', intent.kind, '-DsnName', dsn, '-ManagerFolder', managerFolder, '-Output', output,
    '-PointOfSale', String(row.ptodeventa || ''), '-Number', String(intent.kind === 'receipt' ? row.nro : row.nrotransaccion || '')];
  if (intent.kind === 'sale') args.push('-Transaction', String(row.transaccion || ''), '-VoucherType', String(row.tipocomprobante || ''));
  await execPowerShell(generateScript, args, 90000);
  try {
    const data = fs.readFileSync(output).toString('base64');
    await options.sendDocument({ mimetype: 'application/pdf', data, filename: `${intent.kind === 'receipt' ? 'Recibo' : 'Factura'}_${id}.pdf` });
  } finally {
    try { fs.unlinkSync(output); } catch {}
  }
  return { handled: true, reason: 'document_sent', kind: intent.kind, id };
}

module.exports = { bool, parseDocumentIntent, selectDocuments, handleManagerDocumentRequest };
