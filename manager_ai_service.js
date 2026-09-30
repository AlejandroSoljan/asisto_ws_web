'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const zlib = require('zlib');
const { spawn } = require('child_process');

const pendingDocumentRequests = new Map();
const managerConversationActivity = new Map();
const PENDING_DOCUMENT_TTL_MS = 10 * 60 * 1000;

function bool(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'boolean') return value;
  return ['1', 'true', 'yes', 'si', 'sí', 'on'].includes(String(value).trim().toLowerCase());
}

function parseDocumentIntent(text) {
  const raw = String(text || '').trim();
  const normalized = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const asksToSend = /\b(mand(?:a|as|ame|anos|eme)|envi(?:a|as|ame|anos|eme)|necesito|quiero|comparti(?:me|nos)|pas(?:a|as|ame))\b/.test(normalized);
  const kind = /\b(saldo|saldos|resumen(?:es)? de cuenta|cuenta corriente|estado de cuenta)\b/.test(normalized)
    ? 'statement'
    : /\b(recibo|recibos|pago|pagos)\b/.test(normalized)
    ? 'receipt'
    : /\b(factura|facturas|comprobante|comprobantes)\b/.test(normalized)
      ? 'sale'
      : '';
  if (!kind) return null;
  const numberMatch = normalized.match(/\b(?:n(?:ro|umero)?\.?\s*)?(\d{1,5})\s*[-/]\s*(\d{1,10})\b/i);
  const plainNumber = !numberMatch ? normalized.match(/\b(?:n(?:ro|umero)?\.?\s*)(\d{3,10})\b/i) : null;
  const number = numberMatch ? numberMatch[2] : (plainNumber ? plainNumber[1] : '');
  if (!asksToSend && kind !== 'statement' && !number) return null;
  return {
    kind,
    pointOfSale: numberMatch ? numberMatch[1] : '',
    number,
    latest: kind !== 'statement' && ((!number && !/\b(facturas|recibos|comprobantes)\b/.test(normalized)) || /\b(ultima|ultimo|mas reciente|reciente)\b/.test(normalized))
  };
}

function isStandaloneGreeting(text) {
  const normalized = String(text || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9\s]/g, ' ').trim();
  return /^(hola|buen dia|buenas tardes|buenas noches|buenas)$/.test(normalized);
}

function parseOrderQueryIntent(text) {
  const normalized = String(text || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  if (/\b(hacer|armar|realizar|cargar|crear)\b.{0,20}\bpedido\b/.test(normalized)) return null;
  const order = /\b(mi|mis|el|los|ultimo|ultimos|estado|detalle|datos?)?\s*(pedido|pedidos|compra|compras|entrega|entregas)\b/.test(normalized);
  const detail = /\b(producto|productos|detalle|contenia|compre|cantidad|cantidades)\b/.test(normalized);
  const delivery = /\b(horario|hora|cuando|entrega|direccion|domicilio|llega|llegan|estado)\b/.test(normalized);
  const history = /\b(ultimo|ultimos|historial|anteriores|pedidos|compras)\b/.test(normalized);
  if (!order) return null;
  return { detail, delivery, history, latestOnly: !history || /\b(ultimo pedido|pedido actual|mi pedido)\b/.test(normalized) };
}

function formatManagerOrders(result, intent) {
  const orders = Array.isArray(result?.orders) ? result.orders : [];
  if (!orders.length) return 'No encontré pedidos asociados a este teléfono.';
  const selected = intent.latestOnly ? orders.slice(0, 1) : orders.slice(0, 10);
  const money = value => Number(value || 0).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const pbDate = value => { const m = String(value || '').match(/^\/Date\((\d+)\)\/$/); return m ? new Date(Number(m[1])) : null; };
  const date = value => pbDate(value)?.toLocaleDateString('es-AR') || String(value || '').slice(0, 10);
  const time = value => pbDate(value)?.toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit' }) || String(value || '').slice(0, 5);
  return selected.map((order, index) => {
    const lines = [`${selected.length > 1 ? `${index + 1}. ` : ''}Pedido ${order.ptodeventa || ''}-${order.numero || ''} · ${date(order.fecha)} · $ ${money(order.total)}`];
    if (intent.delivery) {
      const delivery = order.entrega || {};
      if (delivery.estado) lines.push(`Estado: ${{ P: 'Pendiente', F: 'Finalizado', C: 'Cancelado' }[delivery.estado] || delivery.estado}`);
      if (delivery.direccion) lines.push(`Dirección: ${delivery.direccion}`);
      if (delivery.horario_fecha || delivery.horario_desde || delivery.horario_hasta) lines.push(`Entrega: ${date(delivery.horario_fecha)} ${time(delivery.horario_desde)}-${time(delivery.horario_hasta)}`.trim());
      if (delivery.forma_pago) lines.push(`Forma de pago: ${delivery.forma_pago}`);
      if (delivery.observaciones) lines.push(`Observaciones: ${delivery.observaciones}`);
    }
    if (intent.detail) {
      const products = Array.isArray(order.productos) ? order.productos : [];
      lines.push('Productos:');
      for (const product of products.slice(0, 30)) lines.push(`• ${product.descripcion || product.codigo}: ${Number(product.cantidad || 0).toLocaleString('es-AR')} × $ ${money(product.precio_final)}`);
    }
    return lines.join('\n');
  }).join('\n\n');
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
  if (kind === 'statement') return String(row.codigo || row.cliente || 'cuenta');
  if (kind === 'receipt') return `${row.ptodeventa || ''}-${row.nro || ''}`;
  return `${row.ptodeventa || ''}-${row.nrotransaccion || ''}`;
}

function selectDocuments(intent, lookup) {
  if (intent.kind === 'statement') return lookup?.client ? [lookup.client] : [];
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
    const rawDate = String(row.fecha || '');
    const pbMatch = rawDate.match(/^\/Date\((\d+)\)\/$/);
    const date = pbMatch ? new Date(Number(pbMatch[1])).toLocaleDateString('es-AR') : rawDate.slice(0, 10);
    const amount = row.importe == null ? '' : ` - $ ${Number(row.importe).toLocaleString('es-AR')}`;
    return `• ${id}${date ? ` del ${date}` : ''}${amount}`;
  });
  return `Encontré varias opciones. Decime el número de ${noun} que necesitás:\n${lines.join('\n')}`;
}

function pdfPageCount(buffer) {
  if (!Buffer.isBuffer(buffer) || !buffer.length) return 0;
  const chunks = [buffer.toString('latin1')];
  const source = chunks[0];
  const streamPattern = /<<(?:.|\r|\n)*?\/FlateDecode(?:.|\r|\n)*?>>\s*stream\r?\n([\s\S]*?)\r?\nendstream/g;
  for (const match of source.matchAll(streamPattern)) {
    try { chunks.push(zlib.inflateSync(Buffer.from(match[1], 'latin1')).toString('latin1')); } catch {}
  }
  let pageObjects = 0;
  let pageTreeCount = 0;
  for (const text of chunks) {
    pageObjects += (text.match(/\/Type\s*\/Page\b/g) || []).length;
    for (const match of text.matchAll(/\/Type\s*\/Pages\b[\s\S]{0,800}?\/Count\s+(\d+)/g)) {
      pageTreeCount = Math.max(pageTreeCount, Number(match[1]) || 0);
    }
  }
  return Math.max(pageObjects, pageTreeCount);
}

function statementPeriod(intent, now, configuredDays) {
  const until = new Date(now);
  const requestedTo = /^\d{4}-\d{2}-\d{2}$/.test(String(intent.toDate || ''))
    ? new Date(`${intent.toDate}T23:59:59`) : null;
  if (requestedTo && !Number.isNaN(requestedTo.getTime()) && requestedTo < until) until.setTime(requestedTo.getTime());
  let since = new Date(until.getTime() - configuredDays * 86400000);
  if (intent.periodMode === 'relative_months') {
    const months = Math.max(1, Math.min(36, Number(intent.relativeMonths) || 1));
    since = new Date(until);
    since.setMonth(since.getMonth() - months);
  } else if (intent.periodMode === 'relative_days') {
    const days = Math.max(1, Math.min(1095, Number(intent.relativeDays) || configuredDays));
    since = new Date(until.getTime() - days * 86400000);
  } else if (intent.periodMode === 'date_range' && /^\d{4}-\d{2}-\d{2}$/.test(String(intent.fromDate || ''))) {
    const requestedFrom = new Date(`${intent.fromDate}T00:00:00`);
    if (!Number.isNaN(requestedFrom.getTime()) && requestedFrom < until) since = requestedFrom;
  }
  if (since >= until) since = new Date(until.getTime() - 86400000);
  return { since, until };
}

async function handleManagerDocumentRequest(options) {
  const cfg = options.config || {};
  const enabled = bool(cfg.manager_ai_enabled ?? cfg.manager_ia_habilitada ?? cfg.wweb_ai_manager_enabled, false);
  if (!enabled) return { handled: false, reason: 'disabled' };
  const pendingKey = `${String(options.tenantId || '').trim()}:${String(options.phone || '').replace(/\D/g, '')}`;
  const now = Number(options.now || Date.now());
  const conversationTtlMs = Math.max(1, Number(cfg.manager_conversation_inactivity_minutes || 20) || 20) * 60 * 1000;
  const previousActivity = Number(managerConversationActivity.get(pendingKey) || 0);
  const isFirstConversationMessage = !previousActivity || (now - previousActivity) >= conversationTtlMs;
  managerConversationActivity.set(pendingKey, now);
  const configuredGreeting = String(cfg.manager_ai_greeting || '¡Hola! Soy Asisto, el asistente de Supermercado Digital.').trim();
  let introPending = isFirstConversationMessage;
  const sendManagerText = async text => {
    let outgoing = String(text || '').trim();
    if (introPending) {
      introPending = false;
      if (configuredGreeting && !outgoing.includes(configuredGreeting)) outgoing = `${configuredGreeting}\n${outgoing}`.trim();
    }
    await options.sendText(outgoing);
  };
  const savedPending = pendingDocumentRequests.get(pendingKey);
  if (savedPending && now - savedPending.createdAt > PENDING_DOCUMENT_TTL_MS) {
    pendingDocumentRequests.delete(pendingKey);
  }
  const activePending = pendingDocumentRequests.get(pendingKey);
  if (!activePending && configuredGreeting && isStandaloneGreeting(options.text)) {
    await sendManagerText(configuredGreeting);
    return { handled: true, reason: 'configured_greeting' };
  }
  let classifiedIntent = null;
  if (!activePending && typeof options.classifyIntent === 'function') {
    try { classifiedIntent = await options.classifyIntent(String(options.text || '')); } catch (e) {
      console.warn('[MANAGER_AI] no se pudo clasificar por comportamiento:', e?.message || e);
      classifiedIntent = { action: 'none' };
    }
  }
  const classifiedAction = String(classifiedIntent?.action || '').trim().toLowerCase();
  const orderIntent = classifiedAction === 'orders' ? {
    detail: bool(classifiedIntent.detail, false),
    delivery: bool(classifiedIntent.delivery, false),
    history: bool(classifiedIntent.history, false),
    latestOnly: bool(classifiedIntent.latestOnly, true),
  } : (!classifiedIntent ? parseOrderQueryIntent(options.text) : null);
  if (orderIntent && bool(cfg.manager_order_query_enabled, false)) {
    const bridgeFolder = path.resolve(String(cfg.manager_ai_bridge_folder || path.join(__dirname, 'manager-ai')));
    const queryScript = path.join(bridgeFolder, 'query_recent_orders.ps1');
    const dsn = String(cfg.manager_odbc_dsn || cfg.dsn || '').trim();
    if (!dsn || !fs.existsSync(queryScript)) throw new Error('manager_order_query_configuration_incomplete');
    const runPowerShell = typeof options.execPowerShell === 'function' ? options.execPowerShell : execPowerShell;
    const raw = await runPowerShell(queryScript, ['-Phone', String(options.phone), '-DsnName', dsn, '-Limit', '10'], 30000);
    const result = JSON.parse(raw || '{}');
    await sendManagerText(formatManagerOrders(result, orderIntent));
    return { handled: true, reason: 'orders_queried', count: Number(result.count || 0) };
  }
  let intent = classifiedAction === 'document' ? {
    kind: ['sale', 'receipt', 'statement'].includes(String(classifiedIntent.documentKind || '').toLowerCase())
      ? String(classifiedIntent.documentKind).toLowerCase() : 'sale',
    pointOfSale: String(classifiedIntent.pointOfSale || '').replace(/\D/g, ''),
    number: String(classifiedIntent.number || '').replace(/\D/g, ''),
    latest: bool(classifiedIntent.latest, false),
    periodMode: ['relative_months', 'relative_days', 'date_range'].includes(String(classifiedIntent.periodMode || '').toLowerCase())
      ? String(classifiedIntent.periodMode).toLowerCase() : 'default',
    relativeMonths: Number(classifiedIntent.relativeMonths || 0),
    relativeDays: Number(classifiedIntent.relativeDays || 0),
    fromDate: String(classifiedIntent.fromDate || ''),
    toDate: String(classifiedIntent.toDate || ''),
  } : (!classifiedIntent ? parseDocumentIntent(options.text) : null);
  let clientQuery = '';
  if (!intent && activePending) {
    const pendingAnswer = String(options.text || '').trim();
    if (activePending.stage === 'document_selection') {
      const selectedNumber = pendingAnswer.match(/\b(\d{3,10})\b/)?.[1] || '';
      if (selectedNumber) intent = { ...activePending.intent, pointOfSale: '', number: selectedNumber, latest: false };
    } else {
      intent = activePending.intent;
      clientQuery = pendingAnswer;
      const optionNumber = Number(clientQuery);
      if (Number.isInteger(optionNumber) && optionNumber > 0 && Array.isArray(activePending.candidates)) {
        const selectedCandidate = activePending.candidates[optionNumber - 1];
        if (selectedCandidate) clientQuery = String(selectedCandidate.razonSocial || selectedCandidate.cuit || selectedCandidate.codigo || clientQuery);
      }
    }
  }
  if (!intent) return { handled: false, reason: 'not_document_intent' };
  if (!clientQuery) pendingDocumentRequests.delete(pendingKey);
  if (!bool(cfg.manager_document_send_enabled ?? cfg.manager_envio_documentos_habilitado, false)) {
    await sendManagerText('Entendí que necesitás un documento, pero el envío automático todavía no está habilitado.');
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
  const requestedPeriod = intent.kind === 'statement'
    ? statementPeriod(intent, now, days)
    : { until: new Date(now), since: new Date(now - days * 86400000) };
  const { since, until } = requestedPeriod;
  const ymd = value => value.toISOString().slice(0, 10);
  const runPowerShell = typeof options.execPowerShell === 'function' ? options.execPowerShell : execPowerShell;
  const lookupArgs = ['-Phone', String(options.phone), '-FromDate', ymd(since), '-ToDate', ymd(until), '-DsnName', dsn];
  if (clientQuery) lookupArgs.push('-ClientQuery', clientQuery);
  const rawLookup = await runPowerShell(lookupScript, lookupArgs, 30000);
  const lookup = JSON.parse(rawLookup || '{}');
  if (!lookup.found) {
    if (clientQuery) {
      pendingDocumentRequests.set(pendingKey, { intent, createdAt: now });
      await sendManagerText('No encontré ese cliente. Podés responder con la razón social, CUIT o documento, y conservaré tu pedido pendiente.');
      return { handled: true, reason: 'client_selection_not_found' };
    }
    pendingDocumentRequests.set(pendingKey, { stage: 'client_selection', intent, createdAt: now });
    await sendManagerText('No encontré tu teléfono asociado a un cliente de Manager. Si querés, indicame tu razón social o CUIT para que lo revise una persona.');
    return { handled: true, reason: 'client_not_found' };
  }
  if (lookup.ambiguous) {
    pendingDocumentRequests.set(pendingKey, { stage: 'client_selection', intent, candidates: lookup.candidates || [], createdAt: now });
    const optionsList = Array.isArray(lookup.candidates) ? lookup.candidates
      .map((candidate, index) => `${index + 1}. ${candidate.razonSocial || candidate.codigo}${candidate.cuit ? ` · CUIT ${candidate.cuit}` : ''}`)
      .join('\n') : '';
    const requestedDocument = intent.kind === 'receipt' ? 'recibo' : (intent.kind === 'statement' ? 'resumen de cuenta' : 'factura');
    await sendManagerText(`Encontré más de un cliente asociado a este teléfono. Elegí una opción o indicame razón social, CUIT o documento. Tu solicitud de ${requestedDocument} queda pendiente.${optionsList ? `\n${optionsList}` : ''}`);
    return { handled: true, reason: 'ambiguous_client' };
  }
  pendingDocumentRequests.delete(pendingKey);
  const matches = selectDocuments(intent, lookup);
  if (!matches.length) {
    const missingLabel = intent.kind === 'receipt' ? 'ese recibo' : (intent.kind === 'statement' ? 'movimientos de cuenta corriente' : 'esa factura');
    await sendManagerText(`No encontré ${missingLabel} en el período consultado.`);
    return { handled: true, reason: 'document_not_found' };
  }
  if (matches.length > 1) {
    pendingDocumentRequests.set(pendingKey, { stage: 'document_selection', intent, createdAt: now });
    await sendManagerText(listMessage(intent.kind, matches));
    return { handled: true, reason: 'document_selection_required', count: matches.length };
  }

  const row = matches[0];
  const id = documentId(intent.kind, row);
  const output = path.join(os.tmpdir(), `asisto-${String(options.tenantId || 'tenant')}-${intent.kind}-${id}-${Date.now()}.pdf`);
  const args = ['-Kind', intent.kind, '-DsnName', dsn, '-ManagerFolder', managerFolder, '-Output', output];
  if (intent.kind === 'statement') {
    args.push('-ClientCode', String(row.codigo || ''), '-FromDate', ymd(since), '-ToDate', ymd(until));
  } else {
    args.push('-PointOfSale', String(row.ptodeventa || ''), '-Number', String(intent.kind === 'receipt' ? row.nro : row.nrotransaccion || ''));
  }
  if (intent.kind === 'sale') args.push('-Transaction', String(row.transaccion || ''), '-VoucherType', String(row.tipocomprobante || ''));
  await runPowerShell(generateScript, args, 90000);
  let statementFrom = intent.kind === 'statement' ? ymd(since) : '';
  if (intent.kind === 'statement') {
    const configuredDays = Math.max(1, Math.ceil((until.getTime() - since.getTime()) / 86400000));
    const candidateDays = [...new Set([
      configuredDays,
      180, 120, 90, 60, 45, 30, 21, 15, 10, 7, 3, 1
    ].filter(value => value > 0 && value <= configuredDays))];
    let pages = pdfPageCount(fs.existsSync(output) ? fs.readFileSync(output) : Buffer.alloc(0));
    for (const rangeDays of candidateDays.slice(1)) {
      if (pages === 1) break;
      const adjustedSince = new Date(until.getTime() - rangeDays * 86400000);
      statementFrom = ymd(adjustedSince);
      const fromIndex = args.indexOf('-FromDate');
      if (fromIndex >= 0) args[fromIndex + 1] = statementFrom;
      await runPowerShell(generateScript, args, 90000);
      pages = pdfPageCount(fs.existsSync(output) ? fs.readFileSync(output) : Buffer.alloc(0));
    }
  }
  try {
    const noun = intent.kind === 'receipt' ? 'recibo' : (intent.kind === 'statement' ? 'resumen de cuenta corriente' : 'factura');
    const article = intent.kind === 'sale' ? 'la' : 'el';
    const statementPeriod = intent.kind === 'statement' && statementFrom
      ? ` Período: ${statementFrom.split('-').reverse().join('/')} al ${ymd(until).split('-').reverse().join('/')}.`
      : '';
    await sendManagerText(`Te envío ${article} ${noun} ${intent.kind === 'sale' ? 'solicitada' : 'solicitado'}.${statementPeriod}`);
    const data = fs.readFileSync(output).toString('base64');
    const filenamePrefix = intent.kind === 'receipt' ? 'Recibo' : (intent.kind === 'statement' ? 'Resumen_Cuenta' : 'Factura');
    await options.sendDocument({ mimetype: 'application/pdf', data, filename: `${filenamePrefix}_${id}.pdf` });
  } finally {
    try { fs.unlinkSync(output); } catch {}
  }
  return { handled: true, reason: 'document_sent', kind: intent.kind, id };
}

module.exports = { bool, parseDocumentIntent, parseOrderQueryIntent, formatManagerOrders, isStandaloneGreeting, selectDocuments, pdfPageCount, handleManagerDocumentRequest, pendingDocumentRequests, managerConversationActivity };
