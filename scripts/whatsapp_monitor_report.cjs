'use strict';
// Run via SSH, with cwd /opt/asisto/current and the production dotenv preload.
const { getDb, closeDb } = require('./db');
// SEND_HOURS_MODULE
(async () => {
  const db = await getDb();
  if (db.databaseName !== 'Cluster0') throw Error('Unexpected database');
  const now = new Date();
  const dayKey = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Argentina/Buenos_Aires', year: 'numeric', month: '2-digit', day: '2-digit' }).format(now);
  const locks = await db.collection('wa_locks').find({}, { projection: { tenantId: 1, numero: 1, host: 1, state: 1, lastSeenAt: 1, startedAt: 1, runtimeVersion: 1, activeWwebEngine: 1 } }).toArray();
  const configs = await db.collection('tenant_config').find({ _id: { $ne: 'default' } }, { projection: { nom_emp: 1, numero: 1, target_tag: 1, wweb_engine: 1, habilitar_consulta_mensajes: 1, api_mensajes_limite_diario: 1, api_mensajes_limite_mensajes_diario: 1, api_mensajes_limite_no_contactos: 1, api_mensajes_circuit_habilitado: 1, api_mensajes_circuit_min_muestra: 1, api_mensajes_circuit_antiguedad_ms: 1, api_mensajes_circuit_sin_respuesta_ratio: 1, api_mensajes_solicitudes_prioridades_bloqueadas: 1, consulta_mensajes_respetar_horarios: 1 } }).toArray();
  const quotas = await db.collection('wa_api_message_windows').find({ channelType: 'api_daily_quota', dayKey }, { projection: { tenantId: 1, numeroFrom: 1, used: 1, updatedAt: 1 } }).toArray();
  const metrics = await db.collection('wa_api_mensajes_confirmaciones').find({ tipoDocumento: 'metrica', solicitudDayKey: dayKey }, { projection: { tenantId: 1, contadores: 1, updatedAt: 1 } }).toArray();
  const confirmations = await db.collection('wa_api_mensajes_confirmaciones').find({ tipoDocumento: { $ne: 'metrica' } }, { projection: { tenantId: 1, numeroFrom: 1, estado: 1, exclusionPermanente: 1, solicitudDayKey: 1, pedidoAt: 1, contactoConocido: 1, motivoCancelacion: 1 } }).toArray();
  const pending = await db.collection('wa_api_mensajes_confirmaciones').aggregate([
    { $match: { tipoDocumento: { $ne: 'metrica' } } },
    { $project: { tenantId: 1, estado: 1, entries: { $objectToArray: { $ifNull: ['$pendientes', {}] } } } },
    { $unwind: '$entries' },
    { $group: { _id: { tenant: '$tenantId', state: '$estado' }, total: { $sum: 1 }, uncertainOlderThan5min: { $sum: { $cond: [{ $and: [{ $ne: [{ $ifNull: ['$entries.v.envioClaimedAt', null] }, null] }, { $lt: ['$entries.v.envioClaimedAt', new Date(+now - 300000)] }, { $eq: [{ $ifNull: ['$entries.v.envioCompletadoAt', null] }, null] }] }, 1, 0] } } } }
  ]).toArray();
  const policies = await db.collection('wa_wweb_policies').find({}, { projection: { tenantId: 1, numero: 1, disabled: 1, blocked: 1, messagesBlocked: 1, paused: 1, reason: 1, updatedAt: 1 } }).toArray();
  const outgoing = await db.collection('wa_wweb_message_log').aggregate([{ $match: { direction: 'out', at: { $gte: new Date(+now - 86400000) } } }, { $group: { _id: '$tenantId', lastAt: { $max: '$at' }, rawLogEntries: { $sum: 1 } } }]).toArray();
  const hoursDocs = await db.collection('settings').find({ _id: /^store_hours:/ }, { projection: { tenantId: 1, hours: 1, updatedAt: 1 } }).toArray();
  const since = new Date(+now - 86400000);
  const outRows = await db.collection('wa_wweb_message_log').find({ direction: 'out', at: { $gte: since } }, { projection: { tenantId: 1, at: 1, messageId: 1, asistoOrigin: 1 } }).toArray();
  const scheduleViolations = [];
  const scheduleCoverage = [];
  for (const c of configs) {
    if (c.consulta_mensajes_respetar_horarios === false) continue;
    const tenant = String(c._id);
    const doc = hoursDocs.find(d => String(d._id) === 'store_hours:' + tenant) || hoursDocs.find(d => d.tenantId === tenant);
    const validHours = doc?.hours && Object.values(doc.hours).some(r => Array.isArray(r) && r.length);
    if (!validHours) { scheduleCoverage.push({ tenantId: tenant, reason: 'schedule_unavailable' }); continue; }
    const bad = outRows.filter(r => r.tenantId === tenant && (!doc.updatedAt || +new Date(r.at) >= +new Date(doc.updatedAt)) && !sendHours.evaluate(doc.hours, new Date(r.at)).allowed);
    const unique = new Map(bad.map(r => [r.messageId || String(r._id), r]));
    if (unique.size) {
      const rows = [...unique.values()].sort((a,b) => +new Date(a.at) - +new Date(b.at));
      scheduleViolations.push({ tenantId: tenant, uniqueMessages: rows.length, firstAt: rows[0].at, lastAt: rows[rows.length-1].at, rawLogEntries: bad.length, attribution: 'outgoing_log_delivery_not_proven', hoursUpdatedAt: doc.updatedAt || null });
    }
  }
  const circuits = configs.map(c => {
    const sample = confirmations.filter(d => d.tenantId === String(c._id) && d.solicitudDayKey === dayKey && d.pedidoAt && +new Date(d.pedidoAt) <= +now - (Number(c.api_mensajes_circuit_antiguedad_ms) || 7200000));
    const unanswered = sample.filter(d => d.estado === 'pendiente' || d.motivoCancelacion === 'sin_respuesta_timeout').length;
    return { tenantId: c._id, sample: sample.length, unanswered, open: c.api_mensajes_circuit_habilitado !== false && sample.length >= (Number(c.api_mensajes_circuit_min_muestra) || 10) && unanswered / sample.length >= (Number(c.api_mensajes_circuit_sin_respuesta_ratio) || 0.8) };
  });
  const actions = await db.collection('wa_wweb_actions').find({ doneAt: { $exists: false } }, { projection: { lockId: 1, tenantId: 1, action: 1, reason: 1, requestedAt: 1 } }).toArray();
  console.log('ASISTO_MONITOR_JSON=' + JSON.stringify({ at: now, database: db.databaseName, dayKey, locks, configs, quotas, metrics, pending, policies, outgoing, circuits, actions, scheduleViolations, scheduleCoverage }));
  await closeDb();
})().catch(error => { console.error(error.message); process.exit(1); });
