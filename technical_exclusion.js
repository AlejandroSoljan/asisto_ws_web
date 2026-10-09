'use strict';
// A legacy negative lookup is not a customer's opt-out. Do not infer consent.
function effectiveDocument(doc) {
  if (!doc || String(doc.respuestaCancelacion || '').trim()) return doc;
  const technical = doc.exclusionMotivo === 'numero_no_registrado' && doc.motivoCancelacion === 'numero_no_registrado';
  const timeout = doc.motivoCancelacion === 'sin_respuesta_timeout' && !doc.exclusionPermanente && !doc.exclusionMotivo;
  if (!technical && !timeout) return doc;
  const copy = { ...doc, estado: 'pendiente' };
  for (const key of ['exclusionPermanente', 'exclusionMotivo', 'exclusionAt', 'motivoCancelacion', 'canceladoAt']) delete copy[key];
  return copy;
}
module.exports = { effectiveDocument };
