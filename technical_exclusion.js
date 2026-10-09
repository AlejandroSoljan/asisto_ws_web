'use strict';
// A legacy negative lookup is not a customer's opt-out. Do not infer consent.
function effectiveDocument(doc) {
  if (!doc || doc.exclusionMotivo !== 'numero_no_registrado' ||
      doc.motivoCancelacion !== 'numero_no_registrado' ||
      String(doc.respuestaCancelacion || '').trim()) return doc;
  const copy = { ...doc, estado: 'pendiente' };
  for (const key of ['exclusionPermanente', 'exclusionMotivo', 'exclusionAt', 'motivoCancelacion', 'canceladoAt']) delete copy[key];
  return copy;
}
module.exports = { effectiveDocument };
