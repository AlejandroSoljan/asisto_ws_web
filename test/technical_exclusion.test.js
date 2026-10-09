const { test } = require('node:test');
const assert = require('node:assert/strict');
const { effectiveDocument } = require('../technical_exclusion');
test('legacy lookup exclusion is not a cancellation or consent', () => {
  const old = { estado: 'cancelado', exclusionPermanente: true, exclusionMotivo: 'numero_no_registrado', motivoCancelacion: 'numero_no_registrado', respuestaCancelacion: '' };
  const result = effectiveDocument(old);
  assert.equal(result.estado, 'pendiente');
  assert.equal(result.exclusionPermanente, undefined);
  assert.equal(old.exclusionPermanente, true);
});
test('preserves customer opt-out and unknown exclusion', () => {
  for (const reason of ['cancelar_cliente', 'baja', 'manual']) {
    const doc = { exclusionPermanente: true, exclusionMotivo: reason };
    assert.equal(effectiveDocument(doc), doc);
  }
  const doc = { exclusionMotivo: 'numero_no_registrado', motivoCancelacion: 'numero_no_registrado', respuestaCancelacion: 'CANCELAR' };
  assert.equal(effectiveDocument(doc), doc);
});
