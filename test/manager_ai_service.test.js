'use strict';

const assert = require('assert');
const fs = require('fs');
const { parseDocumentIntent, selectDocuments, handleManagerDocumentRequest, pendingDocumentRequests } = require('../manager_ai_service');

assert.deepStrictEqual(parseDocumentIntent('Me mandás la última factura?'), {
  kind: 'sale', pointOfSale: '', number: '', latest: true
});
assert.deepStrictEqual(parseDocumentIntent('Necesito el recibo 0005-12345'), {
  kind: 'receipt', pointOfSale: '0005', number: '12345', latest: false
});
assert.strictEqual(parseDocumentIntent('Hola, buen día'), null);

const lookup = { client: { finanzas: { facturas: [
  { ptodeventa: '0005', nrotransaccion: '00000123' },
  { ptodeventa: '0005', nrotransaccion: '00000456' }
] } } };
assert.strictEqual(selectDocuments(parseDocumentIntent('Mandame la factura 5-123'), lookup).length, 1);
assert.strictEqual(selectDocuments(parseDocumentIntent('Mandame la factura 5-999'), lookup).length, 0);

async function testAmbiguousClientContinuation() {
  pendingDocumentRequests.clear();
  const sentTexts = [];
  const sentDocuments = [];
  let lookupCount = 0;
  const base = {
    tenantId: 'SDG',
    phone: '5493462674128',
    config: {
      manager_ai_enabled: true,
      manager_document_send_enabled: true,
      manager_folder: 'C:\\Manager\\Exe',
      dsn: 'msm_manager',
      manager_ai_bridge_folder: require('path').join(__dirname, '..', 'manager-ai')
    },
    sendText: async text => sentTexts.push(text),
    sendDocument: async doc => sentDocuments.push(doc),
    execPowerShell: async (script, args) => {
      if (/lookup_client\.ps1$/i.test(script)) {
        lookupCount += 1;
        if (lookupCount === 1) return JSON.stringify({ found: true, ambiguous: true, matches: 2, client: {} });
        assert.deepStrictEqual(args.slice(-2), ['-ClientQuery', 'Alejandro Soljan']);
        return JSON.stringify({
          found: true,
          ambiguous: false,
          matches: 1,
          client: { finanzas: { facturas: [{ ptodeventa: '0005', nrotransaccion: '123', fecha: '2026-09-27', transaccion: 'T1', tipocomprobante: 'FC' }] } }
        });
      }
      const outputIndex = args.indexOf('-Output');
      fs.writeFileSync(args[outputIndex + 1], Buffer.from('pdf'));
      return '';
    }
  };

  const first = await handleManagerDocumentRequest({ ...base, text: 'Me pasás la última factura' });
  assert.strictEqual(first.reason, 'ambiguous_client');
  const second = await handleManagerDocumentRequest({ ...base, text: 'Alejandro Soljan' });
  assert.strictEqual(second.reason, 'document_sent');
  assert.strictEqual(sentDocuments.length, 1);
  assert.strictEqual(sentTexts.length, 1);
  assert.strictEqual(pendingDocumentRequests.size, 0);
}

async function testConfiguredGreeting() {
  pendingDocumentRequests.clear();
  const sentTexts = [];
  const result = await handleManagerDocumentRequest({
    tenantId: 'SDG', phone: '5493462674128', text: 'Hola',
    config: { manager_ai_enabled: true, manager_ai_greeting: '¡Hola! Soy Asisto, el asistente de Supermercado Digital. ¿En qué puedo ayudarte?' },
    sendText: async text => sentTexts.push(text)
  });
  assert.strictEqual(result.reason, 'configured_greeting');
  assert.deepStrictEqual(sentTexts, ['¡Hola! Soy Asisto, el asistente de Supermercado Digital. ¿En qué puedo ayudarte?']);
}

Promise.all([testAmbiguousClientContinuation(), testConfiguredGreeting()])
  .then(() => console.log('manager_ai_service tests: ok'))
  .catch(error => { console.error(error); process.exitCode = 1; });
