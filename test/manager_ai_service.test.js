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
assert.deepStrictEqual(parseDocumentIntent('Quiero consultar mi saldo'), {
  kind: 'statement', pointOfSale: '', number: '', latest: false
});
assert.deepStrictEqual(parseDocumentIntent('Resumen de cuenta'), {
  kind: 'statement', pointOfSale: '', number: '', latest: false
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
        if (lookupCount === 1) return JSON.stringify({ found: true, ambiguous: true, matches: 2, candidates: [{ razonSocial: 'Alejandro Soljan' }], client: {} });
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
  assert.strictEqual(sentTexts.length, 2);
  assert.match(sentTexts[0], /Soy Asisto, el asistente de Supermercado Digital/);
  assert.match(sentTexts[1], /Te envío la factura solicitada/);
  assert.strictEqual(pendingDocumentRequests.size, 0);
}

async function testNumberedClientSelection() {
  pendingDocumentRequests.clear();
  let lookupCount = 0;
  const base = {
    tenantId: 'SDG', phone: '5493462674128',
    config: { manager_ai_enabled: true, manager_document_send_enabled: true, manager_folder: 'C:\\Manager\\Exe', dsn: 'msm_manager', manager_ai_bridge_folder: require('path').join(__dirname, '..', 'manager-ai') },
    sendText: async () => {}, sendDocument: async () => {},
    execPowerShell: async (script, args) => {
      if (/lookup_client\.ps1$/i.test(script)) {
        lookupCount += 1;
        if (lookupCount === 1) return JSON.stringify({ found: true, ambiguous: true, matches: 2, candidates: [{ razonSocial: 'Alejandro Soljan' }], client: {} });
        assert.deepStrictEqual(args.slice(-2), ['-ClientQuery', 'Alejandro Soljan']);
        return JSON.stringify({ found: true, ambiguous: false, matches: 1, client: { finanzas: { facturas: [] } } });
      }
      return '';
    }
  };
  await handleManagerDocumentRequest({ ...base, text: 'Pasame la última factura' });
  const result = await handleManagerDocumentRequest({ ...base, text: '1' });
  assert.strictEqual(result.reason, 'document_not_found');
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

async function testAccountStatement() {
  pendingDocumentRequests.clear();
  const sentTexts = [];
  const sentDocuments = [];
  let generateArgs;
  const result = await handleManagerDocumentRequest({
    tenantId: 'SDG', phone: '5493462000001', text: 'Pasame un resumen de cuenta',
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
        return JSON.stringify({ found: true, ambiguous: false, matches: 1, client: { codigo: '000123', razonSocial: 'Cliente Prueba' } });
      }
      generateArgs = args;
      const outputIndex = args.indexOf('-Output');
      fs.writeFileSync(args[outputIndex + 1], Buffer.from('pdf'));
      return '';
    }
  });
  assert.strictEqual(result.reason, 'document_sent');
  assert.ok(generateArgs);
  assert.strictEqual(generateArgs[generateArgs.indexOf('-Kind') + 1], 'statement');
  assert.strictEqual(generateArgs[generateArgs.indexOf('-ClientCode') + 1], '000123');
  assert.ok(generateArgs.includes('-FromDate'));
  assert.ok(generateArgs.includes('-ToDate'));
  assert.match(sentTexts[0], /Soy Asisto, el asistente de Supermercado Digital/);
  assert.match(sentTexts[0], /resumen de cuenta corriente/);
  assert.strictEqual(sentDocuments[0].filename, 'Resumen_Cuenta_000123.pdf');
}

testAmbiguousClientContinuation()
  .then(testConfiguredGreeting)
  .then(testNumberedClientSelection)
  .then(testAccountStatement)
  .then(() => console.log('manager_ai_service tests: ok'))
  .catch(error => { console.error(error); process.exitCode = 1; });
