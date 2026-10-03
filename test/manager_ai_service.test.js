'use strict';

const assert = require('assert');
const fs = require('fs');
const { parseDocumentIntent, parseOrderQueryIntent, formatManagerOrders, parseCandidateChoice, selectDocuments, pdfPageCount, handleManagerDocumentRequest, pendingDocumentRequests } = require('../manager_ai_service');

assert.deepStrictEqual(parseDocumentIntent('Me mandás la última factura?'), {
  kind: 'sale', pointOfSale: '', number: '', latest: true
});
assert.deepStrictEqual(parseDocumentIntent('Necesito el recibo 0005-12345'), {
  kind: 'receipt', pointOfSale: '0005', number: '12345', latest: false
});
assert.deepStrictEqual(parseDocumentIntent('Me pasás el comprobante'), {
  kind: 'sale', pointOfSale: '', number: '', latest: true
});
assert.deepStrictEqual(parseDocumentIntent('Quiero consultar mi saldo'), {
  kind: 'statement', pointOfSale: '', number: '', latest: false
});
assert.deepStrictEqual(parseDocumentIntent('Resumen de cuenta'), {
  kind: 'statement', pointOfSale: '', number: '', latest: false
});
assert.strictEqual(parseDocumentIntent('Hola, buen día'), null);
assert.deepStrictEqual(parseOrderQueryIntent('¿A qué hora llega mi pedido?'), { detail: false, delivery: true, history: false, latestOnly: true });
assert.strictEqual(parseOrderQueryIntent('Quiero hacer un pedido'), null);
assert.strictEqual(parseOrderQueryIntent('¿Cuál es la dirección del supermercado?'), null);
assert.strictEqual(pdfPageCount(Buffer.from('%PDF /Type /Page /Type /Pages /Type /Page', 'latin1')), 2);
assert.strictEqual(pdfPageCount(Buffer.from('%PDF << /Type /Pages /Kids [1 0 R 2 0 R] /Count 2 >>', 'latin1')), 2);
assert.match(formatManagerOrders({ orders: [{ ptodeventa: '0001', numero: '25', fecha: '28/09/2026', total: 100, entrega: { direccion: 'Mitre 1' }, productos: [] }] }, { latestOnly: true, delivery: true, detail: false }), /Dirección: Mitre 1/);
const choiceCandidates = [{ codigo: '10', razonSocial: 'CENTENARIO' }, { codigo: '20', razonSocial: 'Ale Soljan' }];
assert.strictEqual(parseCandidateChoice('Del 2', choiceCandidates), 1);
assert.strictEqual(parseCandidateChoice('el segundo', choiceCandidates), 1);
assert.strictEqual(parseCandidateChoice('el de Alejandro Soljan', choiceCandidates), 1);

for (const script of ['lookup_client.ps1', 'query_recent_orders.ps1', 'generate_document.ps1']) {
  const source = fs.readFileSync(require('path').join(__dirname, '..', 'manager-ai', script), 'utf8');
  assert.match(source, /\$builder\['DBN'\]\s*=\s*\$settings\.DatabaseName/);
  assert.match(source, /\$builder\['LINKS'\]\s*=\s*\$settings\.CommLinks/);
  assert.match(source, /if \(\$settings\.DatabaseName\)/);
}
const lookupScriptSource = fs.readFileSync(require('path').join(__dirname, '..', 'manager-ai', 'lookup_client.ps1'), 'utf8');
assert.match(lookupScriptSource, /WHERE REPLACE\(REPLACE\(REPLACE\(REPLACE\(TRIM\(tel_celular\)/);
assert.match(lookupScriptSource, /IN \(\$phoneLiterals\)/);
assert.strictEqual((lookupScriptSource.match(/\$ordered = @\(Select-ClientMatches/g) || []).length, 2);

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
        if (lookupCount === 1) return JSON.stringify({ found: true, ambiguous: true, matches: 2, candidates: [{ codigo: '506', razonSocial: 'Alejandro Soljan' }], client: {} });
        assert.deepStrictEqual(args.slice(-2), ['-ClientQuery', '506']);
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
  let generateCount = 0;
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
      generateCount += 1;
      const outputIndex = args.indexOf('-Output');
      fs.writeFileSync(args[outputIndex + 1], Buffer.from(generateCount === 1
        ? '%PDF /Type /Page /Type /Page'
        : '%PDF /Type /Page', 'latin1'));
      return '';
    }
  });
  assert.strictEqual(result.reason, 'document_sent');
  assert.ok(generateArgs);
  assert.strictEqual(generateArgs[generateArgs.indexOf('-Kind') + 1], 'statement');
  assert.strictEqual(generateArgs[generateArgs.indexOf('-ClientCode') + 1], '000123');
  assert.ok(generateArgs.includes('-FromDate'));
  assert.ok(generateArgs.includes('-ToDate'));
  assert.strictEqual(generateCount, 2);
  assert.match(sentTexts[0], /Soy Asisto, el asistente de Supermercado Digital/);
  assert.match(sentTexts[0], /resumen de cuenta corriente/);
  assert.match(sentTexts[0], /Período:/);
  assert.strictEqual(sentDocuments[0].filename, 'Resumen_Cuenta_000123.pdf');
}

async function testDocumentSelectionContinuation() {
  pendingDocumentRequests.clear();
  const sentTexts = [];
  const sentDocuments = [];
  const base = {
    tenantId: 'SDG', phone: '5493462000002',
    config: { manager_ai_enabled: true, manager_document_send_enabled: true, manager_folder: 'C:\\Manager\\Exe', dsn: 'msm_manager', manager_ai_bridge_folder: require('path').join(__dirname, '..', 'manager-ai') },
    sendText: async text => sentTexts.push(text), sendDocument: async doc => sentDocuments.push(doc),
    execPowerShell: async (script, args) => {
      if (/lookup_client\.ps1$/i.test(script)) return JSON.stringify({ found: true, ambiguous: false, client: { finanzas: { facturas: [
        { ptodeventa: '0001', nrotransaccion: '00012944', fecha: '/Date(1789092465122)/', importe: 56906, transaccion: 'PD', tipocomprobante: 'B' },
        { ptodeventa: '0001', nrotransaccion: '00012868', fecha: '/Date(1783645134207)/', importe: 55605, transaccion: 'PD', tipocomprobante: 'B' }
      ] } } });
      const outputIndex = args.indexOf('-Output'); fs.writeFileSync(args[outputIndex + 1], Buffer.from('pdf')); return '';
    }
  };
  const first = await handleManagerDocumentRequest({ ...base, text: 'Me pasás las facturas' });
  assert.strictEqual(first.reason, 'document_selection_required');
  assert.doesNotMatch(sentTexts[0], /\/Date\(/);
  const second = await handleManagerDocumentRequest({ ...base, text: 'La 12944' });
  assert.strictEqual(second.reason, 'document_sent');
  assert.strictEqual(sentDocuments[0].filename, 'Factura_0001-00012944.pdf');
}

async function testMissingPhoneKeepsDocumentRequest() {
  pendingDocumentRequests.clear();
  const sentTexts = [];
  const sentDocuments = [];
  let lookupCount = 0;
  const base = {
    tenantId: 'SDG', phone: '5493462688075',
    config: { manager_ai_enabled: true, manager_document_send_enabled: true, manager_folder: 'C:\\Manager\\Exe', dsn: 'msm_manager', manager_ai_bridge_folder: require('path').join(__dirname, '..', 'manager-ai') },
    sendText: async text => sentTexts.push(text), sendDocument: async doc => sentDocuments.push(doc),
    execPowerShell: async (script, args) => {
      if (/lookup_client\.ps1$/i.test(script)) {
        lookupCount += 1;
        if (lookupCount === 1) return JSON.stringify({ found: false, matches: 0 });
        assert.deepStrictEqual(args.slice(-2), ['-ClientQuery', 'Norali brutto']);
        return JSON.stringify({ found: true, ambiguous: false, matches: 1, client: { finanzas: { facturas: [{ ptodeventa: '0001', nrotransaccion: '12944', transaccion: 'PD', tipocomprobante: 'B' }] } } });
      }
      fs.writeFileSync(args[args.indexOf('-Output') + 1], Buffer.from('pdf'));
      return '';
    }
  };

  const first = await handleManagerDocumentRequest({ ...base, text: 'Hola, me pasás la última factura o el alias?' });
  assert.strictEqual(first.reason, 'client_not_found');
  assert.strictEqual(pendingDocumentRequests.size, 1);

  const second = await handleManagerDocumentRequest({ ...base, text: 'Norali brutto' });
  assert.strictEqual(second.reason, 'document_sent');
  assert.strictEqual(sentDocuments.length, 1);
  assert.strictEqual(sentDocuments[0].filename, 'Factura_0001-12944.pdf');
  assert.strictEqual(pendingDocumentRequests.size, 0);
}

async function testBehaviorClassifiesFreeLanguage() {
  pendingDocumentRequests.clear();
  const sentDocuments = [];
  const result = await handleManagerDocumentRequest({
    tenantId: 'SDG', phone: '5493462000009', text: '¿Me alcanzás lo último que me facturaron?',
    config: { manager_ai_enabled: true, manager_document_send_enabled: true, manager_folder: 'C:\\Manager\\Exe', dsn: 'msm_manager', manager_ai_bridge_folder: require('path').join(__dirname, '..', 'manager-ai') },
    classifyIntent: async () => ({ action: 'document', documentKind: 'sale', latest: true }),
    sendText: async () => {}, sendDocument: async doc => sentDocuments.push(doc),
    execPowerShell: async (script, args) => {
      if (/lookup_client\.ps1$/i.test(script)) {
        assert.ok(args.includes('-AllDocumentDates'), 'Latest invoice must not be restricted to statement dates');
        return JSON.stringify({ found: true, ambiguous: false, client: { finanzas: { facturas: [{ ptodeventa: '0001', nrotransaccion: '00012944', transaccion: 'PD', tipocomprobante: 'B' }] } } });
      }
      fs.writeFileSync(args[args.indexOf('-Output') + 1], Buffer.from('pdf'));
      return '';
    }
  });
  assert.strictEqual(result.reason, 'document_sent');
  assert.strictEqual(sentDocuments[0].filename, 'Factura_0001-00012944.pdf');
}

async function testRequestedStatementPeriod() {
  pendingDocumentRequests.clear();
  let lookupArgs;
  let generateArgs;
  const sentTexts = [];
  const result = await handleManagerDocumentRequest({
    tenantId: 'SDG', phone: '5493462000010', text: 'Mandame solamente los últimos dos meses',
    now: new Date('2026-09-28T12:00:00-03:00').getTime(),
    config: { manager_ai_enabled: true, manager_document_send_enabled: true, manager_folder: 'C:\\Manager\\Exe', dsn: 'msm_manager', manager_ai_bridge_folder: require('path').join(__dirname, '..', 'manager-ai') },
    classifyIntent: async () => ({ action: 'document', documentKind: 'statement', latest: true, periodMode: 'relative_months', relativeMonths: 2 }),
    sendText: async text => sentTexts.push(text), sendDocument: async () => {},
    execPowerShell: async (script, args) => {
      if (/lookup_client\.ps1$/i.test(script)) {
        lookupArgs = [...args];
        return JSON.stringify({ found: true, ambiguous: false, client: { codigo: '000123' } });
      }
      generateArgs = [...args];
      fs.writeFileSync(args[args.indexOf('-Output') + 1], Buffer.from('%PDF << /Type /Pages /Count 1 >>', 'latin1'));
      return '';
    }
  });
  assert.strictEqual(result.reason, 'document_sent');
  assert.strictEqual(lookupArgs[lookupArgs.indexOf('-FromDate') + 1], '2026-07-28');
  assert.strictEqual(lookupArgs[lookupArgs.indexOf('-ToDate') + 1], '2026-09-28');
  assert.strictEqual(generateArgs[generateArgs.indexOf('-FromDate') + 1], '2026-07-28');
  assert.match(sentTexts[0], /28\/07\/2026 al 28\/09\/2026/);
}

async function testBehaviorControlsToolsAndPause() {
  const base = {tenantId:'TEST',phone:'1234567890',config:{manager_ai_enabled:true,manager_order_query_enabled:true,dsn:'test'},
    execPowerShell:async()=>{throw Error('must not query ODBC');},sendText:async()=>{throw Error('must not send');}};
  assert.equal((await handleManagerDocumentRequest({...base,text:'Agregame cuatro yogures al pedido',classifyIntent:async()=>({action:'none'})})).handled,false);
  assert.equal((await handleManagerDocumentRequest({...base,text:'Qué tiene mi pedido',classifyIntent:async()=>({action:'paused'})})).reason,'operator_pause');
  let receivedResult=false;
  const sent=[];
  const result=await handleManagerDocumentRequest({...base,text:'A qué hora llega',execPowerShell:async()=>JSON.stringify({orders:[{entrega:{horario_desde:'14:00'}}]}),
    classifyIntent:async(text,context)=>{if(context?.toolResult){receivedResult=true;return {action:'reply',replyText:'Tu entrega comienza a las 14:00.'};}return {action:'orders',delivery:true};},
    sendText:async text=>sent.push(text)});
  assert.equal(result.reason,'orders_queried');assert.ok(receivedResult);assert.match(sent[0],/Tu entrega comienza/);assert.doesNotMatch(sent[0],/Productos:/);
}
async function testInvoiceAndAdditionalRequestStayConversational() {
  pendingDocumentRequests.clear();
  const texts = [], documents = [], aiRequests = [];
  let lookups = 0;
  const request = 'Y pasame la última factura y el alias para transferir';
  const options = {
    tenantId: 'SDG', phone: '5493462674128',
    config: { manager_ai_enabled: true, manager_document_send_enabled: true, manager_folder: 'C:\\Manager\\Exe', dsn: 'msm_manager' },
    classifyIntent: async (text, context) => {
      if (!context?.toolResult) return { action: 'document', documentKind: 'sale', latest: true };
      aiRequests.push({text, result: context.toolResult});
      return { action: 'reply', replyText: context.toolResult.status === 'document_ready_to_send'
        ? 'Te envío la factura. Alias: configurado en el comportamiento.' : 'Elegí un cliente. Alias: configurado en el comportamiento.' };
    },
    execPowerShell: async (script, args) => {
      if (/lookup_client\.ps1$/.test(script)) {
        if (++lookups === 1) return JSON.stringify({found:true,ambiguous:true,candidates:[{codigo:'2935'},{codigo:'506'}]});
        assert.equal(args[args.indexOf('-ClientQuery')+1], '506');
        return JSON.stringify({found:true,client:{codigo:'506',finanzas:{facturas:[{ptodeventa:'0001',nrotransaccion:'00012944'}]}}});
      }
      fs.writeFileSync(args[args.indexOf('-Output')+1], Buffer.from('%PDF test'));
      return '';
    },
    sendText: async text => texts.push(text),
    sendDocument: async doc => { assert.match(texts.at(-1), /Alias:/); documents.push(doc); }
  };
  await handleManagerDocumentRequest({...options,text:request});
  const result = await handleManagerDocumentRequest({...options,text:'Del 2'});
  assert.equal(result.reason,'document_sent');
  assert.equal(documents[0].filename,'Factura_0001-00012944.pdf');
  assert.ok(aiRequests.every(item=>item.text===request));
  assert.equal(aiRequests.at(-1).result.status,'document_ready_to_send');
  assert.ok(!lookupScriptSource.includes("LIKE '%ale%'"));
  assert.match(lookupScriptSource, /\$phoneContains/);
  require('../manager_ai_service').managerConversationActivity.clear();
}

testInvoiceAndAdditionalRequestStayConversational()
  .then(testBehaviorControlsToolsAndPause)
  .then(testAmbiguousClientContinuation)
  .then(testConfiguredGreeting)
  .then(testNumberedClientSelection)
  .then(testMissingPhoneKeepsDocumentRequest)
  .then(testAccountStatement)
  .then(testDocumentSelectionContinuation)
  .then(testBehaviorClassifiesFreeLanguage)
  .then(testRequestedStatementPeriod)
  .then(() => console.log('manager_ai_service tests: ok'))
  .catch(error => { console.error(error); process.exitCode = 1; });
