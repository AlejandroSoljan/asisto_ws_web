const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'app_asisto_ws.js'), 'utf8');
function extract(startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  const end = source.indexOf(endMarker, start);
  assert.ok(start >= 0 && end > start);
  return source.slice(start, end);
}

const functions = [
  extract('function textosSolicitudConfirmacionApiMensajes(', 'function respuestaBajaApiMensajes('),
  extract('function reemplazarVariablesConfirmacionApiMensajes(', 'function esTextoSolicitudConfirmacionApiMensajes('),
].join('\n');

function context(variants, principal, genericVariants = []) {
  const state = {
    api_mensajes_confirmacion_mensajes: variants,
    api_mensajes_confirmacion_mensajes_sin_documento: genericVariants,
    api_mensajes_confirmacion_mensaje: principal,
    api_mensajes_confirmacion_siguiente_variante: 0,
    tenantId: 'NEA',
    nom_chatbot: 'Expreso Alfa',
    onlyDigits: value => String(value).replace(/\D/g, ''),
    nombreClienteDesdeDescripcionApiMensajes: value => String(value).match(/\(([^()]*)\)\s*$/)?.[1] || '',
    tipoDocumentoDesdeDescripcionApiMensajes: value => String(value).replace(/\s*\([^()]*\)\s*$/, '').trim(),
  };
  vm.runInNewContext(functions + '\nthis.choose = textoSolicitudConfirmacionApiMensajes; this.options = textosSolicitudConfirmacionApiMensajes;', state);
  return state;
}

test('la lista prevalece sobre el campo singular y rota entre solicitudes al mismo teléfono', () => {
  const state = context(['Variante A CANCELAR', 'Variante B CANCELAR', 'Variante A CANCELAR'], 'Mensaje antiguo CANCELAR');
  assert.deepEqual(Array.from(state.options()), ['Variante A CANCELAR', 'Variante B CANCELAR']);
  const sent = [state.choose('5493462000000', 'Factura N° 0001-12345 (Juan)'), state.choose('5493462000000', 'Factura N° 0001-12345 (Juan)'), state.choose('5493462000000', 'Factura N° 0001-12345 (Juan)')];
  assert.deepEqual(sent, [
    'Variante A CANCELAR\nCliente: Juan · Documento: Factura N° 0001-12345',
    'Variante B CANCELAR\nCliente: Juan · Documento: Factura N° 0001-12345',
    'Variante A CANCELAR\nCliente: Juan · Documento: Factura N° 0001-12345',
  ]);
});

test('si la variante ya dice BAJA, no agrega una instrucción contradictoria de CANCELAR', () => {
  const state = context(['Respondé OK o BAJA'], 'Ignorado');
  assert.equal(state.choose('5493462000000', 'Factura N° 123 (Juan)'), 'Respondé OK o BAJA\nCliente: Juan · Documento: Factura N° 123');
});

test('coloca número de factura antes de la instrucción de respuesta', () => {
  const state = context(['Hola, somos Expreso Alfa. ¿Nos autorizás? Respondé OK o BAJA.'], 'Ignorado');
  assert.equal(state.choose('5493462000000', 'Factura N° 0001-12345 (Juan)'),
    'Hola, somos Expreso Alfa. ¿Nos autorizás?\nCliente: Juan · Documento: Factura N° 0001-12345\nRespondé OK o BAJA.');
});

test('sin lista, conserva el mensaje singular para configuraciones anteriores', () => {
  const state = context([], 'Mensaje antiguo CANCELAR');
  state.nombreClienteDesdeDescripcionApiMensajes = () => '';
  state.tipoDocumentoDesdeDescripcionApiMensajes = () => '';
  assert.deepEqual(Array.from(state.options()), ['Mensaje antiguo CANCELAR']);
  assert.equal(state.choose('5493462000000', ''), 'Mensaje antiguo CANCELAR');
});

test('plantillas con factura mantienen cliente, empresa y número', () => {
  const state = context([
    '¡Hola, {cliente}! 👋 Desde *{empresa}* queremos compartirte tu *{documento}*. Respondé *OK* para recibirlo o *CANCELAR* para no recibir más mensajes.',
    '¡Hola, {cliente}! 👋 Tenemos listo tu *{documento}* en *{empresa}*. Respondé *OK* o *CANCELAR*.'
  ], 'Ignorado');
  const first = state.choose('5493462000000', 'fc a 0005-00051822 (MOLINOS A. LEDESMA S.R.L.)');
  const second = state.choose('5493462000000', 'fc a 0005-00051822 (MOLINOS A. LEDESMA S.R.L.)');
  assert.match(first, /MOLINOS A\. LEDESMA S\.R\.L\./);
  assert.match(first, /\*fc a 0005-00051822\*/);
  assert.match(first, /Expreso Alfa/);
  assert.notEqual(first, second);
});

test('sin documento usa variantes generales y no muestra tu ** ni Mensaje informativo', () => {
  const state = context(['Tu *{documento}*'], 'Ignorado', [
    '¡Hola, {cliente}! 👋 Desde *{empresa}* tenemos información para compartirte. Respondé *OK* o *CANCELAR*.',
    '¡Hola, {cliente}! 👋 ¿Podemos enviarte información de *{empresa}*? Respondé *OK* o *CANCELAR*.'
  ]);
  const first = state.choose('5493462000000', 'Mensaje informativo (Juan)');
  const second = state.choose('5493462000000', 'Mensaje informativo (Juan)');
  assert.match(first, /Hola, Juan/);
  assert.match(first, /información para compartirte/);
  assert.match(second, /¿Podemos enviarte información/);
  assert.doesNotMatch(first + second, /tu \*\*|Mensaje informativo/);
});
