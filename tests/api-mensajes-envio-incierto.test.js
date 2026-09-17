const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'app_asisto_ws.js'), 'utf8');
const start = source.indexOf('async function guardarLoteRecibidoApiMensajes(');
const end = source.indexOf('async function marcarPendienteEnviadoApiMensajes(', start);
assert.ok(start >= 0 && end > start);

const pending = { '694_832': { envioClaimedAt: new Date(), msj: 'Factura' } };
const claims = new Set();
const col = {
  async findOne() { return { pendientes: pending }; },
  async updateOne(query, update) {
    if (query['pendientes.708_845.envioClaimedAt']) {
      if (claims.has('708_845')) return { matchedCount: 0 };
      claims.add('708_845');
      return { matchedCount: 1 };
    }
    if (update.$set) Object.assign(pending, Object.fromEntries(
      Object.entries(update.$set)
        .filter(([key]) => key.startsWith('pendientes.'))
        .map(([key, value]) => [key.slice('pendientes.'.length), value])
    ));
    return { matchedCount: 1 };
  }
};
const context = {
  ensureMongo: async () => true,
  apiMensajesConfirmacionCollection: () => col,
  apiMensajesConfirmacionId: (nroTel) => `RVL:5493462239802:${nroTel}`,
  apiMensajesConfirmacionTenantId: () => 'RVL',
  apiMensajesConfirmacionNumeroFrom: () => '5493462239802',
  keyPendienteConfirmacionApiMensajes: (idDest, idRenglon) => `${idDest}_${idRenglon}`,
  onlyDigits: (value) => String(value).replace(/\D/g, ''),
  EscribirLog: () => {},
  Date,
  Map,
  Set
};
vm.runInNewContext(source.slice(start, end) + '\nthis.guardar = guardarLoteRecibidoApiMensajes; this.reservar = reservarPendienteEnvioApiMensajes;', context);

(async () => {
  const unidad = { dest: { Nro_tel: '5493462216643', Id_msj_dest: 694, Id_msj_renglon: 832 }, msg: { Msj: 'Factura' } };
  const result = await context.guardar([unidad]);
  assert.equal(result.ok, true);
  assert.equal(result.inciertos.has('694_832'), true, 'no reenviar un resultado incierto');
  assert.equal(result.asumibles.has('694_832'), false, 'no informar E sin envío comprobado');

  assert.equal(await context.reservar('5493462644722', 708, 845), true);
  assert.equal(await context.reservar('5493462644722', 708, 845), false, 'la misma unidad no obtiene dos reservas');
})().catch((error) => { console.error(error); process.exitCode = 1; });
