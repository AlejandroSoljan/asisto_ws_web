const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readMessageMedia } = require('../message_media_reader');
test('audio sólo del contacto/dirección esperados; no vista única', async () => {
  let downloads = 0;
  const msg = { type: 'ptt', fromMe: true, to: '549111', downloadMedia: async () => { downloads++; return { mimetype: 'audio/ogg; codecs=opus', data: Buffer.from('OggS1234').toString('base64') }; } };
  const client = { getMessageById: async () => msg }, payload = { messageId: 'abc', contact: '549111', direction: 'out' };
  assert.equal(JSON.parse(await readMessageMedia(payload, client, async s => s)).status, 'ready');
  assert.equal(JSON.parse(await readMessageMedia({ ...payload, contact: 'other' }, client, async s => s)).status, 'unavailable');
  assert.equal(JSON.parse(await readMessageMedia({ ...payload, direction: 'in' }, client, async s => s)).status, 'unavailable');
  msg._data = { isViewOnce: true }; assert.equal(JSON.parse(await readMessageMedia(payload, client, async s => s)).status, 'unavailable');
  assert.equal(downloads, 1);
});
test('archivos perdidos o MIME activo no se entregan', async () => {
  const payload = { messageId: 'x', direction: 'out', contact: 'c' };
  assert.equal(JSON.parse(await readMessageMedia(payload, { getMessageById: async () => null }, async s => s)).status, 'unavailable');
  const msg = { type: 'image', fromMe: true, to: 'c', downloadMedia: async () => ({ mimetype: 'image/svg+xml', data: 'AAAA' }) };
  assert.equal(JSON.parse(await readMessageMedia(payload, { getMessageById: async () => msg }, async s => s)).status, 'unavailable');
});
