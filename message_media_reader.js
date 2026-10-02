'use strict';
const MAX_BYTES = 5 * 1024 * 1024;
// Read only. Never send, mark seen, transcribe or fetch a caller-provided URL.
async function readMessageMedia(payload, client, normalizeContact) {
  const unavailable = () => JSON.stringify({ status: 'unavailable' });
  if (!payload?.messageId || String(payload.messageId).length > 500 || !client?.getMessageById) return unavailable();
  let timer;
  try {
    const work = async () => {
      const msg = await client.getMessageById(String(payload.messageId));
      if (!msg || !['ptt', 'audio', 'image'].includes(msg.type) || msg._data?.isViewOnce ||
          msg.__baileysRaw?.message?.viewOnceMessage || msg.__baileysRaw?.message?.viewOnceMessageV2 ||
          typeof msg.downloadMedia !== 'function') return unavailable();
      if ((msg.fromMe === true) !== (payload.direction === 'out')) return unavailable();
      const contact = await normalizeContact(msg.fromMe ? msg.to : msg.from);
      if (!contact || String(contact) !== String(payload.contact)) return unavailable();
      const size = Number(msg._data?.size || msg._data?.fileSize || 0);
      if (size > MAX_BYTES) return unavailable();
      const media = await msg.downloadMedia();
      if (!media?.data || media.data.length > Math.ceil(MAX_BYTES / 3) * 4) return unavailable();
      const mime = String(media.mimetype || '').split(';')[0].toLowerCase();
      if (!['image/jpeg','image/png','image/webp','image/gif','audio/ogg','audio/mpeg','audio/mp4','audio/wav'].includes(mime)) return unavailable();
      return JSON.stringify({ status: 'ready', mime, data: media.data });
    };
    return await Promise.race([work(), new Promise(resolve => { timer = setTimeout(() => resolve(unavailable()), 20000); })]);
  } catch { return unavailable(); } finally { clearTimeout(timer); }
}
module.exports = { readMessageMedia };
