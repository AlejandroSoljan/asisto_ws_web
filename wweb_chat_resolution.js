'use strict';

// Recovery is confined to getChat, BEFORE WWebJS.sendMessage is called.
// Never retry an actual send: its delivery may already have happened.
function installInPage() {
  const api = window.WWebJS;
  if (!api || typeof api.getChat !== 'function') throw new Error('wweb_chat_runtime_unavailable');
  if (api.getChat.asistoLidRecovery) return;
  const original = api.getChat;
  const recovery = async function (chatId, options) {
    try { return await original.call(api, chatId, options); }
    catch (error) {
      if (!/^\d+@c\.us$/.test(chatId) ||
          !String(error?.message || error).includes('No LID for user') ||
          typeof api.enforceLidAndPnRetrieval !== 'function') throw error;
      let mapping;
      try { mapping = await api.enforceLidAndPnRetrieval(chatId); }
      catch { throw error; }
      const lid = mapping?.lid?._serialized;
      const phone = mapping?.phone?._serialized;
      // Only use WhatsApp's explicit association with this exact phone.
      // A phone number is NOT a LID; never replace its suffix heuristically.
      if (phone !== chatId || !/^\d+@lid$/.test(lid || '')) throw error;
      return original.call(api, lid, options);
    }
  };
  recovery.asistoLidRecovery = true;
  api.getChat = recovery;
}

async function install(client) {
  if (!client?.pupPage) return;
  await client.pupPage.evaluate(installInPage);
}

async function diagnose(client, phone) {
  if (!/^\d{10,15}$/.test(phone) || !client?.pupPage) return { status: 'invalid_request' };
  await install(client);
  return client.pupPage.evaluate(async (jid) => {
    const result = { jid, status: 'unresolved' };
    try {
      const wid = window.require('WAWebWidFactory').createWid(jid);
      const query = await window.require('WAWebQueryExistsJob').queryWidExists(wid);
      result.query = { present: !!query, keys: Object.keys(query || {}),
        wid: query?.wid?._serialized || null };
    } catch (e) { result.queryError = String(e?.message || e).slice(0, 300); }
    try {
      const map = await window.WWebJS.enforceLidAndPnRetrieval(jid);
      result.phone = map?.phone?._serialized || null;
      result.lid = map?.lid?._serialized || null;
    } catch (e) { result.mappingError = String(e?.message || e).slice(0, 300); }
    try {
      const chat = await window.WWebJS.getChat(jid, { getAsModel: false });
      result.status = chat ? 'resolved' : 'missing_chat';
      result.chatId = chat?.id?._serialized || null;
    } catch (e) { result.error = String(e?.message || e).slice(0, 300); }
    return result;
  }, phone + '@c.us');
}

async function prepareRecipient(client, jid) {
  if (!client?.pupPage || !/^\d+@c\.us$/.test(jid)) return { ready: true };
  await install(client);
  try {
    const exists = await client.pupPage.evaluate(async id =>
      !!(await window.WWebJS.getChat(id, { getAsModel: false })), jid);
    return exists ? { ready: true } : { ready: false, reason: 'chat_unresolved' };
  } catch (e) {
    if (String(e?.message || e).includes('No LID for user')) {
      return { ready: false, reason: 'lid_unresolved' };
    }
    throw e; // Session/transport failures keep their circuit-breaker behavior.
  }
}

module.exports = { installInPage, install, diagnose, prepareRecipient };
