'use strict';

const axios = require('axios');

const TYPE_KEY = '__asistoType';

function encodeSpecial(value) {
  if (value instanceof Date) {
    return { [TYPE_KEY]: 'date', value: value.toISOString() };
  }
  if (value instanceof RegExp) {
    return { [TYPE_KEY]: 'regexp', source: value.source, flags: value.flags };
  }
  if (Buffer.isBuffer(value)) {
    return { [TYPE_KEY]: 'buffer', value: value.toString('base64') };
  }
  if (Array.isArray(value)) return value.map(encodeSpecial);
  if (value && typeof value === 'object') {
    if (value._bsontype === 'ObjectId' && typeof value.toHexString === 'function') {
      return { [TYPE_KEY]: 'objectId', value: value.toHexString() };
    }
    const out = {};
    for (const [key, item] of Object.entries(value)) out[key] = encodeSpecial(item);
    return out;
  }
  return value;
}

function decodeSpecial(value) {
  if (Array.isArray(value)) return value.map(decodeSpecial);
  if (value && typeof value === 'object') {
    if (value[TYPE_KEY] === 'date') return new Date(value.value);
    if (value[TYPE_KEY] === 'regexp') return new RegExp(value.source || '', value.flags || '');
    if (value[TYPE_KEY] === 'buffer') return Buffer.from(String(value.value || ''), 'base64');
    // En la PC no hace falta construir ObjectId. El servidor volverá a convertir
    // los strings hexadecimales cuando se usen como filtro _id.
    if (value[TYPE_KEY] === 'objectId') return String(value.value || '');
    const out = {};
    for (const [key, item] of Object.entries(value)) out[key] = decodeSpecial(item);
    return out;
  }
  return value;
}

function normalizeBaseUrl(value) {
  return String(value || '').trim().replace(/\/+$/, '');
}

function createControlApiClient(options = {}) {
  let baseUrl = normalizeBaseUrl(options.baseUrl);
  let token = String(options.token || '').trim();
  let tenantId = String(options.tenantId || '').trim().toUpperCase();
  let numero = String(options.numero || '').replace(/\D/g, '');
  let readyUntil = 0;
  let readyPromise = null;

  const timeoutMs = Math.max(3000, Number(options.timeoutMs || process.env.WWEB_CONTROL_API_TIMEOUT_MS || 15000));
  const http = axios.create({
    timeout: timeoutMs,
    maxContentLength: 20 * 1024 * 1024,
    maxBodyLength: 20 * 1024 * 1024,
    validateStatus: () => true,
  });

  function configure(next = {}) {
    if (next.baseUrl !== undefined) baseUrl = normalizeBaseUrl(next.baseUrl);
    if (next.token !== undefined) token = String(next.token || '').trim();
    if (next.tenantId !== undefined) tenantId = String(next.tenantId || '').trim().toUpperCase();
    if (next.numero !== undefined) numero = String(next.numero || '').replace(/\D/g, '');
    readyUntil = 0;
  }

  function isConfigured() {
    return !!(baseUrl && token && tenantId);
  }

  function headers() {
    return {
      'content-type': 'application/json',
      'x-api-key': token,
      'x-asisto-tenant': tenantId,
      'x-asisto-numero': numero,
    };
  }

  async function request(path, payload = {}) {
    if (!isConfigured()) throw new Error('control_api_not_configured');
    const response = await http.post(`${baseUrl}${path}`, encodeSpecial({
      tenantId,
      numero,
      ...payload,
    }), { headers: headers() });

    const body = decodeSpecial(response.data);
    if (response.status < 200 || response.status >= 300 || !body || body.ok !== true) {
      const detail = body?.detail || body?.error || `http_${response.status}`;
      const error = new Error(`control_api_${detail}`);
      error.status = response.status;
      error.responseBody = body;
      throw error;
    }
    return body;
  }

  async function ensureReady(force = false) {
    if (!isConfigured()) return false;
    if (!force && Date.now() < readyUntil) return true;
    if (readyPromise) return readyPromise;

    readyPromise = request('/ping', { host: require('os').hostname(), pid: process.pid })
      .then(() => {
        readyUntil = Date.now() + 30000;
        return true;
      })
      .catch(() => false)
      .finally(() => { readyPromise = null; });

    return readyPromise;
  }

  async function dbCall(collection, operation, args = {}) {
    const body = await request('/db', { collection, operation, args });
    return body.result;
  }

  function collection(name) {
    const collectionName = String(name || '').trim();
    return {
      findOne(query = {}, options = {}) {
        return dbCall(collectionName, 'findOne', { query, options });
      },
      insertOne(document = {}, options = {}) {
        return dbCall(collectionName, 'insertOne', { document, options });
      },
      updateOne(query = {}, update = {}, options = {}) {
        return dbCall(collectionName, 'updateOne', { query, update, options });
      },
      updateMany(query = {}, update = {}, options = {}) {
        return dbCall(collectionName, 'updateMany', { query, update, options });
      },
      deleteMany(query = {}, options = {}) {
        return dbCall(collectionName, 'deleteMany', { query, options });
      },
      findOneAndUpdate(query = {}, update = {}, options = {}) {
        return dbCall(collectionName, 'findOneAndUpdate', { query, update, options });
      },
      find(query = {}, options = {}) {
        let sort = null;
        let limit = 0;
        const cursor = {
          sort(value) { sort = value || null; return cursor; },
          limit(value) { limit = Math.max(0, Number(value) || 0); return cursor; },
          async toArray() {
            return dbCall(collectionName, 'find', { query, options, sort, limit });
          }
        };
        return cursor;
      }
    };
  }

  function leanable(promise) {
    return {
      lean: () => promise,
      then: (resolve, reject) => promise.then(resolve, reject),
      catch: (reject) => promise.catch(reject),
      finally: (handler) => promise.finally(handler),
    };
  }

  function model(collectionName) {
    const coll = collection(collectionName);
    return {
      async create(document) {
        const result = await coll.insertOne(document || {});
        return result?.document || document;
      },
      findById(id) {
        return leanable(coll.findOne({ _id: id }));
      },
     findOne(query, options) {
        return leanable(coll.findOne(query || {}, options || {}));
      },
      updateOne(query, update, options) {
        return coll.updateOne(query || {}, update || {}, options || {});
      },
      findOneAndUpdate(query, update, options) {
        return leanable(coll.findOneAndUpdate(query || {}, update || {}, options || {}));
      }
    };
  }

  return {
    configure,
    isConfigured,
    ensureReady,
    request,
    dbCall,
    collection,
    model,
    getConfig: () => ({ baseUrl, token, tenantId, numero, timeoutMs }),
  };
}

module.exports = { createControlApiClient, encodeSpecial, decodeSpecial };