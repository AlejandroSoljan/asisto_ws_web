'use strict';

const { createHash } = require('crypto');
const pevExpectedKeyHash = '842ee599220def5992ffcf9d544d96bf52103b8db579d92d1e9b95c0f9168b97';
const lastReports = new Map();

// Inspect the final URL passed to fetch, never the configured defaults.
function describeRequest(tenant, requestUrl) {
  if (!/^PEV[1-4]?$/.test(String(tenant))) return null;
  const url = new URL(requestUrl);
  const keys = url.searchParams.getAll('key');
  return {
    tenant: String(tenant),
    method: 'GET',
    endpoint: url.origin + url.pathname,
    nro_tel_from: url.searchParams.getAll('nro_tel_from'),
    key_present: keys.length > 0 && keys.every(Boolean),
    key_count: keys.length,
    key_matches_expected: keys.length === 1 && createHash('sha256').update(keys[0]).digest('hex') === pevExpectedKeyHash
  };
}

function logRequestDiagnostic(tenant, requestUrl, logger, now = Date.now()) {
  try {
    const report = describeRequest(tenant, requestUrl);
    if (!report) return;
    const serialized = JSON.stringify(report);
    const previous = lastReports.get(tenant);
    if (previous && previous.serialized === serialized && now - previous.at < 60000) return;
    logger('[API_MENSAJES_REQUEST] ' + serialized);
    lastReports.set(tenant, { serialized, at: now });
  } catch {
    // Diagnostic failures must never alter polling or expose a raw URL.
  }
}

module.exports = { describeRequest, logRequestDiagnostic };
