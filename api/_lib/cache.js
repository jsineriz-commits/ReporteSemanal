// api/_lib/cache.js
// Cache en memoria. Persiste entre invocaciones calientes.
// En cold start se regenera con el warm-up cron (cada 1 hora).

const _store = new Map();

const TTL = {
  CONFIG: 3600 * 1000,        // 1 hora
  DATA:   24 * 3600 * 1000,   // 24 horas (alineado con disk cache)
  REPORT: 24 * 3600 * 1000,   // 24 horas
  AUX:    7200 * 1000,        // 2 horas
};

function set(key, data, ttlMs) {
  _store.set(key, { data, expires: Date.now() + (ttlMs || TTL.REPORT) });
}

function get(key) {
  const entry = _store.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expires) { _store.delete(key); return null; }
  return entry.data;
}

function del(key) { _store.delete(key); }

function delByPrefix(prefix) {
  for (const k of _store.keys()) {
    if (k.startsWith(prefix)) _store.delete(k);
  }
}

function flush() { _store.clear(); }

/** Lista de claves (para debug) */
function keys() { return Array.from(_store.keys()); }

module.exports = { set, get, del, delByPrefix, flush, keys, TTL };
