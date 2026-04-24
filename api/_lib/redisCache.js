// api/_lib/redisCache.js
// Cache persistente en Upstash Redis para datos de Metabase.
// Usa la REST API de Upstash (sin dependencias npm extra).
// Comprime con zlib para reducir el tamaño almacenado.
// TTL: 24 horas. Funciona solo en producción (Vercel).
// En local, si no hay env vars, retorna null silenciosamente.

const zlib = require('zlib');
const { promisify } = require('util');
const gzipAsync   = promisify(zlib.gzip);
const gunzipAsync = promisify(zlib.gunzip);

const REDIS_URL   = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const TTL_S = 24 * 60 * 60; // 24 horas en segundos

const KEYS = {
  base:  'rs:metaBase',
  ops:   'rs:metaOps',
  bcMap: 'rs:bcMap',
  ts:    'rs:ts',
};

function isConfigured() {
  return !!(REDIS_URL && REDIS_TOKEN);
}

// Ejecuta un comando Redis vía REST API de Upstash
async function redisCmd(cmd) {
  const res = await fetch(REDIS_URL, {
    method:  'POST',
    headers: {
      'Authorization': `Bearer ${REDIS_TOKEN}`,
      'Content-Type':  'application/json',
    },
    body: JSON.stringify(cmd),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Upstash HTTP ${res.status}: ${text}`);
  }
  const data = await res.json();
  if (data.error) throw new Error('[redisCache] ' + data.error);
  return data.result;
}

async function compress(obj) {
  const buf = await gzipAsync(JSON.stringify(obj));
  return buf.toString('base64');
}

async function decompress(b64) {
  const buf  = Buffer.from(b64, 'base64');
  const data = await gunzipAsync(buf);
  return JSON.parse(data.toString('utf8'));
}

// ─── readCache ────────────────────────────────────────────────────────────────
// Devuelve { metaBase, metaOps, bcMapObj, ts } o null si no hay datos / no configurado.
async function readCache() {
  if (!isConfigured()) return null;
  try {
    const [b64Base, b64Ops, b64Bc, tsStr] = await Promise.all([
      redisCmd(['GET', KEYS.base]),
      redisCmd(['GET', KEYS.ops]),
      redisCmd(['GET', KEYS.bcMap]),
      redisCmd(['GET', KEYS.ts]),
    ]);
    if (!b64Base || !b64Ops || !b64Bc || !tsStr) {
      console.log('[redisCache] Miss — una o más claves vacías');
      return null;
    }
    const ts  = parseInt(tsStr, 10);
    const age = Date.now() - ts;
    console.log(`[redisCache] Hit — datos de hace ${Math.round(age / 60000)}min`);

    const [metaBase, metaOps, bcMapObj] = await Promise.all([
      decompress(b64Base),
      decompress(b64Ops),
      decompress(b64Bc),
    ]);
    return { metaBase, metaOps, bcMapObj, ts };
  } catch (e) {
    console.error('[redisCache] Error al leer:', e.message);
    return null;
  }
}

// ─── writeCache ───────────────────────────────────────────────────────────────
// Guarda metaBase (Q101), metaOps (Q102) y bcMapObj (mapa kt/kv) en Redis.
// bcMapObj es el objeto plano { cuit: { kt, kv }, ... } derivado de Q221.
async function writeCache(metaBase, metaOps, bcMapObj) {
  if (!isConfigured()) return null;
  try {
    const ts = Date.now();
    const [b64Base, b64Ops, b64Bc] = await Promise.all([
      compress(metaBase),
      compress(metaOps),
      compress(bcMapObj),
    ]);
    const totalKB = Math.round((b64Base.length + b64Ops.length + b64Bc.length) / 1024);

    await Promise.all([
      redisCmd(['SET', KEYS.base,  b64Base,    'EX', TTL_S]),
      redisCmd(['SET', KEYS.ops,   b64Ops,     'EX', TTL_S]),
      redisCmd(['SET', KEYS.bcMap, b64Bc,      'EX', TTL_S]),
      redisCmd(['SET', KEYS.ts,    String(ts), 'EX', TTL_S]),
    ]);
    console.log(`[redisCache] Guardado (${totalKB} KB comprimido) — expira en 24h`);
    return ts;
  } catch (e) {
    console.error('[redisCache] Error al escribir:', e.message);
    return null;
  }
}

// ─── deleteCache ──────────────────────────────────────────────────────────────
async function deleteCache() {
  if (!isConfigured()) return;
  try {
    await redisCmd(['DEL', KEYS.base, KEYS.ops, KEYS.bcMap, KEYS.ts]);
    console.log('[redisCache] Cache Redis eliminado');
  } catch (e) {
    console.error('[redisCache] Error al eliminar:', e.message);
  }
}

module.exports = { isConfigured, readCache, writeCache, deleteCache };
