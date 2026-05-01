// api/_lib/blobCache.js
// Cache persistente en Vercel Blob para datos de Metabase.
// TTL: 24 horas. Sin dependencias npm extra — usa fetch directamente.

const zlib = require('zlib');
const { promisify } = require('util');
const gzipAsync   = promisify(zlib.gzip);
const gunzipAsync = promisify(zlib.gunzip);

const BLOB_TOKEN = process.env.BLOB_READ_WRITE_TOKEN;
const BLOB_API   = 'https://blob.vercel-storage.com';
const API_VER    = '7';
const CACHE_NAME = 'rs-mc-v2.gz'; // v2: evita conflicto con blob público previo
const TTL_MS     = 24 * 60 * 60 * 1000; // 24h

function isConfigured() { return !!BLOB_TOKEN; }

function authHeaders(extra) {
  return { Authorization: `Bearer ${BLOB_TOKEN}`, 'x-api-version': API_VER, ...extra };
}

async function listBlob() {
  const res = await fetch(`${BLOB_API}?prefix=${encodeURIComponent(CACHE_NAME)}&limit=1`, {
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(`Blob list HTTP ${res.status}`);
  return (await res.json()).blobs || [];
}

// ─── readCache ────────────────────────────────────────────────────────────────
async function readCache() {
  if (!isConfigured()) return null;
  try {
    const blobs = await listBlob();
    if (!blobs.length) { console.log('[blobCache] Miss — sin datos'); return null; }
    const blob = blobs[0];
    const age  = Date.now() - new Date(blob.uploadedAt).getTime();
    if (age > TTL_MS) { console.log(`[blobCache] Caducado (${Math.round(age/60000)}min)`); return null; }
    console.log(`[blobCache] Hit — datos de hace ${Math.round(age/60000)}min`);
    // Blobs privados requieren auth — descargar vía endpoint autenticado
    const downloadUrl = `${BLOB_API}?url=${encodeURIComponent(blob.url)}`;
    const res = await fetch(downloadUrl, { headers: authHeaders() });
    if (!res.ok) throw new Error(`Blob fetch HTTP ${res.status}`);
    const raw = await gunzipAsync(Buffer.from(await res.arrayBuffer()));
    return JSON.parse(raw.toString('utf8'));
  } catch (e) { console.error('[blobCache] Error al leer:', e.message); return null; }
}

// ─── writeCache ───────────────────────────────────────────────────────────────
async function writeCache(metaBase, metaOps, bcMapObj) {
  if (!isConfigured()) return null;
  try {
    const ts      = Date.now();
    const buf     = await gzipAsync(JSON.stringify({ ts, metaBase, metaOps, bcMapObj }));
    const mb      = (buf.length / 1048576).toFixed(1);
    // Borrar blob previo para evitar conflicto de access level
    await deleteCache();
    const res = await fetch(`${BLOB_API}/${CACHE_NAME}`, {
      method:  'PUT',
      headers: authHeaders({
        'Content-Type':        'application/gzip',
        'x-add-random-suffix': '0',
        'x-cache-control-max-age': '86400',
        'x-allowed-content-types': 'application/gzip',
      }),
      body: buf,
    });
    if (!res.ok) throw new Error(`Blob PUT HTTP ${res.status}: ${await res.text()}`);
    console.log(`[blobCache] Guardado (${mb} MB gzip) — expira en 24h`);
    return ts;
  } catch (e) { console.error('[blobCache] Error al escribir:', e.message); return null; }
}

// ─── deleteCache ──────────────────────────────────────────────────────────────
async function deleteCache() {
  if (!isConfigured()) return;
  try {
    const blobs = await listBlob();
    if (!blobs.length) return;
    await fetch(BLOB_API, {
      method:  'DELETE',
      headers: authHeaders({ 'Content-Type': 'application/json' }),
      body:    JSON.stringify({ urls: blobs.map(b => b.url) }),
    });
    console.log('[blobCache] Cache eliminado');
  } catch (e) { console.error('[blobCache] Error al eliminar:', e.message); }
}

module.exports = { isConfigured, readCache, writeCache, deleteCache };
