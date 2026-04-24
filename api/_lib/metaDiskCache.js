// api/_lib/metaDiskCache.js
// Cache persistente en disco para datos de Metabase (Q101, Q102, Q221)
// TTL: 12 horas. Permite carga rápida al reiniciar el servidor.

const fs = require('fs');
const path = require('path');

const CACHE_FILE = path.join(__dirname, 'metabase_data_cache.json');
const TTL_MS = 24 * 60 * 60 * 1000; // 24 horas

function readCache() {
  try {
    if (!fs.existsSync(CACHE_FILE)) return null;
    const raw = fs.readFileSync(CACHE_FILE, 'utf8');
    const data = JSON.parse(raw);
    if (!data || !data.ts) return null;
    const age = Date.now() - data.ts;
    if (age > TTL_MS) {
      console.log(`[diskCache] Caducado (${Math.round(age/3600000*10)/10}h > 12h)`);
      return null;
    }
    console.log(`[diskCache] Hit — datos de hace ${Math.round(age/60000)}min`);
    return data;
  } catch (e) {
    console.error('[diskCache] Error al leer:', e.message);
    return null;
  }
}

function writeCache(payload) {
  try {
    const data = { ts: Date.now(), ...payload };
    fs.writeFileSync(CACHE_FILE, JSON.stringify(data), 'utf8');
    const mb = (Buffer.byteLength(JSON.stringify(data)) / 1048576).toFixed(1);
    console.log(`[diskCache] Guardado (${mb} MB) — expira en 12h`);
    return data.ts;
  } catch (e) {
    console.error('[diskCache] Error al escribir:', e.message);
    return null;
  }
}

function deleteCache() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      fs.unlinkSync(CACHE_FILE);
      console.log('[diskCache] Eliminado');
    }
  } catch (e) {
    console.error('[diskCache] Error al eliminar:', e.message);
  }
}

function getCacheTs() {
  try {
    if (!fs.existsSync(CACHE_FILE)) return null;
    const raw = fs.readFileSync(CACHE_FILE, 'utf8');
    const data = JSON.parse(raw);
    if (!data || !data.ts) return null;
    if (Date.now() - data.ts > TTL_MS) return null;
    return data.ts;
  } catch (e) { return null; }
}

module.exports = { readCache, writeCache, deleteCache, getCacheTs, TTL_MS, CACHE_FILE };
