// api/_lib/metabase.js
// Soporta dos modos de auth:
//   1. API Key (METABASE_API_KEY env var) — sin sesión, sin DB issues
//   2. User/password (METABASE_USER + METABASE_PASS) — fallback legacy
const process = require('process');

async function getMetabaseAuth() {
  const rawUrl = (process.env.METABASE_URL || '').trim();
  if (!rawUrl) throw new Error('Falta METABASE_URL en las variables de entorno.');

  const baseUrl = rawUrl.endsWith('/') ? rawUrl : rawUrl + '/';
  const apiKey  = (process.env.METABASE_API_KEY || '').trim();

  // ── Modo 1: API Key (preferido — no crea sesión, evita bugs de FK en login_history) ──
  if (apiKey) {
    return { baseUrl, headers: { 'X-API-KEY': apiKey } };
  }

  // ── Modo 2: User/password (legacy) ──
  const mbUser = (process.env.METABASE_USER || '').trim();
  const mbPass = (process.env.METABASE_PASS || '').trim();
  if (!mbUser || !mbPass) {
    throw new Error('Configuración Metabase incompleta: se necesita METABASE_API_KEY o (METABASE_USER + METABASE_PASS).');
  }

  const res = await fetch(baseUrl + 'api/session', {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ username: mbUser, password: mbPass }),
    signal:  AbortSignal.timeout(15000),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Auth Metabase falló (${res.status}): ${body.substring(0, 200)}`);
  }

  const data = await res.json();
  if (!data.id) throw new Error('Metabase no devolvió token de sesión.');

  return { baseUrl, headers: { 'X-Metabase-Session': data.id } };
}

async function fetchMetabaseQuery(questionId, opts) {
  const maxAttempts = (opts && opts.maxAttempts) ? opts.maxAttempts : 15;
  const auth = await getMetabaseAuth();

  const body = JSON.stringify({ parameters: [] });

  let jsonArray = null;
  let attempts  = 0;

  while (attempts < maxAttempts) {
    const res = await fetch(auth.baseUrl + `api/card/${questionId}/query/json`, {
      method:  'POST',
      headers: {
        'Content-Type': 'application/json',
        ...auth.headers,
      },
      body:   body,
      signal: AbortSignal.timeout(90000),
    });

    if (res.status === 202) {
      attempts++;
      // Job en cola — esperar 3.5s y reintentar
      await new Promise(r => setTimeout(r, 3500));
      continue;
    }

    // Errores transitorios de gateway (502, 503, 504) → reintentar con backoff
    if (res.status === 502 || res.status === 503 || res.status === 504) {
      attempts++;
      const wait = Math.min(5000 + attempts * 2000, 20000); // 5s, 7s, 9s… máx 20s
      console.warn(`[metabase] Q${questionId} → ${res.status}, reintento ${attempts}/${maxAttempts} en ${wait/1000}s...`);
      await new Promise(r => setTimeout(r, wait));
      continue;
    }

    if (!res.ok) {
      throw new Error(`Metabase JSON Export error para Q${questionId} (${res.status})`);
    }

    const rawText = await res.text();
    try {
      jsonArray = JSON.parse(rawText);
    } catch (e) {
      throw new Error(`Metabase devolvió respuesta inválida para Q${questionId} (${res.status}).`);
    }
    break;
  }

  if (!jsonArray) {
    throw new Error(`Metabase Timeout: Q${questionId} no finalizó después de ${maxAttempts} intentos (~${Math.round(maxAttempts * 3.5)}s).`);
  }

  if (!Array.isArray(jsonArray)) {
    throw new Error(`Error en Metabase Export Q${questionId}: respuesta no es un array.`);
  }

  if (jsonArray.length === 0) {
    return { rows: [], headers: [] };
  }

  const rawHeaders = Object.keys(jsonArray[0]);
  const headers    = rawHeaders.map(h => h.trim().toLowerCase());
  const rows       = jsonArray.map(obj => rawHeaders.map(h => obj[h]));

  return { rows, headers };
}

module.exports = { getMetabaseAuth, fetchMetabaseQuery };
