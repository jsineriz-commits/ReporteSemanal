// api/_lib/metabase.js
const process = require('process');

async function fetchMetabaseToken() {
  const rawUrl  = (process.env.METABASE_URL || '').trim();
  const mbUser  = (process.env.METABASE_USER || '').trim();
  const mbPass  = (process.env.METABASE_PASS || '').trim();

  if (!rawUrl || !mbUser || !mbPass) {
    throw new Error('Configuración Metabase incompleta en Vercel (faltan METABASE_URL, METABASE_USER o METABASE_PASS).');
  }

  const baseUrl = rawUrl.endsWith('/') ? rawUrl : rawUrl + '/';

  const res = await fetch(baseUrl + 'api/session', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: mbUser, password: mbPass }),
    signal: AbortSignal.timeout(15000),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Auth Metabase falló (${res.status}): ${body.substring(0, 200)}`);
  }

  const data = await res.json();
  if (!data.id) throw new Error('Metabase no devolvió token de sesión.');

  return { id: data.id, baseUrl };
}

async function fetchMetabaseQuery(questionId) {
  const tokenData = await fetchMetabaseToken();
  const params = new URLSearchParams();
  params.append('parameters', '[]');

  let jsonArray = null;
  let attempts = 0;

  while (attempts < 15) {
    const res = await fetch(tokenData.baseUrl + `api/card/${questionId}/query/json`, {
      method:  'POST',
      headers: {
        'Content-Type':        'application/x-www-form-urlencoded',
        'X-Metabase-Session':  tokenData.id,
      },
      body: params.toString(),
      signal: AbortSignal.timeout(90000),
    });

    if (res.status === 202) {
      attempts++;
      // Esperar 3.5 segundos antes de reintentar si el job sigue en cola
      await new Promise(r => setTimeout(r, 3500));
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
    break; // Salió bien
  }

  if (!jsonArray) {
    throw new Error(`Metabase Timeout: la exportación de Q${questionId} no finalizó después de varios intentos.`);
  }

  if (!Array.isArray(jsonArray)) {
    throw new Error(`Error en Metabase Export Q${questionId}: Estructura JSON inesperada (no es array).`);
  }

  if (jsonArray.length === 0) {
    return { rows: [], headers: [] };
  }

  const rawHeaders = Object.keys(jsonArray[0]);
  const headers = rawHeaders.map(h => h.trim().toLowerCase());
  const rows = jsonArray.map(obj => rawHeaders.map(h => obj[h]));

  return { rows, headers };
}

module.exports = { fetchMetabaseToken, fetchMetabaseQuery };
