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

  const res = await fetch(tokenData.baseUrl + `api/card/${questionId}/query`, {
    method:  'POST',
    headers: {
      'Content-Type':        'application/json',
      'X-Metabase-Session':  tokenData.id,
    },
    body: JSON.stringify({ ignore_cache: false, parameters: [] }),
    signal: AbortSignal.timeout(90000),
  });

  let jsonRes;
  try {
    // Obtenemos el texto para loguear si falla el parseo
    const rawText = await res.text();
    jsonRes = JSON.parse(rawText);
  } catch (e) {
    throw new Error(`Metabase devolvió respuesta inválida para Q${questionId} (${res.status}).`);
  }

  if (jsonRes.status === "failed") {
    throw new Error(`Metabase Error en Q${questionId}: ${jsonRes.error || "failed"}`);
  }

  if (!jsonRes.data || !Array.isArray(jsonRes.data.rows)) {
    throw new Error(`Error en Metabase Export Q${questionId}: Estructura JSON inesperada.`);
  }

  if (jsonRes.data.rows.length === 0) {
    return { rows: [], headers: [] };
  }

  const headers = jsonRes.data.cols.map(c => (c.name || '').trim().toLowerCase());
  const rows = jsonRes.data.rows;

  return { rows, headers };
}

module.exports = { fetchMetabaseToken, fetchMetabaseQuery };
