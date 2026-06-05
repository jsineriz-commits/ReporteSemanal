// check_engines_targeted.mjs
// Consulta engines solo de las tablas específicas usadas en Q102

const METABASE_URL = 'https://metabase.dcac.ar/';
const METABASE_API_KEY = 'mb_OB8cA0XB9CFF8hy4eQFGGDtClBlqtkBIaz2I70Ry0tI=';
const headers = { 'Content-Type': 'application/json', 'X-API-KEY': METABASE_API_KEY };
const DB_ID = 2;

// Solo las tablas que hacemos SELECT directo (no joins a pequeñas como categorias/provincias/etc)
const TARGET_TABLES = [
  ['dcac', 'negocios'],
  ['dcac', 'revisaciones'],
  ['dcac', 'liquidaciones'],
  ['negocios', 'liquidaciones'],
  ['negocios', 'liquidacion_oficial'],
  ['dcac', 'detalles_carga'],
  ['dcac', 'lotes_x_interesados'],
  ['dcac', 'analisis_resultados'],
  ['dcac', 'analisis_resultados_proyectado'],
  ['dcac', 'informes_baja'],
  ['dcac', 'informes_revisaciones'],
  ['negocios', 'cd_cotizaciones'],
  ['dcac', 'sociedades_tags'],
  ['dcac', 'rel_usuarios_sociedades'],
  ['negocios', 'compra_inmediata_faena'],
  ['negocios', 'cd_logs_estado'],
];

// Armamos una IN list corta
const inList = TARGET_TABLES.map(([db, t]) => `('${db}','${t}')`).join(',');
const sql = `SELECT database, name, engine FROM system.tables WHERE (database, name) IN (${inList})`;

async function createAndRun(query) {
  // Crear card
  const createRes = await fetch(METABASE_URL + 'api/card', {
    method: 'POST', headers,
    body: JSON.stringify({
      name: '_temp_engines',
      display: 'table',
      dataset_query: {
        'lib/type': 'mbql/query', database: DB_ID,
        stages: [{ 'lib/type': 'mbql.stage/native', native: query, 'template-tags': {} }]
      },
      visualization_settings: {}
    }),
    signal: AbortSignal.timeout(15000)
  });
  const card = await createRes.json();
  if (card.error || card.errors) throw new Error(JSON.stringify(card.error || card.errors));
  const cardId = card.id;
  console.log('   Card temporal:', cardId);

  try {
    // Ejecutar con timeout más corto
    const runRes = await fetch(METABASE_URL + `api/card/${cardId}/query/json`, {
      method: 'POST', headers,
      body: JSON.stringify({ parameters: [] }),
      signal: AbortSignal.timeout(30000)
    });
    const ct = runRes.headers.get('content-type') || '';
    if (!ct.includes('json')) {
      const txt = await runRes.text();
      throw new Error(`Non-JSON: ${txt.substring(0, 200)}`);
    }
    return await runRes.json();
  } finally {
    await fetch(METABASE_URL + `api/card/${cardId}`, { method: 'DELETE', headers }).catch(() => {});
    console.log(`   Card ${cardId} eliminado`);
  }
}

async function main() {
  console.log('🔍 Verificando engines de tablas clave de Q102...\n');
  console.log('SQL:', sql.substring(0, 200));
  
  let rows;
  try {
    rows = await createAndRun(sql);
  } catch(e) {
    console.error('❌ Error:', e.message);
    process.exit(1);
  }

  if (!Array.isArray(rows) || rows.length === 0) {
    console.log('Respuesta:', JSON.stringify(rows).substring(0, 500));
    return;
  }

  const { writeFile } = await import('fs/promises');
  await writeFile('./engines_q102.json', JSON.stringify(rows, null, 2));

  console.log('\n=== Engines de tablas usadas en Q102 ===\n');
  const needsFinal = [];
  rows.forEach(row => {
    const db = row.database, name = row.name, engine = row.engine || '';
    const isMerge = ['ReplacingMergeTree','AggregatingMergeTree','CollapsingMergeTree','SummingMergeTree']
      .some(t => engine.includes(t));
    if (isMerge) {
      needsFinal.push(`${db}.${name}`);
      console.log(`🔴 FINAL NEEDED  ${db}.${name}\n   → ${engine}`);
    } else {
      console.log(`✅ OK            ${db}.${name}\n   → ${engine}`);
    }
  });

  console.log(`\n${'─'.repeat(50)}`);
  console.log(`Total tablas que NECESITAN FINAL: ${needsFinal.length}`);
  if (needsFinal.length) {
    console.log('Tablas:');
    needsFinal.forEach(t => console.log('  •', t));
  }
}

main().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
