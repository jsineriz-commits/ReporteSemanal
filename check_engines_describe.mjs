// check_engines_describe.mjs
// Usa SHOW CREATE TABLE para ver el engine de cada tabla

const METABASE_URL = 'https://metabase.dcac.ar/';
const METABASE_API_KEY = 'mb_OB8cA0XB9CFF8hy4eQFGGDtClBlqtkBIaz2I70Ry0tI=';
const headers = { 'Content-Type': 'application/json', 'X-API-KEY': METABASE_API_KEY };
const DB_ID = 2;

// Tablas clave de Q102 (las que hacemos SELECT directo)
const TABLES = [
  'dcac.negocios',
  'dcac.revisaciones',
  'negocios.liquidaciones',
  'negocios.liquidacion_oficial',
  'dcac.detalles_carga',
  'dcac.lotes_x_interesados',
  'dcac.analisis_resultados',
  'dcac.analisis_resultados_proyectado',
  'dcac.informes_baja',
  'dcac.informes_revisaciones',
  'negocios.cd_cotizaciones',
  'dcac.sociedades_tags',
  'dcac.rel_usuarios_sociedades',
  'negocios.compra_inmediata_faena',
  'negocios.cd_logs_estado',
];

async function createAndRunCard(query) {
  const createRes = await fetch(METABASE_URL + 'api/card', {
    method: 'POST', headers,
    body: JSON.stringify({
      name: '_temp_desc',
      display: 'table',
      dataset_query: {
        'lib/type': 'mbql/query', database: DB_ID,
        stages: [{ 'lib/type': 'mbql.stage/native', native: query, 'template-tags': {} }]
      },
      visualization_settings: {}
    }),
    signal: AbortSignal.timeout(10000)
  });
  const card = await createRes.json();
  if (card.error) throw new Error(card.error);
  const cardId = card.id;

  try {
    const runRes = await fetch(METABASE_URL + `api/card/${cardId}/query/json`, {
      method: 'POST', headers,
      body: JSON.stringify({ parameters: [] }),
      signal: AbortSignal.timeout(20000)
    });
    const data = await runRes.json();
    if (data.error) throw new Error(data.error);
    return Array.isArray(data) ? data : [];
  } finally {
    fetch(METABASE_URL + `api/card/${cardId}`, { method: 'DELETE', headers }).catch(() => {});
  }
}

async function checkTable(table) {
  // SHOW CREATE TABLE devuelve una sola fila con el DDL que incluye el engine
  const sql = `SHOW CREATE TABLE ${table}`;
  try {
    const rows = await createAndRunCard(sql);
    if (!rows.length) return { table, engine: 'UNKNOWN', ddl: '' };
    const ddl = Object.values(rows[0])[0] || '';
    
    let engine = 'MergeTree'; // default
    const engineMatch = ddl.match(/ENGINE\s*=\s*(\w+)/i);
    if (engineMatch) engine = engineMatch[1];
    
    return { table, engine, ddl };
  } catch(e) {
    return { table, engine: 'ERROR', error: e.message };
  }
}

async function main() {
  console.log('🔍 Verificando engine de cada tabla usada en Q102...\n');
  
  const results = [];
  const needsFinal = [];

  for (const table of TABLES) {
    process.stdout.write(`   ${table} ... `);
    const result = await checkTable(table);
    results.push(result);
    
    const isMerge = ['ReplacingMergeTree','AggregatingMergeTree','CollapsingMergeTree','SummingMergeTree']
      .some(t => result.engine.includes(t));
    
    if (result.engine === 'ERROR') {
      console.log(`❓ ERROR: ${result.error}`);
    } else if (isMerge) {
      console.log(`🔴 ${result.engine} → NECESITA FINAL`);
      needsFinal.push(table);
    } else {
      console.log(`✅ ${result.engine}`);
    }
  }

  const { writeFile } = await import('fs/promises');
  await writeFile('./engines_q102.json', JSON.stringify(results, null, 2));

  console.log(`\n${'═'.repeat(55)}`);
  console.log(`📊 Tablas que necesitan FINAL: ${needsFinal.length}/${TABLES.length}`);
  needsFinal.forEach(t => console.log(`   🔴 ${t}`));
  console.log('\n💾 Detalle guardado en engines_q102.json');
}

main().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
