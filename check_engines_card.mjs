// check_engines_card.mjs
// Crea una pregunta temporal en Metabase para ver los engines de las tablas

const METABASE_URL = 'https://metabase.dcac.ar/';
const METABASE_API_KEY = 'mb_OB8cA0XB9CFF8hy4eQFGGDtClBlqtkBIaz2I70Ry0tI=';
const headers = { 'Content-Type': 'application/json', 'X-API-KEY': METABASE_API_KEY };
const DB_ID = 2;

const sql = `SELECT database, name, engine FROM system.tables WHERE database IN ('dcac', 'negocios') ORDER BY database, name`;

async function createTempCard() {
  const res = await fetch(METABASE_URL + 'api/card', {
    method: 'POST',
    headers,
    body: JSON.stringify({
      name: '_temp_engine_check',
      display: 'table',
      dataset_query: {
        'lib/type': 'mbql/query',
        database: DB_ID,
        stages: [{ 'lib/type': 'mbql.stage/native', native: sql, 'template-tags': {} }]
      },
      visualization_settings: {}
    }),
    signal: AbortSignal.timeout(30000)
  });
  const ct = res.headers.get('content-type') || '';
  if (!ct.includes('json')) throw new Error(`No-JSON create: ${(await res.text()).substring(0, 200)}`);
  const data = await res.json();
  if (data.errors || data.error) throw new Error(JSON.stringify(data.errors || data.error));
  return data.id;
}

async function runCard(cardId) {
  const res = await fetch(METABASE_URL + `api/card/${cardId}/query/json`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ parameters: [] }),
    signal: AbortSignal.timeout(60000)
  });
  const ct = res.headers.get('content-type') || '';
  if (!ct.includes('json')) throw new Error(`No-JSON run: ${(await res.text()).substring(0, 200)}`);
  return res.json();
}

async function deleteCard(cardId) {
  await fetch(METABASE_URL + `api/card/${cardId}`, { method: 'DELETE', headers });
}

async function main() {
  let cardId;
  try {
    console.log('🔧 Creando card temporal para query system.tables...');
    cardId = await createTempCard();
    console.log(`   Card creado: ${cardId}`);

    console.log('▶️  Ejecutando...');
    const rows = await runCard(cardId);
    
    if (!Array.isArray(rows)) {
      console.log('Respuesta inesperada:', JSON.stringify(rows).substring(0, 500));
      return;
    }

    const { writeFile } = await import('fs/promises');
    await writeFile('./engines.json', JSON.stringify(rows, null, 2));

    console.log('\n=== Engines de tablas ===');
    const needsFinal = [];
    rows.forEach(row => {
      const db = row.database || row[0];
      const name = row.name || row[1];
      const engine = row.engine || row[2] || '';
      const isMerge = ['ReplacingMergeTree','AggregatingMergeTree','CollapsingMergeTree','SummingMergeTree']
        .some(t => engine.includes(t));
      if (isMerge) {
        needsFinal.push(`${db}.${name}`);
        console.log(`🔴 FINAL NEEDED: ${db}.${name} → ${engine}`);
      } else {
        console.log(`✅ OK:           ${db}.${name} → ${engine}`);
      }
    });

    console.log(`\n📊 Tablas que necesitan FINAL: ${needsFinal.length}`);
    if (needsFinal.length) console.log('   ', needsFinal.join(', '));

  } finally {
    if (cardId) {
      console.log(`\n🗑️  Eliminando card temporal ${cardId}...`);
      await deleteCard(cardId);
    }
  }
}

main().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
