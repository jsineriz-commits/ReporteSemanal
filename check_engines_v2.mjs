// check_engines_v2.mjs
// Usa la API de Metabase correcta para ejecutar una query ad-hoc

const METABASE_URL = 'https://metabase.dcac.ar/';
const METABASE_API_KEY = 'mb_OB8cA0XB9CFF8hy4eQFGGDtClBlqtkBIaz2I70Ry0tI=';
const headers = { 'Content-Type': 'application/json', 'X-API-KEY': METABASE_API_KEY };
const DB_ID = 2; // base de datos ClickHouse en Metabase

const sql = `SELECT database, name, engine FROM system.tables WHERE database IN ('dcac', 'negocios') ORDER BY database, name`;

async function runNativeQuery(query) {
  // Endpoint correcto para queries ad-hoc en Metabase
  const body = {
    database: DB_ID,
    type: 'native',
    native: { query },
    parameters: []
  };

  const res = await fetch(METABASE_URL + 'api/dataset', {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(60000)
  });

  const ct = res.headers.get('content-type') || '';
  if (!ct.includes('json')) {
    const text = await res.text();
    throw new Error(`No-JSON response (${res.status}): ${text.substring(0, 300)}`);
  }

  const data = await res.json();
  if (data.error) throw new Error(`Metabase error: ${data.error}`);
  return data;
}

async function main() {
  console.log('🔍 Consultando engines via Metabase API /dataset ...');

  let data;
  try {
    data = await runNativeQuery(sql);
  } catch (e) {
    console.error('❌ /api/dataset falló:', e.message);
    console.log('\n🔄 Intentando via /api/card (crear card temporal)...');
    // Fallback: leer las tablas de system desde Metabase usando card existente
    // Alternativamente, inferimos por la Q102 actual
    inferFromSQL();
    return;
  }

  const cols = data.data?.cols?.map(c => c.name) || [];
  const rows = data.data?.rows || [];

  console.log('Columnas:', cols);

  const needsFinal = [];
  rows.forEach(row => {
    const db = row[0], name = row[1], engine = row[2] || '';
    const isMerge = engine.includes('ReplacingMergeTree') ||
                    engine.includes('AggregatingMergeTree') ||
                    engine.includes('CollapsingMergeTree') ||
                    engine.includes('SummingMergeTree');
    if (isMerge) {
      needsFinal.push({ db, name, engine });
      console.log(`🔴 FINAL NEEDED: ${db}.${name} → ${engine}`);
    } else {
      console.log(`✅ OK: ${db}.${name} → ${engine}`);
    }
  });

  console.log(`\n📊 Total necesitan FINAL: ${needsFinal.length}`);
}

function inferFromSQL() {
  // Basándonos en convención de nombres y estructura,
  // las tablas tipo ReplacingMergeTree en sistemas ClickHouse suelen ser
  // las de "liquidaciones", "analisis_resultados", "analisis_resultados_proyectado"
  console.log('\n⚠️  Sin acceso a system.tables. Inferencia por convención:');
  console.log('   Las tablas de tipo *resultados*, *liquidaciones*, *detalles_carga*');
  console.log('   en ClickHouse suelen ser ReplacingMergeTree → necesitan FINAL');
  console.log('\n   Necesito acceso directo a ClickHouse o permisos de admin en Metabase');
  console.log('   para verificar los engines. ¿Tenés acceso HTTP a ClickHouse?');
}

main().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
