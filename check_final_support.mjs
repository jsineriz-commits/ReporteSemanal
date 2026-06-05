// check_final_support.mjs
// Prueba si cada tabla soporta FINAL ejecutando SELECT ... FROM tabla FINAL LIMIT 0

const METABASE_URL = 'https://metabase.dcac.ar/';
const METABASE_API_KEY = 'mb_OB8cA0XB9CFF8hy4eQFGGDtClBlqtkBIaz2I70Ry0tI=';
const headers = { 'Content-Type': 'application/json', 'X-API-KEY': METABASE_API_KEY };
const DB_ID = 2;

// Tablas que hacemos SELECT directo en Q102 (las que entran en los _uniq CTEs o en faena/invernada_base)
const TABLES = [
  'negocios.liquidaciones',
  'dcac.detalles_carga',
  'dcac.lotes_x_interesados',
  'dcac.analisis_resultados',
  'dcac.analisis_resultados_proyectado',
  'dcac.informes_baja',
  'dcac.informes_revisaciones',
  'negocios.cd_cotizaciones',
  'dcac.negocios',
  'dcac.revisaciones',
  'negocios.liquidacion_oficial',
  'negocios.compra_inmediata_faena',
  'negocios.cd_logs_estado',
  'dcac.rel_usuarios_sociedades',
  'dcac.sociedades_tags',
];

async function createCard(query) {
  const res = await fetch(METABASE_URL + 'api/card', {
    method: 'POST', headers,
    body: JSON.stringify({
      name: '_tmp',
      display: 'table',
      dataset_query: {
        'lib/type': 'mbql/query', database: DB_ID,
        stages: [{ 'lib/type': 'mbql.stage/native', native: query, 'template-tags': {} }]
      },
      visualization_settings: {}
    }),
    signal: AbortSignal.timeout(8000)
  });
  const d = await res.json();
  if (d.error) throw new Error('create: ' + d.error);
  return d.id;
}

async function runCard(id) {
  const res = await fetch(METABASE_URL + `api/card/${id}/query/json`, {
    method: 'POST', headers,
    body: JSON.stringify({ parameters: [] }),
    signal: AbortSignal.timeout(15000)
  });
  const ct = res.headers.get('content-type') || '';
  if (!ct.includes('json')) throw new Error('non-json: ' + (await res.text()).substring(0, 100));
  return res.json();
}

async function deleteCard(id) {
  fetch(METABASE_URL + `api/card/${id}`, { method: 'DELETE', headers }).catch(() => {});
}

async function testFinal(table) {
  const sql = `SELECT 1 FROM ${table} FINAL LIMIT 0`;
  let cardId;
  try {
    cardId = await createCard(sql);
    const result = await runCard(cardId);
    
    // Si es array (sin error), FINAL funciona
    if (Array.isArray(result)) return { table, supportsFinal: true, note: 'OK' };
    
    // Si tiene error de "not supported" → no es MergeTree que soporta FINAL
    const errMsg = result.error || result.message || '';
    if (errMsg.toLowerCase().includes('final') || errMsg.toLowerCase().includes('not supported')) {
      return { table, supportsFinal: false, note: errMsg.substring(0, 100) };
    }
    return { table, supportsFinal: true, note: 'OK (no error)' };
  } catch(e) {
    const msg = e.message || '';
    if (msg.includes('timeout')) return { table, supportsFinal: null, note: 'TIMEOUT' };
    return { table, supportsFinal: null, note: 'ERROR: ' + msg.substring(0, 80) };
  } finally {
    if (cardId) await deleteCard(cardId);
  }
}

async function main() {
  console.log('🔍 Verificando soporte de FINAL en tablas de Q102...\n');
  console.log('   (SELECT 1 FROM tabla FINAL LIMIT 0 — si falla, no soporta FINAL)\n');

  const supportsFinal = [];
  const noFinal = [];
  const unknown = [];

  for (const table of TABLES) {
    process.stdout.write(`   ${table.padEnd(42)} `);
    const result = await testFinal(table);
    
    if (result.supportsFinal === true) {
      console.log(`✅ SOPORTA FINAL`);
      supportsFinal.push(table);
    } else if (result.supportsFinal === false) {
      console.log(`❌ NO SOPORTA FINAL  (${result.note})`);
      noFinal.push(table);
    } else {
      console.log(`❓ ${result.note}`);
      unknown.push(table);
    }
  }

  console.log(`\n${'═'.repeat(60)}`);
  console.log(`✅ Soportan FINAL (${supportsFinal.length}): ${supportsFinal.join(', ')}`);
  console.log(`❌ NO soportan FINAL (${noFinal.length}): ${noFinal.join(', ')}`);
  console.log(`❓ Sin verificar (${unknown.length}): ${unknown.join(', ')}`);

  if (supportsFinal.length > 0) {
    console.log('\n🔧 Tablas donde agregar FINAL en Q102:');
    supportsFinal.forEach(t => console.log(`   • ${t}`));
  }
}

main().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
