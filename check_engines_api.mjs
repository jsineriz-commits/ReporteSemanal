// check_engines_api.mjs
// Usa la API de metadata de Metabase (no ClickHouse) para ver tablas sincronizadas

const METABASE_URL = 'https://metabase.dcac.ar/';
const METABASE_API_KEY = 'mb_OB8cA0XB9CFF8hy4eQFGGDtClBlqtkBIaz2I70Ry0tI=';
const headers = { 'Content-Type': 'application/json', 'X-API-KEY': METABASE_API_KEY };
const DB_ID = 2;

// Tablas que usamos en Q102
const TARGET = new Set([
  'negocios', 'revisaciones', 'liquidaciones', 'liquidacion_oficial',
  'detalles_carga', 'lotes_x_interesados', 'analisis_resultados',
  'analisis_resultados_proyectado', 'informes_baja', 'informes_revisaciones',
  'cd_cotizaciones', 'sociedades_tags', 'rel_usuarios_sociedades',
  'compra_inmediata_faena', 'cd_logs_estado'
]);

async function main() {
  console.log('🔍 Consultando API de metadata de Metabase para DB', DB_ID, '...\n');
  
  // GET /api/database/:id/metadata — devuelve schemas y tablas
  const res = await fetch(METABASE_URL + `api/database/${DB_ID}?include=tables`, {
    headers,
    signal: AbortSignal.timeout(30000)
  });
  
  const ct = res.headers.get('content-type') || '';
  if (!ct.includes('json')) {
    const txt = await res.text();
    throw new Error(`Non-JSON: ${txt.substring(0, 200)}`);
  }
  
  const db = await res.json();
  
  const { writeFile } = await import('fs/promises');
  
  console.log('DB name:', db.name);
  console.log('Engine:', db.engine);
  
  const tables = db.tables || [];
  console.log('Tablas totales sincronizadas:', tables.length);
  
  // Filtrar las relevantes para Q102
  const relevant = tables.filter(t => TARGET.has(t.name));
  
  console.log('\n=== Tablas relevantes para Q102 ===\n');
  relevant.forEach(t => {
    // Metabase guarda algo de metadata pero no el engine de ClickHouse directamente
    console.log(`  ${t.schema}.${t.name}`);
    console.log(`    entity_type: ${t.entity_type}`);
    console.log(`    is_upload: ${t.is_upload}`);
    console.log(`    rows: ${t.rows}`);
    console.log();
  });
  
  // Guardar todo para inspección
  await writeFile('./db_metadata.json', JSON.stringify(db, null, 2));
  console.log('💾 Metadata completa guardada en db_metadata.json');

  // Intentar también /api/table/:id para ver si hay más info del engine
  if (relevant.length > 0) {
    const firstTable = relevant[0];
    console.log(`\n🔎 Detalle de tabla: ${firstTable.schema}.${firstTable.name} (id: ${firstTable.id})`);
    const tRes = await fetch(METABASE_URL + `api/table/${firstTable.id}/query_metadata`, {
      headers, signal: AbortSignal.timeout(15000)
    });
    const tData = await tRes.json();
    console.log('Campos disponibles:', Object.keys(tData));
    await writeFile('./table_metadata_sample.json', JSON.stringify(tData, null, 2));
    console.log('💾 Detalle guardado en table_metadata_sample.json');
  }
}

main().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
