// check_engines.mjs — ver engines de las tablas usadas en Q102
const METABASE_URL = 'https://metabase.dcac.ar/';
const METABASE_API_KEY = 'mb_OB8cA0XB9CFF8hy4eQFGGDtClBlqtkBIaz2I70Ry0tI=';
const headers = { 'Content-Type': 'application/json', 'X-API-KEY': METABASE_API_KEY };

const sql = `SELECT database, name, engine FROM system.tables WHERE database IN ('dcac', 'negocios') ORDER BY database, name`;

async function runQuery(q) {
  const res = await fetch(METABASE_URL + 'api/dataset', {
    method: 'POST',
    headers,
    body: JSON.stringify({ database: 2, type: 'native', native: { query: q } })
  });
  const data = await res.json();
  if (data.error) throw new Error(data.error);
  return data.data?.rows || [];
}

async function main() {
  console.log('🔍 Consultando engines de tablas en dcac y negocios...\n');
  const rows = await runQuery(sql);
  
  const needsFinal = [];
  const ok = [];

  rows.forEach(([db, name, engine]) => {
    const isMerge = engine && (
      engine.includes('ReplacingMergeTree') ||
      engine.includes('AggregatingMergeTree') ||
      engine.includes('CollapsingMergeTree') ||
      engine.includes('SummingMergeTree') ||
      engine.includes('VersionedCollapsingMergeTree')
    );
    if (isMerge) needsFinal.push({ db, name, engine });
    else ok.push({ db, name, engine });
  });

  console.log('=== 🔴 Tablas que necesitan FINAL ===');
  needsFinal.forEach(t => console.log(`  ${t.db}.${t.name} → ${t.engine}`));

  console.log('\n=== ✅ Tablas MergeTree simples (sin FINAL necesario) ===');
  ok.forEach(t => console.log(`  ${t.db}.${t.name} → ${t.engine}`));
}

main().catch(e => { console.error('Error:', e.message); process.exit(1); });
