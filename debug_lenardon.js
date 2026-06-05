// debug_lenardon.js — chequea el CUIT de LENARDON IVAR RAUL en Q102 y Q221
const fs = require('fs');
const path = require('path');

const cacheFile = path.join(__dirname, 'metabase_data_cache.json');
if (!fs.existsSync(cacheFile)) {
  console.log('❌ No hay cache en disco. Arranca el servidor primero y espera que se cargue.');
  process.exit(1);
}

const cache = JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
const data = cache.data;

// ─── Q102: buscar LENARDON ──────────────────────────────────────────────────
const hOps = data.metaOps.headers;
const iSocV = hOps.indexOf('rs_vendedora');
const iCuitV = hOps.indexOf('cuit_vend');
const iId   = hOps.indexOf('id');

console.log('\n=== Q102 Headers ===');
console.log(hOps.map((h,i)=> i+':'+h).join(', '));

const lenardonRows = (data.metaOps.rows || []).filter(r =>
  String(r[iSocV] || '').toLowerCase().includes('lenardon')
);
console.log('\n=== Filas Q102 con LENARDON (' + lenardonRows.length + ') ===');
lenardonRows.slice(0, 5).forEach(r => {
  console.log(`  ID=${r[iId]}  soc_v="${r[iSocV]}"  cuit_vend="${r[iCuitV]}"  (type: ${typeof r[iCuitV]})`);
});

// ─── Q221: buscar CUIT 20166352453 ─────────────────────────────────────────
const hEstab = data.metaEstab.headers;
const iCuitE = hEstab.indexOf('cuit_titular_est');
const iTit   = hEstab.indexOf('titular_est');
const iBov   = hEstab.indexOf('bovinos');

console.log('\n=== Q221: CUIT 20166352453 en metaEstab ===');
const q221Rows = (data.metaEstab.rows || []).filter(r =>
  String(r[iCuitE] || '').includes('20166352453') ||
  String(r[iTit] || '').toLowerCase().includes('lenardon')
);
q221Rows.slice(0, 5).forEach(r => {
  console.log(`  titular="${r[iTit]}"  cuit="${r[iCuitE]}"  bovinos=${r[iBov]}`);
});

// ─── Comparar exactamente ──────────────────────────────────────────────────
if (lenardonRows.length > 0) {
  const cuitQ102 = String(lenardonRows[0][iCuitV] || '').trim();
  const cuitQ221 = '20166352453';
  console.log('\n=== Comparación ===');
  console.log('  CUIT en Q102:', JSON.stringify(cuitQ102), '| len:', cuitQ102.length);
  console.log('  CUIT en Q221:', JSON.stringify(cuitQ221), '| len:', cuitQ221.length);
  console.log('  ¿Match exacto?:', cuitQ102 === cuitQ221 ? '✅ SÍ' : '❌ NO');
  console.log('  ¿Match slice(0,10)?:', cuitQ102.slice(0,10) === cuitQ221.slice(0,10) ? '✅ SÍ' : '❌ NO');
}
