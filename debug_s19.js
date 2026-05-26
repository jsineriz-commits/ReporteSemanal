const fs = require('fs');
function loadEnv(p) {
  if (!fs.existsSync(p)) return;
  for (const l of fs.readFileSync(p, 'utf8').split('\n')) {
    const eq = l.indexOf('=');
    if (eq < 0 || l.trim().startsWith('#')) continue;
    const k = l.slice(0, eq).trim();
    let v = l.slice(eq + 1).trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
    if (!(k in process.env)) process.env[k] = v;
  }
}
loadEnv('.env.local');
loadEnv('.env');

const { getReport } = require('./api/_lib/logic');

// Semana 19: SAB 09/05 - VIE 15/05/2026 (ART = UTC-3)
const startTs = new Date('2026-05-09T00:00:00-03:00').getTime();
const endTs   = new Date('2026-05-15T23:59:59-03:00').getTime();
const AC = 'Agustin Acuna';

console.log('=== DEBUG REPORTE SEMANA 19 ===');
console.log('AC:', AC);
console.log('Range:', new Date(startTs).toLocaleDateString('es-AR'), '->', new Date(endTs).toLocaleDateString('es-AR'));
console.log('startTs:', startTs, '| endTs:', endTs);

getReport(AC, startTs, endTs).then(d => {
  console.log('\n=== CABEZAS OFRECIDAS ===');
  console.log('  cab (total)    :', d.cab);
  console.log('  trop           :', d.trop);
  console.log('  socOf          :', d.socOf);
  console.log('  cabPublicadas  :', d.cabPublicadas, '  <- Publicadas/Ofrec. chip');
  console.log('  cabConc        :', d.cabConc,       '  <- Concretadas chip');
  console.log('  cabNoConc      :', d.cabNoConc,     '  <- No Concretadas chip');
  console.log('  pCab (sem ant) :', d.pCab,          '  <- para chip variacion');
  console.log('  ccc            :', d.ccc);
  console.log('  cotizadas      :', d.cotizadas);
  console.log('  dT (x dia)     :', d.dT);

  console.log('\n=== CABEZAS COMPRADAS ===');
  console.log('  cabC           :', d.cabC);
  console.log('  pCabC          :', d.pCabC);
  console.log('  cabCWeekTrop   :', d.cabCWeekTrop);
  console.log('  cabCSocCount   :', d.cabCSocCount);
  console.log('  dCompras (xdia):', d.dCompras);

  console.log('\n=== CARGAS ===');
  console.log('  carg           :', d.carg);
  console.log('  cargProp       :', d.cargProp);
  console.log('  dCargas (x dia):', d.dCargas);

  console.log('\n=== SACs ===');
  console.log('  sacs.length    :', d.sacs ? d.sacs.length : 0);

  console.log('\n=== SOC. GESTIONADAS CRM ===');
  console.log('  tSG            :', d.tSG);
  console.log('  com            :', d.com);
  console.log('  age            :', d.age);
  console.log('  pTSG (sem ant) :', d.pTSG);
  console.log('  dGestion (xdia):', d.dGestion);

}).catch(err => {
  console.error('ERROR:', err.message);
  console.error(err.stack);
});
