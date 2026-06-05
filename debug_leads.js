const fs = require('fs');
const path = require('path');
function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) return;
  const lines = fs.readFileSync(filePath, 'utf8').split('\n');
  for (const raw of lines) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eqIdx = line.indexOf('=');
    if (eqIdx < 0) continue;
    const key = line.slice(0, eqIdx).trim();
    let val = line.slice(eqIdx + 1).trim();
    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) val = val.slice(1, -1);
    if (!(key in process.env)) process.env[key] = val;
  }
}
loadEnvFile(path.join(__dirname, '.env.local'));
loadEnvFile(path.join(__dirname, '.env'));

const logic = require('./api/_lib/logic');

async function test() {
  try {
    const D = await logic.loadData();
    
    // leadFuenteMap
    const leadFuenteMap = {};
    for (const lr of D.leads) {
      if (lr[3]) leadFuenteMap[lr[3]] = lr[2];
    }
    
    // Comentarios de Sem 16 para aacuna (20260418-20260424)
    const acMail = 'aacuna@decampoacampo.com';
    const sem16Coms = D.coms.filter(r => r[0] === acMail && r[1] >= '20260418' && r[1] <= '20260424');
    
    console.log(`\nComs de Sem 16 para aacuna: ${sem16Coms.length}`);
    sem16Coms.forEach((r, i) => {
      const idLead = r[7] || '';
      const fuente = leadFuenteMap[idLead];
      console.log(`  [${i}] idLead=${JSON.stringify(idLead)} | fuente en map=${JSON.stringify(fuente)} | soc=${r[3]} | fecha=${r[1]}`);
    });
    
    // También ver leadFuenteMap sample
    console.log('\nSample leadFuenteMap (primeros 5):');
    Object.entries(leadFuenteMap).slice(0,5).forEach(([k,v]) => console.log(`  "${k}" → "${v}"`));
    
  } catch(e) {
    console.error('Error:', e.message);
  }
  process.exit(0);
}
test();
