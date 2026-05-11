// fix_q102_null.mjs
// Aplica el fix de NULL en fecha_pago_real_N dentro de cte_estados_inv (Q102).
// Problema: toString(NULL) en ClickHouse devuelve '\N', no '' ni '0000-00-00',
//           por lo que las condiciones de pago nunca matchean cuando el LEFT JOIN no tiene fila.
// Fix:      Reemplaza los checks de IN/NOT IN por versiones que manejan NULL explícitamente.
// Ejecutar: node fix_q102_null.mjs

import { readFileSync, writeFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import https from 'https';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ─── Config ──────────────────────────────────────────────────────────────────
const API_KEY   = 'mb_OB8cA0XB9CFF8hy4eQFGGDtClBlqtkBIaz2I70Ry0tI=';
const CARD_ID   = 102;
const HOST      = 'metabase.dcac.ar';
const SQL_IN    = join(__dirname, 'q102_from_metabase.sql');
const SQL_OUT   = join(__dirname, 'q102_null_fixed.sql');

// ─── Aplicar el fix de NULL ───────────────────────────────────────────────────
function applyNullFix(sql) {
  let fixed = sql;
  let count = 0;

  // Para cada número de pago (1-4) hacemos las dos sustituciones:
  //   toString(AR.fecha_pago_real_N) IN ('','0000-00-00')
  //   → (AR.fecha_pago_real_N IS NULL OR toString(AR.fecha_pago_real_N) IN ('','0000-00-00'))
  //
  //   toString(AR.fecha_pago_real_N) NOT IN ('','0000-00-00')
  //   → (AR.fecha_pago_real_N IS NOT NULL AND toString(AR.fecha_pago_real_N) NOT IN ('','0000-00-00'))

  for (const n of [1, 2, 3, 4]) {
    const field = `AR.fecha_pago_real_${n}`;

    // IN check (fecha vacía = pago NO realizado)
    const inOld  = `toString(${field}) IN ('','0000-00-00')`;
    const inNew  = `(${field} IS NULL OR toString(${field}) IN ('','0000-00-00'))`;

    // NOT IN check (fecha presente = pago realizado)
    const notOld = `toString(${field}) NOT IN ('','0000-00-00')`;
    const notNew = `(${field} IS NOT NULL AND toString(${field}) NOT IN ('','0000-00-00'))`;

    // Contar ocurrencias antes de reemplazar
    const cIn  = (fixed.split(inOld).length - 1);
    const cNot = (fixed.split(notOld).length - 1);

    // NOT IN primero (más específico) para no hacer match parcial del IN
    fixed = fixed.split(notOld).join(notNew);
    fixed = fixed.split(inOld).join(inNew);

    count += cIn + cNot;
    console.log(`  fecha_pago_real_${n}: ${cIn} IN + ${cNot} NOT IN → reemplazados`);
  }

  return { fixed, count };
}

// ─── Pushear a Metabase via API Key ──────────────────────────────────────────
function putCard(sql) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      dataset_query: {
        database: 2,
        type: 'native',
        native: { query: sql },
      },
    });

    const options = {
      hostname: HOST,
      port: 443,
      path: `/api/card/${CARD_ID}`,
      method: 'PUT',
      headers: {
        'X-API-KEY': API_KEY,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
      timeout: 120000,
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, data }));
    });

    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ─── Main ────────────────────────────────────────────────────────────────────
(async () => {
  try {
    // 1. Leer SQL original
    console.log(`📖 Leyendo ${SQL_IN}...`);
    const original = readFileSync(SQL_IN, 'utf8');
    console.log(`   ${original.split('\n').length} líneas, ${original.length} chars`);

    // 2. Aplicar fix
    console.log('\n🔧 Aplicando fix de NULL en cte_estados_inv...');
    const { fixed, count } = applyNullFix(original);

    if (count === 0) {
      console.log('\n⚠️  No se encontraron ocurrencias para reemplazar.');
      console.log('   ¿El SQL ya fue parcheado, o cambió el formato?');
      process.exit(1);
    }
    console.log(`\n✅ ${count} reemplazos totales aplicados.`);

    // 3. Guardar SQL fijo localmente
    writeFileSync(SQL_OUT, fixed, 'utf8');
    console.log(`💾 SQL corregido guardado en: q102_null_fixed.sql`);

    // 4. Mostrar diff resumido
    console.log('\n📋 Ejemplo de cambio (fecha_pago_real_1):');
    console.log('  ANTES: toString(AR.fecha_pago_real_1) IN (\'\',\'0000-00-00\')');
    console.log('  DESPUÉS: (AR.fecha_pago_real_1 IS NULL OR toString(AR.fecha_pago_real_1) IN (\'\',\'0000-00-00\'))');

    // 5. Push a Metabase
    console.log(`\n🚀 Pusheando a Metabase card/${CARD_ID}...`);
    const { status, data } = await putCard(fixed);
    console.log(`   HTTP ${status}`);

    try {
      const parsed = JSON.parse(data);
      if (parsed.name) {
        console.log(`✅ Card actualizada: "${parsed.name}"`);
      } else if (parsed.errors) {
        console.log('❌ Errores:', JSON.stringify(parsed.errors, null, 2));
      } else {
        console.log('Respuesta:', data.substring(0, 400));
      }
    } catch (e) {
      console.log('Respuesta raw:', data.substring(0, 400));
    }

    console.log('\n📌 Próximo paso: refrescá el cache del Reporte Semanal (Actualizar versión)');
    console.log('   para que se re-fetche Q102 desde Metabase con la query corregida.');

  } catch (e) {
    console.error('❌ Error:', e.message);
    process.exit(1);
  }
})();
