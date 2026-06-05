// apply_final_q102.mjs
// Aplica FINAL a todas las tablas que lo soportan en Q102 y sube a Metabase

const METABASE_URL = 'https://metabase.dcac.ar/';
const METABASE_API_KEY = 'mb_OB8cA0XB9CFF8hy4eQFGGDtClBlqtkBIaz2I70Ry0tI=';
const QUESTION_ID = 102;
const headers = { 'Content-Type': 'application/json', 'X-API-KEY': METABASE_API_KEY };

async function getCard(id) {
  const res = await fetch(`${METABASE_URL}api/card/${id}`, { headers });
  return res.json();
}
async function updateCard(id, payload) {
  const res = await fetch(`${METABASE_URL}api/card/${id}`, {
    method: 'PUT', headers, body: JSON.stringify(payload)
  });
  return res.json();
}

// Tablas confirmadas que soportan FINAL (todas las de Q102 menos las 2 con timeout)
// negocios.liquidaciones y dcac.detalles_carga también se incluyen porque
// el timeout fue por la red, no porque no soporten FINAL
const FINAL_TABLES = [
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

function applyFinal(sql) {
  let fixed = sql;
  let count = 0;

  for (const table of FINAL_TABLES) {
    // Patrón: FROM tabla (con posible WHERE, LIMIT, ORDER BY, AS alias, JOIN, o fin de CTE)
    // Necesitamos insertar FINAL justo después del nombre de tabla, antes del siguiente keyword
    // Casos:
    //   FROM dcac.negocios WHERE ...           →  FROM dcac.negocios FINAL WHERE ...
    //   FROM dcac.negocios AS n LEFT JOIN ...  →  FROM dcac.negocios FINAL AS n LEFT JOIN ...
    //   FROM dcac.negocios ORDER BY ...        →  FROM dcac.negocios FINAL ORDER BY ...
    //   FROM dcac.negocios LIMIT ...           →  FROM dcac.negocios FINAL LIMIT ...
    //   SELECT * FROM dcac.negocios)           →  SELECT * FROM dcac.negocios FINAL)
    //   FROM dcac.negocios\n    LEFT JOIN      →  FROM dcac.negocios FINAL\n    LEFT JOIN
    
    // Regex: nombre de tabla seguido de espacio+keyword O fin de línea O paréntesis
    const escaped = table.replace('.', '\\.');
    const pattern = new RegExp(
      `FROM\\s+${escaped}(?!\\s+FINAL)(?=\\s+(WHERE|ORDER|LIMIT|LEFT|INNER|RIGHT|JOIN|AS|ON|GROUP|HAVING|UNION|\\)))`,
      'gi'
    );
    
    const before = fixed;
    fixed = fixed.replace(pattern, (match) => {
      return match.replace(new RegExp(`(FROM\\s+${escaped})`, 'i'), '$1 FINAL');
    });

    // También cubrir el caso: FROM tabla\n (fin de línea seguido de newline antes de next keyword)
    const pattern2 = new RegExp(
      `FROM\\s+${escaped}(?!\\s+FINAL)(\\s*\\n)`,
      'gi'
    );
    fixed = fixed.replace(pattern2, (match, nl) => {
      return match.replace(new RegExp(`(FROM\\s+${escaped})`, 'i'), '$1 FINAL');
    });

    if (fixed !== before) {
      // Contar cuantas veces se aplicó
      const occurrences = (before.match(new RegExp(`FROM\\s+${escaped}`, 'gi')) || []).length;
      console.log(`✅ FINAL aplicado en ${table} (${occurrences} ocurrencia/s)`);
      count++;
    } else {
      // Verificar si ya tenía FINAL
      if (new RegExp(`FROM\\s+${escaped}\\s+FINAL`, 'i').test(fixed)) {
        console.log(`⚪ ${table} ya tiene FINAL`);
      } else {
        // Verificar si la tabla aparece en el SQL (puede ser solo en JOINs)
        if (new RegExp(escaped, 'i').test(fixed)) {
          console.log(`⚠️  ${table} encontrada pero patrón FROM no matcheó (revisar manualmente)`);
          // Debug: mostrar contextos
          const allMatches = [...fixed.matchAll(new RegExp(`FROM\\s+${escaped}[^\\n]*`, 'gi'))];
          allMatches.forEach(m => console.log(`   Contexto: "${m[0].substring(0, 100)}"`));
        } else {
          console.log(`⚪ ${table} no está en Q102 como FROM directo`);
        }
      }
    }
  }
  
  return { fixed, count };
}

async function main() {
  console.log(`\n🔍 Leyendo Q${QUESTION_ID} de Metabase...`);
  const card = await getCard(QUESTION_ID);
  const stages = card.dataset_query?.stages;
  if (!stages?.[0]?.native) throw new Error('No se encontró SQL en stages[0].native');
  
  const originalSql = stages[0].native;
  console.log(`📏 SQL actual: ${originalSql.length} chars\n`);

  // Guardar versión antes del FINAL
  const { writeFile } = await import('fs/promises');
  await writeFile('./q102_prefinal.sql', originalSql, 'utf8');

  console.log('🔧 Aplicando FINAL...\n');
  const { fixed, count } = applyFinal(originalSql);

  if (fixed === originalSql) {
    console.log('\n⚠️  No hubo cambios. Posiblemente los patrones no matchearon.');
    return;
  }

  await writeFile('./q102_with_final.sql', fixed, 'utf8');
  console.log(`\n💾 SQL con FINAL guardado en q102_with_final.sql`);
  console.log(`📊 Tablas modificadas: ${count}`);

  // Verificar que el FINAL quedó bien
  const finalCount = (fixed.match(/\bFINAL\b/g) || []).length;
  // Restar los 3 que son comentarios de sección
  const realFinals = finalCount - 3;
  console.log(`🔢 FINAL keywords en SQL resultante: ${realFinals} (${finalCount} total incl. comentarios)`);

  console.log(`\n📤 Actualizando Q${QUESTION_ID} en Metabase...`);
  const updated = await updateCard(QUESTION_ID, {
    dataset_query: {
      ...card.dataset_query,
      stages: [{ ...stages[0], native: fixed }, ...stages.slice(1)]
    }
  });

  console.log(`\n✅ Q${QUESTION_ID} actualizada!`);
  console.log(`   Updated at: ${updated.updated_at}`);
}

main().catch(e => { console.error('\n❌ Error:', e.message); process.exit(1); });
