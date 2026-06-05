// fix_final_syntax.mjs
// Corrige la sintaxis de FINAL: mueve FINAL después del alias (AS xxx FINAL)
// ClickHouse 24.x acepta: FROM tabla AS alias FINAL o FROM tabla FINAL (sin alias)

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
  const d = await res.json();
  if (d.error) throw new Error('PUT: ' + d.error);
  return d;
}

function fixFinalSyntax(sql) {
  let fixed = sql;

  // ── Patrón problemático: "tabla FINAL AS alias" → "tabla AS alias FINAL"
  // Esto afecta a los CTEs con alias (aso_comercial, repre_vinc, c5_datitos, etc.)
  // Regex: tabla FINAL AS <alias> (seguido de espacio y keyword o fin)
  // Reemplazamos: tabla FINAL AS alias → tabla AS alias FINAL
  //
  // Pero ojo: en los bloques _uniq los FROM no tienen alias (LIMIT 1 BY funciona solo)
  // y "FROM tabla FINAL LIMIT/ORDER/WHERE" ya es sintaxis correcta.
  //
  // Solo el patrón "TABLA FINAL AS alias" es el problemático.

  let count = 0;
  
  // Patrón: word.word FINAL AS alias_word
  // Reemplazar por: word.word AS alias_word FINAL
  const badPattern = /(\w+\.\w+)\s+FINAL\s+AS\s+(\w+)/g;
  
  fixed = fixed.replace(badPattern, (match, table, alias) => {
    count++;
    return `${table} AS ${alias} FINAL`;
  });

  console.log(`🔧 Patrones "TABLA FINAL AS alias" corregidos: ${count}`);
  return { fixed, count };
}

async function main() {
  console.log(`\n🔍 Leyendo Q${QUESTION_ID} de Metabase (versión con FINAL roto)...`);
  const card = await getCard(QUESTION_ID);
  const stages = card.dataset_query?.stages;
  if (!stages?.[0]?.native) throw new Error('No SQL en stages[0].native');

  const currentSql = stages[0].native;
  console.log(`📏 SQL actual: ${currentSql.length} chars`);

  // Verificar cuántos FINAL hay actualmente
  const currentFinals = (currentSql.match(/\bFINAL\b/g) || []).length;
  console.log(`🔢 FINAL actuales: ${currentFinals}`);

  // Mostrar los problemáticos antes de corregir
  const badMatches = [...currentSql.matchAll(/\w+\.\w+\s+FINAL\s+AS\s+\w+/g)];
  console.log(`\n⚠️  Patrones "TABLA FINAL AS alias" a corregir: ${badMatches.length}`);
  badMatches.forEach(m => console.log(`   • "${m[0]}"`));

  console.log('\n🔧 Aplicando corrección de sintaxis...');
  const { fixed, count } = fixFinalSyntax(currentSql);

  if (fixed === currentSql) {
    console.log('⚠️  No hubo cambios.');
    return;
  }

  // Verificar resultado
  const goodFinals = (fixed.match(/\bFINAL\b/g) || []).length;
  const remainingBad = [...fixed.matchAll(/\w+\.\w+\s+FINAL\s+AS\s+\w+/g)];
  console.log(`\n📊 FINAL después de fix: ${goodFinals} (misma cantidad, solo reubicados)`);
  console.log(`✅ Patrones problemáticos restantes: ${remainingBad.length}`);

  // Mostrar muestra de cómo quedó
  console.log('\n📋 Muestra de FINAL corregidos (primeros 5):');
  const corrected = [...fixed.matchAll(/\w+\.\w+\s+AS\s+\w+\s+FINAL/g)];
  corrected.slice(0, 5).forEach(m => console.log(`   ✅ "${m[0]}"`));

  const { writeFile } = await import('fs/promises');
  await writeFile('./q102_final_fixed.sql', fixed, 'utf8');
  console.log('\n💾 SQL corregido guardado en q102_final_fixed.sql');

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
