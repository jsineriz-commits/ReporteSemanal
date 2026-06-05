// fix_q102_v2.mjs
// Lee la Q102 de Metabase (formato MBQL stages), aplica fixes de memoria y actualiza

const METABASE_URL = 'https://metabase.dcac.ar/';
const METABASE_API_KEY = 'mb_OB8cA0XB9CFF8hy4eQFGGDtClBlqtkBIaz2I70Ry0tI=';
const QUESTION_ID = 102;

const headers = {
  'Content-Type': 'application/json',
  'X-API-KEY': METABASE_API_KEY
};

async function getCard(id) {
  const res = await fetch(`${METABASE_URL}api/card/${id}`, { headers });
  if (!res.ok) throw new Error(`GET card failed: ${res.status} ${await res.text()}`);
  return res.json();
}

async function updateCard(id, payload) {
  const res = await fetch(`${METABASE_URL}api/card/${id}`, {
    method: 'PUT',
    headers,
    body: JSON.stringify(payload)
  });
  if (!res.ok) throw new Error(`PUT card failed: ${res.status} ${await res.text()}`);
  return res.json();
}

// ── APLICAR FIXES AL SQL ──────────────────────────────────────────────────
function applyFixes(sql) {
  let fixed = sql;
  let fixCount = 0;

  // ── FIX 1: Agregar lo_uniq al bloque de deduplicación ─────────────────
  // Buscamos la línea de ir_uniq y agregamos lo_uniq después
  const LO_UNIQ = `lo_uniq AS (SELECT lo_negocio, lo_tipo_liquid, lo_fecha_faena_real, lo_fecha_pago_real FROM negocios.liquidacion_oficial WHERE lo_tipo_liquid = 'interna' LIMIT 1 BY lo_negocio),`;

  if (!fixed.includes('lo_uniq')) {
    // Insertar después del último _uniq del bloque (ir_uniq)
    const IR_UNIQ_PATTERN = /ir_uniq AS \(SELECT \* FROM dcac\.informes_revisaciones LIMIT 1 BY revisacion\),/;
    if (IR_UNIQ_PATTERN.test(fixed)) {
      fixed = fixed.replace(
        IR_UNIQ_PATTERN,
        `ir_uniq AS (SELECT * FROM dcac.informes_revisaciones LIMIT 1 BY revisacion),\n${LO_UNIQ}`
      );
      console.log('✅ FIX 1: Agregado lo_uniq al bloque de deduplicación');
      fixCount++;
    } else {
      console.log('⚠️  FIX 1: No se encontró ir_uniq para insertar después. Buscando cotiz_uniq...');
      const COTIZ_PATTERN = /cotiz_uniq AS \(SELECT \* FROM negocios\.cd_cotizaciones LIMIT 1 BY lote_nro\),/;
      if (COTIZ_PATTERN.test(fixed)) {
        fixed = fixed.replace(
          COTIZ_PATTERN,
          `cotiz_uniq AS (SELECT * FROM negocios.cd_cotizaciones LIMIT 1 BY lote_nro),\n${LO_UNIQ}`
        );
        console.log('✅ FIX 1 (alt): Agregado lo_uniq después de cotiz_uniq');
        fixCount++;
      } else {
        console.log('❌ FIX 1 FAILED: No se pudo insertar lo_uniq');
      }
    }
  } else {
    console.log('⚠️  FIX 1: lo_uniq ya existe en el SQL, saltando...');
  }

  // ── FIX 2: Reemplazar JOIN directo a liquidacion_oficial en Estados_FAE ─
  // Buscamos el patrón exacto dentro de Estados_FAE
  // Patrón: tiene liq_uniq como l y liquidacion_oficial como lo, con filtro interna en WHERE
  const OLD_LO_JOIN = `LEFT JOIN negocios.liquidacion_oficial AS lo ON n.id = lo.lo_negocio\n    WHERE toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND lo.lo_tipo_liquid = 'interna'`;
  const NEW_LO_JOIN = `LEFT JOIN lo_uniq AS lo ON n.id = lo.lo_negocio\n    WHERE toString(n.borrado) != '1' AND toString(n.no_concretado) != '1'`;

  if (fixed.includes(OLD_LO_JOIN)) {
    fixed = fixed.replace(OLD_LO_JOIN, NEW_LO_JOIN);
    console.log('✅ FIX 2: Reemplazado LEFT JOIN liquidacion_oficial por lo_uniq en Estados_FAE');
    fixCount++;
  } else {
    // Intentar variante sin espacios exactos
    console.log('⚠️  FIX 2: Patrón exacto no encontrado, intentando variante...');
    // Mostrar el contexto donde aparece liquidacion_oficial
    const idx = fixed.indexOf('liquidacion_oficial AS lo ON n.id = lo.lo_negocio');
    if (idx >= 0) {
      const ctx = fixed.substring(Math.max(0, idx - 200), idx + 300);
      console.log('   Contexto encontrado:', ctx);
    } else {
      console.log('   No se encontró ninguna referencia a liquidacion_oficial AS lo');
    }
  }

  // ── FIX 3a: Acotar faena_base a 2026 ──────────────────────────────────
  const patterns3a = [
    [`toDateOrNull(toString(fecha)) >= '2024-01-01'`, `toDateOrNull(toString(fecha)) >= '2026-01-01'`],
    [`toDateOrNull(toString(fecha)) >= '2025-01-01'`, `toDateOrNull(toString(fecha)) >= '2026-01-01'`],
  ];
  
  let done3a = false;
  for (const [old, nw] of patterns3a) {
    if (fixed.includes(old)) {
      fixed = fixed.replace(old, nw);
      console.log(`✅ FIX 3a: faena_base acotado → 2026-01-01 (era: ${old.match(/'\d{4}/)?.[0]})`);
      fixCount++;
      done3a = true;
      break;
    }
  }
  if (!done3a) {
    if (fixed.includes(`toDateOrNull(toString(fecha)) >= '2026-01-01'`)) {
      console.log('✅ FIX 3a: faena_base ya está en 2026-01-01');
    } else {
      console.log('⚠️  FIX 3a: Patrón de faena_base no encontrado');
    }
  }

  // ── FIX 3b: Acotar invernada_base a 2026 ──────────────────────────────
  const patterns3b = [
    [`toDateOrNull(toString(fecha_hora)) >= '2024-01-01'`, `toDateOrNull(toString(fecha_hora)) >= '2026-01-01'`],
    [`toDateOrNull(toString(fecha_hora)) >= '2025-01-01'`, `toDateOrNull(toString(fecha_hora)) >= '2026-01-01'`],
  ];
  
  let done3b = false;
  for (const [old, nw] of patterns3b) {
    if (fixed.includes(old)) {
      fixed = fixed.replace(old, nw);
      console.log(`✅ FIX 3b: invernada_base acotado → 2026-01-01 (era: ${old.match(/'\d{4}/)?.[0]})`);
      fixCount++;
      done3b = true;
      break;
    }
  }
  if (!done3b) {
    if (fixed.includes(`toDateOrNull(toString(fecha_hora)) >= '2026-01-01'`)) {
      console.log('✅ FIX 3b: invernada_base ya está en 2026-01-01');
    } else {
      console.log('⚠️  FIX 3b: Patrón de invernada_base no encontrado');
    }
  }

  console.log(`\n📊 Total de fixes aplicados: ${fixCount}`);
  return { fixed, fixCount };
}

async function main() {
  try {
    console.log(`\n🔍 Leyendo Q${QUESTION_ID} de Metabase...`);
    const card = await getCard(QUESTION_ID);
    
    console.log(`📋 Nombre: ${card.name}`);
    console.log(`📊 query_type: ${card.query_type}`);
    
    // El SQL está en stages[0].native (formato MBQL moderno)
    const stages = card.dataset_query?.stages;
    if (!stages || !stages[0] || !stages[0].native) {
      throw new Error('No se encontró el SQL en stages[0].native');
    }

    const originalSql = stages[0].native;
    console.log(`📏 SQL original: ${originalSql.length} caracteres`);
    
    const { writeFile } = await import('fs/promises');
    await writeFile('./q102_from_metabase.sql', originalSql, 'utf8');
    console.log('💾 SQL original guardado en q102_from_metabase.sql');

    // Aplicar fixes
    console.log('\n🔧 Aplicando fixes...');
    const { fixed: fixedSql, fixCount } = applyFixes(originalSql);
    
    if (fixedSql === originalSql) {
      console.log('\n⚠️  El SQL no cambió. Revisá q102_from_metabase.sql para inspeccionar el SQL actual.');
      return;
    }

    await writeFile('./q102_fixed.sql', fixedSql, 'utf8');
    console.log('💾 SQL con fixes guardado en q102_fixed.sql');
    
    // Construir payload de actualización
    const updatedDatasetQuery = {
      ...card.dataset_query,
      stages: [
        {
          ...stages[0],
          native: fixedSql
        },
        ...stages.slice(1)
      ]
    };

    console.log(`\n📤 Actualizando Q${QUESTION_ID} en Metabase con ${fixCount} fix(es)...`);
    
    const updatedCard = await updateCard(QUESTION_ID, {
      dataset_query: updatedDatasetQuery
    });

    console.log(`\n✅ Q${QUESTION_ID} actualizada exitosamente!`);
    console.log(`   Nombre: ${updatedCard.name}`);
    console.log(`   Updated at: ${updatedCard.updated_at}`);

  } catch (err) {
    console.error('\n❌ Error:', err.message);
    process.exit(1);
  }
}

main();
