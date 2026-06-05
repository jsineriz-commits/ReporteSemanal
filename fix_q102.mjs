// fix_q102.mjs
// Lee la Q102 de Metabase, aplica los fixes de memoria y la actualiza

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

  // FIX 1: Agregar lo_uniq al bloque de deduplicación (después de ir_uniq)
  // Buscamos el fin del bloque de deduplicación
  const LO_UNIQ_BLOCK = `lo_uniq AS (
    SELECT lo_negocio, lo_tipo_liquid, lo_fecha_faena_real, lo_fecha_pago_real
    FROM negocios.liquidacion_oficial
    WHERE lo_tipo_liquid = 'interna'
    LIMIT 1 BY lo_negocio
),`;

  if (!fixed.includes('lo_uniq')) {
    // Insertar después de la línea de ir_uniq
    fixed = fixed.replace(
      /ir_uniq AS \(SELECT \* FROM dcac\.informes_revisaciones LIMIT 1 BY revisacion\),/,
      `ir_uniq AS (SELECT * FROM dcac.informes_revisaciones LIMIT 1 BY revisacion),\n${LO_UNIQ_BLOCK}`
    );
    console.log('✅ FIX 1: Agregado lo_uniq al bloque de deduplicación');
  } else {
    console.log('⚠️  FIX 1: lo_uniq ya existe, saltando...');
  }

  // FIX 2: En Estados_FAE, reemplazar el JOIN directo a liquidacion_oficial por lo_uniq
  // Cambiar: LEFT JOIN negocios.liquidacion_oficial AS lo ON n.id = lo.lo_negocio
  // Por:     LEFT JOIN lo_uniq AS lo ON n.id = lo.lo_negocio
  // Y eliminar el filtro: AND lo.lo_tipo_liquid = 'interna'
  
  // El bloque de Estados_FAE tiene este patrón:
  const OLD_JOIN_LO = `    LEFT JOIN liq_uniq AS l ON n.id = l.negocio\n    LEFT JOIN negocios.liquidacion_oficial AS lo ON n.id = lo.lo_negocio\n    WHERE toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND lo.lo_tipo_liquid = 'interna'`;
  const NEW_JOIN_LO = `    LEFT JOIN liq_uniq AS l ON n.id = l.negocio\n    LEFT JOIN lo_uniq AS lo ON n.id = lo.lo_negocio\n    WHERE toString(n.borrado) != '1' AND toString(n.no_concretado) != '1'`;

  if (fixed.includes("LEFT JOIN negocios.liquidacion_oficial AS lo ON n.id = lo.lo_negocio")) {
    fixed = fixed.replace(OLD_JOIN_LO, NEW_JOIN_LO);
    console.log('✅ FIX 2: Reemplazado JOIN directo a liquidacion_oficial por lo_uniq en Estados_FAE');
  } else {
    console.log('⚠️  FIX 2: Patrón no encontrado (puede que ya esté corregido o el SQL difiere)');
    console.log('   Buscando variante...');
    // Intentar variante más amplia
    if (fixed.includes('negocios.liquidacion_oficial AS lo')) {
      // Mostrar contexto
      const idx = fixed.indexOf('negocios.liquidacion_oficial AS lo');
      console.log('   Contexto encontrado:', fixed.substring(idx - 100, idx + 200));
    }
  }

  // FIX 3: Acotar faena_base e invernada_base a 2026 en lugar de 2024
  // Esto reduce drásticamente el volumen de datos cargados en RAM
  const OLD_FAENA_BASE = `SELECT * FROM dcac.negocios WHERE toDateOrNull(toString(fecha)) >= '2024-01-01'`;
  const NEW_FAENA_BASE = `SELECT * FROM dcac.negocios WHERE toDateOrNull(toString(fecha)) >= '2026-01-01'`;
  
  if (fixed.includes(OLD_FAENA_BASE)) {
    fixed = fixed.replace(OLD_FAENA_BASE, NEW_FAENA_BASE);
    console.log('✅ FIX 3a: faena_base acotado a 2026-01-01');
  } else if (fixed.includes(NEW_FAENA_BASE)) {
    console.log('✅ FIX 3a: faena_base ya está en 2026-01-01');
  } else {
    console.log('⚠️  FIX 3a: Patrón faena_base no encontrado exactamente');
  }

  const OLD_INV_BASE = `SELECT * FROM dcac.revisaciones WHERE toDateOrNull(toString(fecha_hora)) >= '2024-01-01'`;
  const NEW_INV_BASE = `SELECT * FROM dcac.revisaciones WHERE toDateOrNull(toString(fecha_hora)) >= '2026-01-01'`;
  
  if (fixed.includes(OLD_INV_BASE)) {
    fixed = fixed.replace(OLD_INV_BASE, NEW_INV_BASE);
    console.log('✅ FIX 3b: invernada_base acotado a 2026-01-01');
  } else if (fixed.includes(NEW_INV_BASE)) {
    console.log('✅ FIX 3b: invernada_base ya está en 2026-01-01');
  } else {
    console.log('⚠️  FIX 3b: Patrón invernada_base no encontrado exactamente');
  }

  return fixed;
}

async function main() {
  try {
    console.log(`\n🔍 Leyendo Q${QUESTION_ID} de Metabase...`);
    const card = await getCard(QUESTION_ID);
    
    console.log(`📋 Nombre: ${card.name}`);
    console.log(`📊 Tipo: ${card.dataset_query?.type}`);
    
    const queryType = card.dataset_query?.type;
    if (queryType !== 'native') {
      throw new Error(`La query ${QUESTION_ID} no es nativa (es ${queryType}). No se puede editar el SQL directamente.`);
    }

    const originalSql = card.dataset_query.native.query;
    console.log(`📏 SQL original: ${originalSql.length} caracteres`);
    
    // Guardar SQL original localmente para referencia
    const { writeFile } = await import('fs/promises');
    await writeFile('./q102_from_metabase.sql', originalSql, 'utf8');
    console.log('💾 SQL original guardado en q102_from_metabase.sql');

    // Aplicar fixes
    console.log('\n🔧 Aplicando fixes...');
    const fixedSql = applyFixes(originalSql);
    
    if (fixedSql === originalSql) {
      console.log('\n⚠️  El SQL no cambió. Los patrones no coincidieron exactamente.');
      console.log('   Revisá q102_from_metabase.sql para inspeccionar el SQL actual.');
      return;
    }

    await writeFile('./q102_fixed.sql', fixedSql, 'utf8');
    console.log('💾 SQL con fixes guardado en q102_fixed.sql');
    
    console.log(`\n📤 Actualizando Q${QUESTION_ID} en Metabase...`);
    
    const updatedCard = await updateCard(QUESTION_ID, {
      dataset_query: {
        ...card.dataset_query,
        native: {
          ...card.dataset_query.native,
          query: fixedSql
        }
      }
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
