// inspect_q102.mjs — ver estructura completa del card 102

const METABASE_URL = 'https://metabase.dcac.ar/';
const METABASE_API_KEY = 'mb_OB8cA0XB9CFF8hy4eQFGGDtClBlqtkBIaz2I70Ry0tI=';

const headers = {
  'Content-Type': 'application/json',
  'X-API-KEY': METABASE_API_KEY
};

async function main() {
  const res = await fetch(`${METABASE_URL}api/card/102`, { headers });
  const card = await res.json();
  
  const { writeFile } = await import('fs/promises');
  await writeFile('./q102_card_full.json', JSON.stringify(card, null, 2), 'utf8');
  
  console.log('=== CARD 102 ESTRUCTURA ===');
  console.log('name:', card.name);
  console.log('type:', card.type);
  console.log('dataset_query keys:', Object.keys(card.dataset_query || {}));
  console.log('dataset_query.type:', card.dataset_query?.type);
  console.log('dataset_query.native:', card.dataset_query?.native ? 'EXISTS' : 'NOT FOUND');
  console.log('dataset_query.query:', card.dataset_query?.query ? 'EXISTS' : 'NOT FOUND');
  
  // Ver si es un modelo con query directo
  if (card.dataset_query) {
    console.log('\nDataset query completo:');
    console.log(JSON.stringify(card.dataset_query, null, 2).substring(0, 2000));
  }

  // Ver si hay query_type o similar
  console.log('\nquery_type:', card.query_type);
  console.log('display:', card.display);
  console.log('archived:', card.archived);
  console.log('\n✅ Estructura completa guardada en q102_card_full.json');
}

main().catch(e => console.error('Error:', e.message));
