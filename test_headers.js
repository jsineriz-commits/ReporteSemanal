const { getSheetData } = require('./api/_lib/sheets');

async function test() {
  const baseRaw = await getSheetData('BASE');
  if (baseRaw && baseRaw[0]) {
    console.log("BASE Headers:");
    baseRaw[0].forEach((h, i) => console.log(`${i}: ${h}`));
  } else {
    console.log("No headers found in BASE");
  }
}

test();
