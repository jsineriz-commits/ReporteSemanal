#!/usr/bin/env node
// test_reporte_pdf.js
// Genera test_reporte.pdf localmente con datos simulados para visualizar el diseño.
// Uso:  node test_reporte_pdf.js
//       node test_reporte_pdf.js --open    (abre el PDF al terminar en Windows)

const path = require('path');
const fs   = require('fs');

// ── Cargar .env.local sin dependencias externas ──────────────────────────────
function loadEnvFile(p) {
  if (!fs.existsSync(p)) return;
  for (const raw of fs.readFileSync(p, 'utf8').split('\n')) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq < 0) continue;
    const key = line.slice(0, eq).trim();
    let val = line.slice(eq + 1).trim();
    if ((val.startsWith('"') && val.endsWith('"')) ||
        (val.startsWith("'") && val.endsWith("'"))) val = val.slice(1, -1);
    if (!(key in process.env)) process.env[key] = val;
  }
}
loadEnvFile(path.join(__dirname, '.env.local'));
loadEnvFile(path.join(__dirname, '.env'));

// ── Datos simulados ───────────────────────────────────────────────────────────
// Incluye todos los campos nuevos usados en el rediseño de métricas
const mockData = {
  // Cabezas ofrecidas
  cab:             175,
  cabPublicadas:    50,           // Publicadas/Ofrecidas (chip azul)
  pCab:            175,           // semana anterior → igual, chip neutro
  cabConc:          80,           // Concretadas (chip verde)
  cabNoConc:        20,           // No Concretadas (chip rojo)
  trop:              3,
  socOf:             2,
  ccc:             '80%',         // % CCC
  cotizadas:       '71%',         // % Cotizadas

  // Cabezas compradas
  cabC:              0,
  pCabC:             0,           // = vs sem ant
  cabCWeekTrop:      0,
  cabCSocCount:      0,

  // Cargas asistidas
  carg:            0,
  cargProp:        0,
  cargPrevSem:     0,           // = vs sem ant

  // SACs
  sacs:          [],            // array → length = 0
  sacsPrevSem:    0,
  sacAprob:       0,
  sacRech:        0,
  sacPend:        0,

  // CRM / Soc. Gestionadas
  tSG:                24,
  tSGPrevSem:          3,          // → +21 vs sem ant (chip verde)
  pTSG:                3,          // semana anterior
  nuevas:              0,
  pNuevas:             1,          // → -1 vs sem ant (chip rojo)
  tSGAsigSem:          0,          // asig. esta semana (gestionadas)
  socSinGestNum:       3,
  pSocSinGestNum:      4,          // → -1 vs sem ant (chip rojo)
  socSinGestAsigSem:   0,          // asig. esta semana (sin gestión)
  socSinGestAvgDays:   12,
  com:                 7,          // Comentarios
  age:                20,          // Agendas
  socOps:              0,

  // Distribución diaria [Sáb, Dom, Lun, Mar, Mié, Jue, Vie]
  dT:       [0, 0,  1,  0,  0, 0, 1],  // Tropas Ofrecidas
  dCompras: [0, 0,  1,  0,  0, 0, 0],  // Tropas Compradas  (=dCompras en la API)
  dCargas:  [0, 0,  0,  3,  0, 0, 0],  // Cargas Asistidas
  dGestion: [0, 0,  5,  5,  0, 8, 6],  // Soc. Gestionadas  (=dGestion en la API)
  dSacs:    [0, 0,  0,  0,  0, 0, 0],  // SACs enviados

  // Evolución de operaciones (últimas semanas)
  operSemMesLabels: ['S17', 'S18', 'S19', 'S20'],
  operSemMesVals:   [    0,    75,    49,   123],

  // Top negocios
  top5: [
    { q: 95,  kt: '0.3k', kv: '0.1k', ktC: '0.8k', kvC: '',    d: ['31600', 'INV VEND', 'SUCESION DE TARULLI VOLFRANCO', '', 'NICOLA JUAN WALTER',   '', '22/05/2026', 95,  'Sí', 'vend'] },
    { q: 43,  kt: '1.8k', kv: '1.1k', ktC: '',      kvC: '',    d: ['111089','FAE COMP', 'FRANCHESCA S.A.',               '', 'PATAGON FOODS SA S (J.L.T.)', '', '18/05/2026', 43,  '',   'vend'] },
    { q: 16,  kt: '0.1k', kv: '0k',   ktC: '0.8k', kvC: '',    d: ['31703', 'INV VEND', 'Marcelo Zapata',                '', 'NICOLA JUAN WALTER',   '', '20/05/2026', 16,  '',   'vend'] },
  ],

  // Detalle ofrecidas
  detOf: [
    { id: 'OF-001', fecha: '10/05/2026', soc: 'Estancia El Roble', q: 200, un: 'Tropas', est: 'CNC', kt: 'Vaca', kv: 'Novillo' },
    { id: 'OF-002', fecha: '11/05/2026', soc: 'La Pampa SRL',      q: 150, un: 'Soc',    est: 'COT', kt: 'Toro', kv: 'Ternero' },
  ],

  // CRM detalle
  actSemanal: [
    { kt: '0.4k', kv: '0.3k', soc: 'Vanzato Maria Beatriz',    fuente: 'NR',    estado: 'EN REVISION', tipo: 'Comentario', cm: 'Hable con Santiago, no nos conocia, le explique el mecanismo, quedo interesado, habia publicado 23 cut pero le vendio unas al vecino y desarmo el lote. ya hizo tacto y mando el descarte a un remate, quedo interesado estamos en contacto' },
    { kt: '0.1k', kv: '',      soc: 'Cordoba Juan Jesus',       fuente: 'NR',    estado: 'NUEVO',       tipo: 'Comentario', cm: 'cotizamos pero dio vueltas con el precio, lo saco por otro lado' },
    { kt: '',     kv: '',      soc: 'Fernando Pagano Mata',     fuente: 'NR',    estado: 'NUEVO',       tipo: 'Agenda',     cm: 'llamar' },
    { kt: '0k',   kv: '',      soc: 'El Rancho De Chascomus Sa',fuente: 'EVENTO',estado: 'EN REVISION', tipo: 'Comentario', cm: 'tarde en poder contactarme, macanudo, tiene un campo en chascomus de 450 donde hace 350ha agricultura, tiene 250 madres, ahora alquilo otro campito a 15km del de el y se quedo con casi toda la hacienda ya que se ahora bajo la carga animal. va a hacer tacto y me consulto del mag, no tiene mucho descarte pero cuando le a mandado algo a jauregui y colombo le cobraron muchos gastos. estamos en contacto quedo interesado' },
    { kt: '0k',   kv: '0k',   soc: 'Ana Toth',                 fuente: 'NR',    estado: 'NUEVO',       tipo: 'Agenda',     cm: 'mandar poca fe le veo' },
  ],
  ssgTop5: [
    { kt: '-', kv: '-', soc: 'Mauro Martin Flores',   fuente: 'NR', fa: '04-05-2026', ug: '-',           sg: '-', w: 22 },
    { kt: '-', kv: '-', soc: 'Agrop. Los Alamos SRL', fuente: 'NR', fa: '01-05-2026', ug: '10-05-2026',  sg: '-', w: 25 },
    { kt: '-', kv: '-', soc: 'Juan Perez Gutierrez',  fuente: 'NR', fa: '28-04-2026', ug: '-',           sg: '-', w: 28 },
  ],

  hideCRM: false,

  // SACs tabla
  sacsTable: [],

  // Detalle operadas y cargas
  detC:    [],
  detCarg: [],
};

// ── Generar PDF ───────────────────────────────────────────────────────────────
const Module = require('module');

// Parsear argumentos CLI
const args = process.argv.slice(2);
const isReal = args.includes('--real');
function getArg(name) {
  const idx = args.indexOf(name);
  return idx !== -1 && args[idx + 1] ? args[idx + 1] : null;
}

// Simular req/res para llamar directamente al handler
const mockRes = {
  headersSent: false,
  _status: 200,
  status(code) { this._status = code; return this; },
  json(data) {
    if (!data.ok) {
      console.error('❌ Error del handler:', data.error);
      process.exit(1);
    }
    const buf = Buffer.from(data.pdfBase64, 'base64');
    const out  = path.join(__dirname, 'test_reporte.pdf');
    fs.writeFileSync(out, buf);
    const kb = (buf.length / 1024).toFixed(1);
    console.log(`✅ PDF generado: ${out}  (${kb} KB)`);
    console.log(`   Archivo: ${data.fileName}`);

    // Abrir automáticamente si se pasó --open
    if (args.includes('--open')) {
      const { exec } = require('child_process');
      exec(`start "" "${out}"`, (err) => {
        if (err) console.warn('⚠️  No se pudo abrir automáticamente el PDF.');
      });
    }
  },
};

// Parámetros del reporte
const acArg      = getArg('--ac')     || 'ASESOR DEMO';
const semanaArg  = getArg('--semana') || '20';
const startArg   = getArg('--start');
const endArg     = getArg('--end');

// Fechas por defecto: semana 20 de 2026 — Sáb 16/05 al Vie 22/05
// Los reportes corren de Sábado a Viernes (se mandan los viernes)
const startTs = startArg ? Number(startArg) : new Date('2026-05-16T00:00:00-03:00').getTime();
const endTs   = endArg   ? Number(endArg)   : new Date('2026-05-22T23:59:59-03:00').getTime();

const mockReq = {
  method: 'POST',
  body: { ac: acArg, startTs, endTs, semana: semanaArg },
};

if (isReal) {
  // ── Modo REAL: usa la API con datos reales de la BD ──
  console.log(`🔴 Modo REAL — AC: "${acArg}"  semana: ${semanaArg}`);
  console.log(`   startTs: ${new Date(startTs).toLocaleDateString('es-AR')}  endTs: ${new Date(endTs).toLocaleDateString('es-AR')}`);
  delete require.cache[require.resolve('./api/generatePDF.js')];
  const handler = require('./api/generatePDF.js');
  handler(mockReq, mockRes).catch(err => {
    console.error('❌ Error inesperado:', err.message);
    process.exit(1);
  });
} else {
  // ── Modo MOCK: datos simulados ──
  console.log(`🟡 Modo MOCK — AC: "${acArg}"  semana: ${semanaArg}`);
  const origLoad = Module._load;
  Module._load = function (request, parent, isMain) {
    if (request.includes('_lib/logic') || request.includes('_lib\\logic')) {
      return { getReport: async () => mockData };
    }
    return origLoad.apply(this, arguments);
  };

  delete require.cache[require.resolve('./api/generatePDF.js')];
  const handler = require('./api/generatePDF.js');
  handler(mockReq, mockRes).catch(err => {
    console.error('❌ Error inesperado:', err.message);
    process.exit(1);
  });
}
