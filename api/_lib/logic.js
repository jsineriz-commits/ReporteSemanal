// api/_lib/logic.js
// Traducción completa de Code.gs (Reportes Semanales v7.1) a Node.js.
// Mantiene la lógica idéntica al original.

const { getSheetData, g } = require('./sheets');
const cache = require('./cache');
const props = require('./props');

// ─── Helpers de fecha ─────────────────────────────────────────────────────────
// Estrategia: los seriales de Sheets → UTC noon (+12h) evita cruzar medianoche UTC.
// Todos los formatos se calculan en UTC (consistente con la conversión de seriales).

function parseSheetDate(val) {
  if (val === null || val === undefined || val === '') return null;
  if (val instanceof Date) return val;
  if (typeof val === 'number') {
    if (val <= 0 || val > 2958466) return null;
    return new Date((val - 25569) * 86400000 + 43200000); // UTC noon
  }
  if (typeof val === 'string') {
    const d = new Date(val);
    return isNaN(d.getTime()) ? null : d;
  }
  return null;
}

// yyyyMMdd en UTC
function toDateStr(val) {
  const d = parseSheetDate(val);
  if (!d) return '';
  const y  = d.getUTCFullYear();
  const m  = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${y}${m}${dd}`;
}

// dd/MM/yyyy en UTC
function toFmt(val) {
  const d = parseSheetDate(val);
  if (!d) return '';
  const y  = d.getUTCFullYear();
  const m  = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${dd}/${m}/${y}`;
}

// Índice de día semanal: Sab=0, Dom=1, Lun=2, Mar=3, Mie=4, Jue=5, Vie=6
function toDayIdx(val) {
  const d = parseSheetDate(val);
  if (!d) return -1;
  return (d.getUTCDay() + 1) % 7;
}

// Formatea Date como yyyyMMdd en UTC (para cómputo de rangos en getReport)
function fmtDateUTC(d) {
  if (!(d instanceof Date) || isNaN(d.getTime())) return '';
  const y  = d.getUTCFullYear();
  const m  = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${y}${m}${dd}`;
}

// ─── norm ─────────────────────────────────────────────────────────────────────
// Idéntica al original — normaliza nombres para comparación.
function norm(s) {
  if (!s) return '';
  return String(s).trim().toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[áà]/g, 'a').replace(/[éè]/g, 'e')
    .replace(/[íì]/g, 'i').replace(/[óò]/g, 'o')
    .replace(/[úù]/g, 'u').replace(/ñ/g, 'n');
}

// ─── getReportCacheVersion ────────────────────────────────────────────────────
function getReportCacheVersion() {
  return 'crm-act-v9_' + props.getR12Ver();
}

// ─── clearCache ───────────────────────────────────────────────────────────────
function clearCache() {
  cache.flush();        // limpia toda la caché en memoria
  props.resetR12Ver();  // nueva versión → invalida claves de reportes
  return 'Cache limpiado';
}

// ─── getConfig ────────────────────────────────────────────────────────────────
async function getConfig() {
  const cached = cache.get('config');
  if (cached) return cached;

  const aux = (await getSheetData('aux')).slice(1);
  const acMap = {}, repsList = [], acsList = [], semanas = [];
  const seenSem = {};

  aux.forEach((row, idx) => {
    const nombre = String(g(row, 14) || '').trim();
    const mail   = String(g(row, 20) || '').trim().toLowerCase();

    if (nombre && mail) {
      acMap[nombre] = mail;
      if (idx >= 29) {
        if (!repsList.includes(nombre)) repsList.push(nombre);
      } else {
        if (!acsList.includes(nombre)) acsList.push(nombre);
      }
    }

    // semanas: col I(8)=n, col K(10)=start, col L(11)=end, col M(12)=year
    const n = g(row, 8), s = g(row, 10), e = g(row, 11), y = g(row, 12);
    if (n && s && e && !seenSem[n]) {
      seenSem[n] = true;
      const dS = parseSheetDate(s);
      const dE = parseSheetDate(e);
      if (dS && dE) {
        semanas.push({ n, s: dS.getTime(), e: dE.getTime(), y });
      }
    }
  });

  const cfg = { acs: acsList.sort(), reps: repsList.sort(), acMap, semanas };
  cache.set('config', cfg, cache.TTL.CONFIG);
  return cfg;
}

// ─── loadData ─────────────────────────────────────────────────────────────────
// Lee las 9 hojas en paralelo y construye los arrays de datos.
async function loadData() {
  const cached = cache.get('data');
  if (cached) return cached;

  console.log('[logic] loadData: cache miss, leyendo 9 hojas en paralelo...');

  const [
    baseRaw, opsRaw, comsRaw, agendasRaw,
    leadsRaw, auxLeadsRaw, sacsRaw, rematesRaw, bcfullRaw,
  ] = await Promise.all([
    getSheetData('BASE'),
    getSheetData('OPS'),
    getSheetData('Comentarios_CRM'),
    getSheetData('Agenda_CRM'),
    getSheetData('Leads_CRM'),
    getSheetData('aux leads'),
    getSheetData('SAC'),
    getSheetData('REMATES'),
    getSheetData('BCFULL'),
  ]);

  // ── BASE ──
  const base = [];
  baseRaw.slice(1).forEach(row => {
    const ac      = norm(g(row, 5));
    const repVend = norm(g(row, 20));
    const repComp = norm(g(row, 21));
    if (!ac && !repVend && !repComp) return;
    const f = toDateStr(g(row, 1)); if (!f) return;
    const est = String(g(row, 3) || '').trim().toUpperCase();
    let conc = false, pub = false, ofr = false, noConc = false;
    const cotizo = Number(g(row, 6)) === 1 ? 1 : 0;
    if      (est === 'CONCRETADA')                            { conc  = true; }
    else if (est === 'PUBLICADO')                             { pub   = true; }
    else if (est === 'OFRECIMIENTOS')                         { ofr   = true; }
    else if (est === 'NO CONCRETADA' || est === 'NO CONCRETADAS') { noConc = true; }
    else return;
    base.push([
      ac,                           // 0 ac
      f,                            // 1 f
      toDayIdx(g(row, 1)),          // 2 di
      g(row, 2) || '',              // 3 soc
      Number(g(row, 4)) || 0,       // 4 cab
      conc ? 1 : 0,                 // 5 conc
      pub  ? 1 : 0,                 // 6 pub
      ofr  ? 1 : 0,                 // 7 ofr
      noConc ? 1 : 0,               // 8 noConc
      cotizo,                       // 9 cotizo
      String(g(row, 0) || ''),      // 10 id (col A)
      String(g(row, 16) || ''),     // 11 CUIT (col Q)
      String(g(row, 7) || ''),      // 12 UN (col H)
      toFmt(g(row, 1)),             // 13 fmtFecha
      repVend,                      // 14 rep vend (col U)
      repComp,                      // 15 rep comp (col V)
    ]);
  });

  // ── OPS ──
  const ops = [];
  opsRaw.slice(1).forEach(row => {
    const aV = norm(g(row, 6)), aC = norm(g(row, 8));
    const rV = norm(g(row, 34)), rC = norm(g(row, 35));
    if (!aV && !aC && !rV && !rC) return;
    const f = toDateStr(g(row, 2)); if (!f) return;
    const cargAcRaw = String(g(row, 22) || '').trim();
    const cargF     = g(row, 18) ? toDateStr(g(row, 18)) : '';
    ops.push([
      aV, aC, f, toDayIdx(g(row, 2)),
      Number(g(row, 9))  || 0,    // 4 Q total (col J)
      g(row, 5) || '',             // 5 socV (col F)
      g(row, 7) || '',             // 6 socC (col H)
      toFmt(g(row, 2)),            // 7 fmtFecha
      String(g(row, 0) || ''),     // 8 ID (col A)
      String(g(row, 1) || ''),     // 9 UN (col B)
      String(g(row, 10) || ''),    // 10 Cat (col K)
      norm(cargAcRaw),             // 11 ac carga normalizado
      cargF,                       // 12 fecha carga (col S)
      cargF ? toDayIdx(g(row, 18)) : -1, // 13 di carga
      String(g(row, 21) || ''),    // 14 cuitV (col V)
      String(g(row, 22) || ''),    // 15 cuitC (col W)
      cargAcRaw.toLowerCase(),     // 16 ac carga lowercase
      Number(g(row, 16)) || 0,     // 17 Q particular (col Q)
      rV,                          // 18 repV (col AI)
      rC,                          // 19 repC (col AJ)
    ]);
  });

  // ── COMENTARIOS CRM ──
  // col A(0)=idLead, B(1)=soc, C(2)=mail AC, D(3)=fecha, E(4)=comentario
  const coms = [];
  comsRaw.slice(1).forEach(row => {
    const mail = String(g(row, 2) || '').trim().toLowerCase(); if (!mail) return;
    const f    = toDateStr(g(row, 3)); if (!f) return;
    coms.push([
      mail, f, toDayIdx(g(row, 3)),
      g(row, 1) || '',    // 3 soc
      1,                  // 4 esCom
      'Comentario',       // 5 tipo
      g(row, 4) || '',    // 6 comentario
      String(g(row, 0) || ''), // 7 idLead
    ]);
  });

  // ── AGENDA CRM ──
  // col A(0)=idLead, B(1)=soc, C(2)=mail, D(3)=comentario, F(5)=fecha agenda
  const agendas = [];
  agendasRaw.slice(1).forEach(row => {
    const mail = String(g(row, 2) || '').trim().toLowerCase(); if (!mail) return;
    const f    = toDateStr(g(row, 5)); if (!f) return;
    agendas.push([
      mail, f, toDayIdx(g(row, 5)),
      g(row, 1) || '',    // 3 soc
      g(row, 3) || '',    // 4 comentario
      'Agenda',           // 5 tipo
      String(g(row, 0) || ''), // 6 idLead
    ]);
  });

  // ── LEADS CRM ──
  // col B(1)=fechaAsig, C(2)=mail, D(3)=fuente, L(11)=estado
  const leads = [];
  leadsRaw.slice(1).forEach(row => {
    const mail = String(g(row, 2) || '').trim().toLowerCase(); if (!mail) return;
    if (String(g(row, 3) || '').trim() !== 'UA') return;
    if (String(g(row, 11) || '').trim() === 'NO HABILITADO') return;
    const f = toDateStr(g(row, 1)); if (!f) return;
    leads.push([mail, f]);
  });

  // ── AUX LEADS ──
  // col A(0)=idLead, B(1)=mail, C(2)=fechaAsig, E(4)=estado,
  // W(22)=diasEstadoActual, AA(26)=cuit, AD(29)=razonSocial,
  // AF(31)=kt, AK(36)=kv, AL(37)=fuente, AM(38)=sinGestion,
  // AN(39)=ultActividad, AO(40)=ultGestion, AP(41)=comentario
  const auxLeads = [];
  auxLeadsRaw.slice(1).forEach(row => {
    const mail = String(g(row, 1) || '').trim().toLowerCase(); if (!mail) return;
    const est  = String(g(row, 4) || '').trim().toUpperCase();
    const f    = toDateStr(g(row, 2));
    auxLeads.push([
      mail,                                   // 0 mail
      f,                                      // 1 fechaAsig
      f ? toDayIdx(g(row, 2)) : -1,           // 2 dayIdx
      est === 'NUEVO' ? 1 : 0,                // 3 esNuevo
      String(g(row, 31) || ''),               // 4 kt (col AF)
      String(g(row, 36) || ''),               // 5 kv (col AK)
      String(g(row, 29) || ''),               // 6 razonSocial (col AD)
      String(g(row, 38) || ''),               // 7 sinGestion (col AM)
      Number(g(row, 22)) || 0,               // 8 diasEstadoActual (col W)
      Number(g(row, 32)) || 0,               // 9 ng
      String(g(row, 40) || ''),              // 10 ultGestion (col AO)
      String(g(row, 39) || ''),              // 11 ultActividad (col AN)
      String(g(row, 26) || ''),              // 12 cuit (col AA)
      String(g(row, 37) || ''),              // 13 fuente (col AL)
      String(g(row, 41) || ''),              // 14 comentario (col AP)
      est,                                   // 15 estado raw
      String(g(row, 0) || ''),               // 16 idLead (col A)
    ]);
  });

  // ── SAC ──
  // col Q(16)=soc, T(19)=fecha, U(20)=jdSol, W(22)=jdApro,
  // X(23)=UN, Y(24)=estado, AC(28)=acNorm
  const sacs = [];
  sacsRaw.slice(1).forEach(row => {
    const ac = norm(g(row, 28)); if (!ac) return;
    const f  = toDateStr(g(row, 19)); if (!f) return;
    sacs.push([
      ac,                         // 0 acNorm
      f,                          // 1 fecha
      toDayIdx(g(row, 19)),       // 2 dayIdx
      String(g(row, 16) || ''),   // 3 soc
      String(g(row, 24) || ''),   // 4 estado
      String(g(row, 20) || ''),   // 5 jdSol
      String(g(row, 22) || ''),   // 6 jdApro
      String(g(row, 23) || ''),   // 7 UN
    ]);
  });

  // ── REMATES ──
  const remates = [];
  rematesRaw.slice(1).forEach(row => {
    const ac = norm(g(row, 2)); if (!ac) return;
    const f  = toDateStr(g(row, 1)); if (!f) return;
    remates.push([ac, f, String(g(row, 3) || Math.random())]);
  });

  // ── BCFULL ──
  const bcfull = [];
  bcfullRaw.slice(1).forEach(row => {
    const cuit = String(g(row, 1) || '').trim(); if (!cuit) return;
    bcfull.push([cuit, String(g(row, 3) || ''), String(g(row, 4) || '')]);
  });

  const data = { base, ops, coms, agendas, leads, auxLeads, sacs, remates, bcfull };
  cache.set('data', data, cache.TTL.DATA);
  console.log(`[logic] loadData: base=${base.length} ops=${ops.length} auxLeads=${auxLeads.length} completado.`);
  return data;
}

// ─── warmup ──────────────────────────────────────────────────────────────────
async function warmup() {
  await getConfig();
  await loadData();
  return { ok: true };
}

// ─── scheduledWarmup (equivale al trigger horario de Apps Script) ─────────────
async function scheduledWarmup() {
  console.log('[logic] scheduledWarmup: iniciando actualización de caché...');
  clearCache();
  await warmup();
  console.log('[logic] scheduledWarmup: caché actualizada.');
  return { ok: true };
}

// ─── debugCacheStatus ─────────────────────────────────────────────────────────
function debugCacheStatus(ac, startTs, endTs) {
  const cfgCached  = cache.get('config');
  const dataCached = cache.get('data');
  let rKey = '', rHit = false;
  if (ac && startTs && endTs && cfgCached) {
    const acMail = cfgCached.acMap[ac] || '';
    const ver    = getReportCacheVersion();
    if (acMail) {
      rKey = `R12_${ver}_${acMail.replace(/[@.]/g, '_')}_${startTs}_${endTs}`;
      rHit = !!cache.get(rKey);
    }
  }
  return {
    reportCacheVersion: getReportCacheVersion(),
    hasCFG8:      !!cfgCached,
    hasDATA10:    !!dataCached,
    data10Chunks: '1',
    ac: ac || '',
    acMail: cfgCached ? (cfgCached.acMap[ac] || '') : '',
    reportKey: rKey,
    reportHit: rHit,
  };
}

// ─── refreshCacheAndWarmup ───────────────────────────────────────────────────
async function refreshCacheAndWarmup(ac, startTs, endTs) {
  const clearMsg = clearCache();
  const warm     = await warmup();
  const status   = debugCacheStatus(ac, startTs, endTs);
  return { ok: true, clear: clearMsg, warmup: warm, status };
}

// ─── getReport ────────────────────────────────────────────────────────────────
// Equivale EXACTAMENTE a getReport() del .gs original.
// Traducción línea a línea con mínimos cambios para async/await.
async function getReport(ac, startTs, endTs, opts) {
  opts = opts || {};
  const skipPrevLookup = !!opts.skipPrevLookup;

  const cfg = await getConfig();
  const acMail = cfg.acMap[ac];
  if (!acMail) return { error: 'AC no encontrado: ' + ac };

  const acN = norm(ac);
  const ver  = getReportCacheVersion();
  const rMode = skipPrevLookup ? '_raw' : '';
  const rKey  = `R12_${ver}_${acMail.replace(/[@.]/g, '_')}_${startTs}_${endTs}${rMode}`;

  const ssgSanitized2 = acMail.replace(/[^a-zA-Z0-9_]/g, '_');
  const ssgStoreKey2  = `SSGN_${ssgSanitized2}_${startTs}`;
  const ssgPrevKey2   = `SSGN_${ssgSanitized2}_${startTs - 604800000}`;

  // ── Cache hit ──
  const hit = cache.get(rKey);
  if (hit) {
    const result = { ...hit };
    const storedPrev = props.getProp(ssgPrevKey2);
    if (storedPrev !== null) result.pSocSinGestNum = parseInt(storedPrev, 10) || 0;
    return result;
  }

  const D = await loadData();

  // ── Rangos de fechas (UTC) ──
  const d0 = new Date(startTs);
  const d1 = new Date(endTs);
  const ini   = fmtDateUTC(d0);
  const fin   = fmtDateUTC(d1);
  const ini_  = fmtDateUTC(new Date(startTs - 7 * 86400000));
  const fin_  = fmtDateUTC(new Date(endTs   - 7 * 86400000));

  // Inicio de mes de d1 (UTC noon)
  const m0 = new Date(Date.UTC(d1.getUTCFullYear(), d1.getUTCMonth(), 1, 12));
  const iniM = fmtDateUTC(m0);

  // Mes anterior (mismo corte de día)
  const prevMonthStart = new Date(Date.UTC(m0.getUTCFullYear(), m0.getUTCMonth() - 1, 1, 12));
  const prevMonthEnd   = new Date(Date.UTC(m0.getUTCFullYear(), m0.getUTCMonth(), 0, 12)); // día 0 = último del mes anterior
  const prevCutDay     = Math.min(d1.getUTCDate(), prevMonthEnd.getUTCDate());
  const prevMonthCut   = new Date(Date.UTC(prevMonthStart.getUTCFullYear(), prevMonthStart.getUTCMonth(), prevCutDay, 12));
  const iniMPrev = fmtDateUTC(prevMonthStart);
  const finMPrev = fmtDateUTC(prevMonthCut);

  // Últimas 4 semanas (evolución de operaciones)
  let ult4Sem = (cfg.semanas || []).filter(s => {
    if (!s || !s.s || !s.e) return false;
    return new Date(s.e).getTime() <= d1.getTime();
  }).sort((a, b) => a.s - b.s);
  if (ult4Sem.length > 4) ult4Sem = ult4Sem.slice(ult4Sem.length - 4);

  const semMes = ult4Sem.map(s => ({
    label: 'S' + s.n,
    ini:   fmtDateUTC(new Date(s.s)),
    fin:   fmtDateUTC(new Date(s.e)),
  }));

  // Última semana del mes anterior
  const semPrevMes = (cfg.semanas || []).filter(s => {
    if (!s || !s.s || !s.e) return false;
    const we = new Date(s.e);
    return we.getUTCFullYear() === prevMonthStart.getUTCFullYear() &&
           we.getUTCMonth()    === prevMonthStart.getUTCMonth();
  }).sort((a, b) => a.e - b.e);
  const ultSemPrevMes = semPrevMes.length ? semPrevMes[semPrevMes.length - 1] : null;
  let semPrevIni = '', semPrevFin = '';
  if (ultSemPrevMes) {
    semPrevIni = fmtDateUTC(new Date(ultSemPrevMes.s));
    semPrevFin = fmtDateUTC(new Date(ultSemPrevMes.e));
  }

  const inS     = f => f >= ini    && f <= fin;
  const inA     = f => f >= ini_   && f <= fin_;
  const inM     = f => f >= iniM   && f <= fin;
  const inMPrev = f => f >= iniMPrev && f <= finMPrev;

  // ── Resultado ──
  const r = {
    cab: 0, trop: 0, pCab: 0, pTrop: 0, cccNum: 0, cccDen: 0, cabPublicadas: 0,
    cabCompra: 0, pCabCompra: 0, tropCompra: 0, socCompra: 0,
    dT: [0,0,0,0,0,0,0], dCompras: [0,0,0,0,0,0,0],
    cabC: 0, cabCWeekTrop: 0, cabCSocCount: 0, pCabC: 0,
    cabV: 0, cabConc: 0, trConc: 0, pConc: 0,
    cabOperMtd: 0, pCabOperMtd: 0, cabVOperMtd: 0, cabCOperMtd: 0,
    carg: 0, cargProp: 0, cargAjen: 0, dCargas: [0,0,0,0,0,0,0],
    com: 0, age: 0, tSG: 0, pTSG: 0, dGestion: [0,0,0,0,0,0,0],
    nuevas: 0, pNuevas: 0, nuevasFuentes: {}, socSinGestNum: 0, pSocSinGestNum: 0, socSinGestAsigSem: 0,
    sacs: [], sacsTable: [], pSac: 0, sacAprob: 0, sacRech: 0, sacPend: 0,
    rem: 0, pRem: 0, dSacs: [0,0,0,0,0,0,0],
    top5: [], ssgTop5: [], actSemanal: [],
    detOf: [], detC: [], detCarg: [],
    operSemMesLabels: [], operSemMesVals: [], operSemMesDets: [], prevSemOperBase: 0,
    socOf: 0, socOps: 0, ccc: '0%', cotizadas: '0%',
    tSGAsigSem: 0, socSinGestAvgDays: 0, hideCRM: false,
    rankingOfrecidas: [], rankingCompradas: [], rankingOperadas: [],
  };

  // Detectar si hay datos CRM para este AC
  let hasCrmData = D.auxLeads.some(row => row[0] === acMail) ||
                   D.coms.some(row => row[0] === acMail)     ||
                   D.agendas.some(row => row[0] === acMail)  ||
                   D.leads.some(row => row[0] === acMail);
  r.hideCRM = !hasCrmData;

  r.operSemMesLabels = semMes.map(s => s.label);
  r.operSemMesVals   = semMes.map(() => 0);
  r.operSemMesDets   = semMes.map(() => []);

  // ── Mapa CUIT → Kt, Kv ──
  const cuitKtKvMap = {};
  // 1. Prioridad: aux leads (por CUIT en col AA = idx 12 del array procesado)
  for (const row of D.auxLeads) {
    const cuit = String(row[12] || '').trim();
    if (cuit && !cuitKtKvMap[cuit]) cuitKtKvMap[cuit] = { kt: row[4], kv: row[5] || '-' };
  }
  // 2. Complemento: BCFULL
  for (const row of D.bcfull) {
    const cuit = String(row[0] || '').trim();
    if (cuit && !cuitKtKvMap[cuit]) cuitKtKvMap[cuit] = { kt: row[1], kv: row[2] || '-' };
  }

  // ── BASE ──
  const socOf = {};
  let cabConcBase = 0, cabNoConcBase = 0, cabCotizadasCcc = 0;
  const seenBaseId = {};

  for (let i = 0; i < D.base.length; i++) {
    const row = D.base[i];
    if (row[0] !== acN && row[14] !== acN && row[15] !== acN) continue;
    const baseId = String(row[10] || '').trim();
    const bKey   = baseId ? baseId : `b_idx_${i}`;
    if (seenBaseId[bKey]) continue;
    seenBaseId[bKey] = true;

    const f = row[1];
    if (inS(f)) {
      r.cab    += row[4]; r.trop++;
      if (row[5]) cabConcBase  += row[4];
      if (row[8]) cabNoConcBase += row[4];
      if (row[6] || row[7]) r.cabPublicadas += row[4];
      if ((row[5] || row[8]) && row[9]) cabCotizadasCcc += row[4];
      if (row[3]) socOf[row[3]] = 1;
      if (row[2] >= 0) r.dT[row[2]]++;

      const cuitOf = String(row[11] || '').trim();
      const dataOf = cuitKtKvMap[cuitOf] || { kt: '-', kv: '-' };
      const fStr   = String(f || '');
      const fFmt   = fStr.length === 8
        ? `${fStr.slice(6,8)}/${fStr.slice(4,6)}/${fStr.slice(0,4)}`
        : fStr;

      r.detOf.push({
        id:    row[10] || '',
        fecha: row[13] ? String(row[13]) : fFmt,
        soc:   row[3]  || '-',
        q:     row[4]  || 0,
        un:    row[12] || '-',
        kt:    dataOf.kt,
        kv:    dataOf.kv,
        est:   row[5] ? 'C' : (row[8] ? 'NC' : (row[6] ? 'P' : (row[7] ? 'O' : '-'))),
        cot:   row[9] || 0,
      });
    }
    if (inA(f)) { r.pCab += row[4]; r.pTrop++; }
  }
  r.socOf = Object.keys(socOf).length;

  // ── OPS ──
  const socOps = {}, socCompraWeek = {}, allOps = [];
  const seenS = {}, seenA = {}, seenM = {}, seenMPrev = {};
  const seenSemMes = semMes.map(() => ({}));
  const seenPrevSem = {}, seenCWeek = {}, seenOpsId = {};

  for (let i = 0; i < D.ops.length; i++) {
    const row   = D.ops[i];
    const isV   = (row[0] === acN) || (row[18] === acN);
    const isC   = (row[1] === acN) || (row[19] === acN);
    const isCargForAc = row[11] === acN || row[16] === acMail;

    if (!isV && !isC && !isCargForAc) continue;
    const opIdStr = String(row[8] || '').trim();
    const oKey    = opIdStr ? opIdStr : `o_idx_${i}`;
    if (seenOpsId[oKey]) continue;
    seenOpsId[oKey] = true;

    // Cargas
    if (isCargForAc && row[12] && inS(row[12])) {
      r.carg++;
      if (isV) r.cargProp++; else r.cargAjen++;
      if (row[13] >= 0) r.dCargas[row[13]]++;

      const fCS  = String(row[12] || '');
      const fCFmt = fCS.length === 8
        ? `${fCS.slice(6,8)}/${fCS.slice(4,6)}/${fCS.slice(0,4)}`
        : fCS;
      const cuitLookupCarg = String(row[14] || '').trim();
      const dataCarg = cuitKtKvMap[cuitLookupCarg] || { kt: '-', kv: '-' };
      r.detCarg.push({
        id:    String(row[8] || ''),
        fecha: fCFmt,
        soc:   String(row[5] || '-') + (isV ? ' (Propia)' : ' (Ajena)'),
        q:     Number(row[4]) || 0,
        un:    String(row[9] || '-'),
        kt:    dataCarg.kt,
        kv:    dataCarg.kv,
      });
    }

    if (!isV && !isC) continue;
    const f  = row[2];
    const id = row[8];
    const opKey = id ? String(id) : `idx_${i}`;

    if (inS(f)) {
      if (!seenS[opKey]) {
        seenS[opKey] = 1;
        r.cabConc += row[4];
        r.trConc++;
      }
      if (isC && !seenCWeek[opKey]) {
        seenCWeek[opKey] = 1;
        r.cabCWeekTrop++;
        if (row[3] >= 0) r.dCompras[row[3]]++;
      }
      if (isV) { r.cabV += row[4]; if (row[5]) socOps[row[5]] = 1; }
      if (isC) {
        r.cabC += row[4];
        if (row[6]) socCompraWeek[row[6]] = 1;
        const cuitC = String(row[15] || '').trim();
        const dataC = cuitKtKvMap[cuitC] || { kt: row[10] || '-', kv: '-' };
        r.detC.push({ id: row[8], un: row[9], soc: row[6], fecha: row[7], q: row[4], kt: dataC.kt, kv: dataC.kv });
      }
      const cuitLookup = String(row[14] || '').trim();
      const ktKv = cuitKtKvMap[cuitLookup] || { kt: '-', kv: '-' };
      const tieneCargar = isCargForAc ? 'Sí' : '';
      const acLado = isV && isC ? 'vend/comp' : (isV ? 'vend' : 'comp');
      allOps.push({ q: row[4], kt: ktKv.kt, kv: ktKv.kv, d: [row[8], row[9], row[5], row[0], row[6], row[1], row[7], row[4], tieneCargar, acLado] });
    }
    if (inA(f) && !seenA[opKey]) { seenA[opKey] = 1; r.pConc += row[4]; }
    if (inA(f) && isC) r.pCabC += row[4];
    if (inM(f) && !seenM[opKey]) { seenM[opKey] = 1; r.cabOperMtd += row[4]; }
    if (inM(f)) { if (isV) r.cabVOperMtd += row[4]; if (isC) r.cabCOperMtd += row[4]; }
    if (inMPrev(f) && !seenMPrev[opKey]) { seenMPrev[opKey] = 1; r.pCabOperMtd += row[4]; }

    for (let w = 0; w < semMes.length; w++) {
      if (f < semMes[w].ini || f > semMes[w].fin) continue;
      if (!seenSemMes[w][opKey]) {
        seenSemMes[w][opKey] = 1;
        r.operSemMesVals[w] += row[4];
        const cuitLkp = String(row[14] || '').trim();
        const ktkvW   = cuitKtKvMap[cuitLkp] || { kt: '-', kv: '-' };
        r.operSemMesDets[w].push({ id: row[8], un: row[9], soc: String(row[5] || row[6] || '-'), fecha: row[7], q: row[4], kt: ktkvW.kt, kv: ktkvW.kv });
      }
      break;
    }
    if (semPrevIni && f >= semPrevIni && f <= semPrevFin && !seenPrevSem[opKey]) {
      seenPrevSem[opKey] = 1;
      r.prevSemOperBase += row[4];
    }
  }
  r.socOps = Object.keys(socOps).length;
  r.cabCSocCount = Object.keys(socCompraWeek).length;
  allOps.sort((a, b) => b.q - a.q);
  r.top5 = allOps.slice(0, 5);

  r.ccc       = (cabConcBase + cabNoConcBase) > 0
    ? Math.round(cabConcBase / (cabConcBase + cabNoConcBase) * 100) + '%'
    : '0%';
  r.cotizadas = (cabConcBase + cabNoConcBase) > 0
    ? Math.round(cabCotizadasCcc / (cabConcBase + cabNoConcBase) * 100) + '%'
    : '0%';

  // ── CRM (Comentarios + Agenda) ──
  const socGest = {}, pSocGest = {}, gestDia = [{},{},{},{},{},{},{}];
  const comSocGest = {}, ageSocGest = {};
  const crmGestiones = {};

  function getLeadKey(idLead, soc, fallback) {
    const id = String(idLead || '').trim();
    if (id) return 'id:' + id;
    const s = String(soc || '').trim();
    if (s) return 'soc:' + s;
    return fallback;
  }

  for (let i = 0; i < D.coms.length; i++) {
    const row = D.coms[i];
    if (row[0] !== acMail) continue;
    const f    = row[1];
    const gKey = getLeadKey(row[7], row[3], 'com:' + i);
    if (inS(f)) {
      if (row[4] && gKey) comSocGest[gKey] = 1;
      if (gKey) {
        socGest[gKey] = 1;
        if (row[2] >= 0) gestDia[row[2]][gKey] = 1;
        if (!crmGestiones[gKey] || f > crmGestiones[gKey].f)
          crmGestiones[gKey] = { f, tipo: 'Comentario', cm: row[6] || '', soc: row[3] || '', idLead: String(row[7] || '') };
      }
    }
    if (inA(f) && gKey) pSocGest[gKey] = 1;
  }
  for (let i = 0; i < D.agendas.length; i++) {
    const row = D.agendas[i];
    if (row[0] !== acMail) continue;
    const f    = row[1];
    const gKey = getLeadKey(row[6], row[3], 'age:' + i);
    if (inS(f)) {
      if (gKey) ageSocGest[gKey] = 1;
      if (gKey) {
        socGest[gKey] = 1;
        if (row[2] >= 0) gestDia[row[2]][gKey] = 1;
        if (!crmGestiones[gKey] || f > crmGestiones[gKey].f)
          crmGestiones[gKey] = { f, tipo: 'Agenda', cm: row[4] || '', soc: row[3] || '', idLead: String(row[6] || '') };
      }
    }
    if (inA(f) && gKey) pSocGest[gKey] = 1;
  }
  r.com  = Object.keys(comSocGest).length;
  r.age  = Object.keys(ageSocGest).length;
  r.tSG  = Object.keys(socGest).length;
  r.pTSG = Object.keys(pSocGest).length;
  for (let d = 0; d < 7; d++) r.dGestion[d] = Object.keys(gestDia[d]).length;

  // ── AUX LEADS ──
  const ssgByLead    = {};
  const asigSemSoc   = {}, asigPrevSoc = {}, asigSemFuenteCount = {};
  const seenAsigSemFuenteSoc = {};
  const socSinGestAsigSemSet = {}, socSinGestSet = {}, prevSocSinGestSet = {};
  const asigSemData  = {}, auxByLead = {};

  function saveSsgRow(socKey, rowData) {
    if (!socKey || !rowData) return;
    const prev = ssgByLead[socKey];
    if (!prev) { ssgByLead[socKey] = rowData; return; }
    const prevSem = Number(prev.asigSem) || 0;
    const rowSem  = Number(rowData.asigSem) || 0;
    if (rowSem !== prevSem) { if (rowSem > prevSem) ssgByLead[socKey] = rowData; return; }
    const prevW = Number(prev.w) || 0;
    const rowW  = Number(rowData.w) || 0;
    if (rowW !== prevW) { if (rowW < prevW) ssgByLead[socKey] = rowData; return; }
    const prevFa = prev.fa || '', rowFa = rowData.fa || '';
    if (rowFa > prevFa) ssgByLead[socKey] = rowData;
  }

  for (let i = 0; i < D.auxLeads.length; i++) {
    const row = D.auxLeads[i];
    if (row[0] !== acMail) continue;

    const socKey = getLeadKey(row[16], row[6], 'aux:' + i);

    if (row[1] && inS(row[1])) {
      asigSemSoc[socKey] = 1;
      if (!asigSemData[socKey] || row[1] > (asigSemData[socKey].fa || ''))
        asigSemData[socKey] = { kt: row[4], kv: row[5], soc: row[6], fa: row[1], sg: row[10], ug: row[11], w: row[8], fuente: row[13], asigSem: 1 };
      const fuente = String(row[13] || '').trim().toUpperCase() || 'OTROS';
      const sfKey  = fuente + '|' + socKey;
      if (!seenAsigSemFuenteSoc[sfKey]) {
        seenAsigSemFuenteSoc[sfKey] = 1;
        asigSemFuenteCount[fuente] = (asigSemFuenteCount[fuente] || 0) + 1;
      }
    }
    if (row[1] && inA(row[1])) asigPrevSoc[socKey] = 1;

    if (row[3]) {  // esNuevo
      socSinGestSet[socKey] = 1;
      saveSsgRow(socKey, { kt: row[4], kv: row[5], soc: row[6], fa: row[1], sg: row[10], ug: row[11], w: row[8], fuente: row[13], asigSem: (row[1] && inS(row[1])) ? 1 : 0 });
      if (row[1] && inS(row[1])) socSinGestAsigSemSet[socKey] = 1;
    }
    if (row[3] && row[1] && row[1] <= fin_) prevSocSinGestSet[socKey] = 1;

    if (!row[3] && socKey && row[1] && inS(row[1])) {
      if (!auxByLead[socKey] || row[1] > (auxByLead[socKey].fa || ''))
        auxByLead[socKey] = { kt: row[4], kv: row[5], fa: row[1], fuente: row[13], estado: row[15], cm: row[14], tipo: 'Asignación', soc: row[6], idLead: row[16] };
    }
  }

  // Incorporar asignadas sin gestión CRM
  for (const asigKey of Object.keys(asigSemSoc)) {
    if (!socGest[asigKey]) {
      socSinGestAsigSemSet[asigKey] = 1;
      socSinGestSet[asigKey] = 1;
      saveSsgRow(asigKey, asigSemData[asigKey]);
    }
  }
  for (const pAsigKey of Object.keys(asigPrevSoc)) {
    if (!pSocGest[pAsigKey]) prevSocSinGestSet[pAsigKey] = 1;
  }

  const ssgAll = Object.keys(ssgByLead).map(k => ssgByLead[k]);
  let ssgDaysSum = 0, ssgDaysCount = 0;
  for (const ssg of ssgAll) {
    const days = Number(ssg.w);
    if (!isNaN(days)) { ssgDaysSum += days; ssgDaysCount++; }
  }
  ssgAll.sort((a, b) => {
    const aSem = Number(a.asigSem) || 0, bSem = Number(b.asigSem) || 0;
    if (aSem !== bSem) return bSem - aSem;
    const aw = Number(a.w) || 0, bw = Number(b.w) || 0;
    if (aw !== bw) return aw - bw;
    if (!a.fa && !b.fa) return 0;
    if (!a.fa) return 1;
    if (!b.fa) return -1;
    return a.fa < b.fa ? 1 : a.fa > b.fa ? -1 : 0;
  });
  r.ssgTop5         = ssgAll.slice(0, 5);
  r.socSinGestNum   = Object.keys(socSinGestSet).length;
  r.pSocSinGestNum  = Object.keys(prevSocSinGestSet).length;
  props.setProp(ssgStoreKey2, String(r.socSinGestNum));

  // Variación correcta: reporte de la semana anterior
  if (!skipPrevLookup) {
    try {
      const prevRpt = await getReport(ac, startTs - 604800000, endTs - 604800000, { skipPrevLookup: true });
      if (prevRpt && !prevRpt.error && prevRpt.socSinGestNum !== undefined) {
        r.pSocSinGestNum = Number(prevRpt.socSinGestNum) || 0;
      } else {
        const stored = props.getProp(ssgPrevKey2);
        if (stored !== null) r.pSocSinGestNum = parseInt(stored, 10) || 0;
      }
    } catch (e) {
      const stored = props.getProp(ssgPrevKey2);
      if (stored !== null) r.pSocSinGestNum = parseInt(stored, 10) || 0;
    }
  } else {
    const stored = props.getProp(ssgPrevKey2);
    if (stored !== null) r.pSocSinGestNum = parseInt(stored, 10) || 0;
  }

  r.socSinGestAvgDays = ssgDaysCount ? Math.round((ssgDaysSum / ssgDaysCount) * 10) / 10 : 0;
  r.nuevas            = Object.keys(asigSemSoc).length;
  r.pNuevas           = Object.keys(asigPrevSoc).length;
  r.nuevasFuentes     = asigSemFuenteCount;
  r.socSinGestAsigSem = Object.keys(socSinGestAsigSemSet).length;

  let tsgAsig = 0;
  for (const sgKey of Object.keys(socGest)) { if (asigSemSoc[sgKey]) tsgAsig++; }
  r.tSGAsigSem = tsgAsig;

  // Top Soc. Gestionadas
  const actArr = [];
  for (const crmKey of Object.keys(crmGestiones)) {
    const crm    = crmGestiones[crmKey];
    const al     = auxByLead[crmKey];
    const sortFa = al ? al.fa : crm.f;
    actArr.push({ kt: al ? al.kt : '-', kv: al ? al.kv : '-', soc: crm.soc || (al ? al.soc : '-') || '-', fa: crm.f, fuente: al ? al.fuente : '-', estado: al ? al.estado : '-', cm: crm.cm, tipo: crm.tipo, _sortFa: sortFa });
  }
  actArr.sort((a, b) => {
    if (!a._sortFa && !b._sortFa) return 0;
    if (!a._sortFa) return 1;
    if (!b._sortFa) return -1;
    return a._sortFa < b._sortFa ? 1 : a._sortFa > b._sortFa ? -1 : 0;
  });
  r.actSemanal = actArr.slice(0, 5).map(({ _sortFa, ...rest }) => rest);

  // ── SAC ──
  for (const row of D.sacs) {
    if (row[0] !== acN) continue;
    if (inS(row[1])) {
      const estSac = String(row[4] || '').trim().toUpperCase();
      let estShow = '';
      if      (estSac === 'APROBADO')  { estShow = 'APROBADO';  r.sacAprob++; }
      else if (estSac === 'RECHAZADO') { estShow = 'RECHAZADO'; r.sacRech++;  }
      else if (estSac === 'PENDIENTE') { estShow = 'PENDIENTE'; r.sacPend++;  }
      r.sacs.push({ s: row[3], f: row[1], e: estShow });
      r.sacsTable.push({ soc: row[3], fecha: row[1], estado: estShow, jdSol: row[5], jdApro: row[6], un: row[7] });
      if (row[2] >= 0) r.dSacs[row[2]]++;
    }
    if (inA(row[1])) r.pSac++;
  }

  // ── REMATES ──
  const remIds = {}, pRemIds = {};
  for (const row of D.remates) {
    if (row[0] !== acN) continue;
    if (inS(row[1])) remIds[row[2]]  = 1;
    if (inA(row[1])) pRemIds[row[2]] = 1;
  }
  r.rem  = Object.keys(remIds).length;
  r.pRem = Object.keys(pRemIds).length;

  // ── RANKING GLOBAL ──
  const rOfrec = {}, rComp = {}, rOper = {};
  const seenRnkOf = {}, seenRnkOp = {};

  for (let i = 0; i < D.base.length; i++) {
    const row    = D.base[i];
    const baseId = String(row[10] || '').trim();
    const bKey   = baseId ? baseId : `rb_idx_${i}`;
    if (!seenRnkOf[bKey]) {
      seenRnkOf[bKey] = true;
      if (inS(row[1]) && (row[5] || row[8] || row[6] || row[7])) {
        const q  = Number(row[4]) || 0;
        const n  = String(row[0]  || '').trim();
        const rv = String(row[14] || '').trim();
        const rc = String(row[15] || '').trim();
        if (n)                       rOfrec[n]  = (rOfrec[n]  || 0) + q;
        if (rv && rv !== n)          rOfrec[rv] = (rOfrec[rv] || 0) + q;
        if (rc && rc !== n && rc !== rv) rOfrec[rc] = (rOfrec[rc] || 0) + q;
      }
    }
  }
  for (let i = 0; i < D.ops.length; i++) {
    const row   = D.ops[i];
    const opId  = String(row[8] || '').trim();
    const oKey  = opId ? opId : `ro_idx_${i}`;
    if (!seenRnkOp[oKey]) {
      seenRnkOp[oKey] = true;
      if (inS(row[2])) {
        const q = Number(row[4]) || 0;
        const vNs = [];
        const aV = String(row[0]  || '').trim(); if (aV && !vNs.includes(aV)) vNs.push(aV);
        const rV = String(row[18] || '').trim(); if (rV && !vNs.includes(rV)) vNs.push(rV);
        const cNs = [];
        const aC = String(row[1]  || '').trim(); if (aC && !cNs.includes(aC)) cNs.push(aC);
        const rC = String(row[19] || '').trim(); if (rC && !cNs.includes(rC)) cNs.push(rC);
        const uniqueOps = [...new Set([...vNs, ...cNs])];
        uniqueOps.forEach(x => { rOper[x] = (rOper[x] || 0) + q; });
        cNs.forEach(x => { rComp[x] = (rComp[x] || 0) + q; });
      }
    }
  }
  function toRnk(obj) {
    return Object.entries(obj).map(([nombre, q]) => ({ nombre, q })).sort((a, b) => b.q - a.q);
  }
  r.rankingOfrecidas = toRnk(rOfrec);
  r.rankingCompradas = toRnk(rComp);
  r.rankingOperadas  = toRnk(rOper);

  // ── Cachear y devolver ──
  cache.set(rKey, r, cache.TTL.REPORT);
  return r;
}

// ─── getConfigData (para el modal de mails) ───────────────────────────────────
async function getConfigData() {
  const rows = await getSheetData('Config 2.0');
  const config = [];
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    if (!g(row, 0) || !String(g(row, 0)).trim()) break;
    config.push([
      g(row, 0) || '',  // A: Nombre
      g(row, 1) || '',  // B: ID Carpeta Drive
      g(row, 2) || '',  // C: Email
      g(row, 3) || '',  // D: Nombre mail
      '',               // E: (unused)
      g(row, 5) || '',  // F: CC
    ]);
  }
  return { config };
}

module.exports = {
  getConfig, loadData, warmup, scheduledWarmup,
  clearCache, debugCacheStatus, refreshCacheAndWarmup,
  getReport, getConfigData, getReportCacheVersion,
};
