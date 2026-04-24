// api/_lib/logic.js
// Traducción completa de Code.gs (Reportes Semanales v7.1) a Node.js.
// Mantiene la lógica idéntica al original.

const { getSheetData, g } = require('./sheets');
const { fetchMetabaseQuery } = require('./metabase');
const cache = require('./cache');
const props = require('./props');
const diskCache = require('./metaDiskCache');
const redisCache = require('./redisCache');

// ─── bcfull: mapa módulo-level cargado en background desde Metabase Q221 ───────────────
const _bcfullMap  = new Map(); // cuit → { kt: bovinos, kv: vaca }
let   _bcfullState = 'idle';   // 'idle' | 'loading' | 'done' | 'error'

function _buildBcfullMap(metaEstab) {
  const h     = metaEstab.headers || [];
  const iCuit = h.indexOf('cuit_titular_est');
  const iBov  = h.indexOf('bovinos');
  const iVaca = h.indexOf('vaca');
  if (iCuit < 0 || iBov < 0 || iVaca < 0) {
    console.error('[bcfull] Q221: columnas no encontradas. headers=', h);
    return;
  }
  const agg = Object.create(null);
  for (const row of (metaEstab.rows || [])) {
    const cuit = String(row[iCuit] || '').trim();
    if (!cuit) continue;
    if (!agg[cuit]) agg[cuit] = { bov: 0, vac: 0 };
    agg[cuit].bov += Number(row[iBov]) || 0;
    agg[cuit].vac += Number(row[iVaca]) || 0;
  }
  _bcfullMap.clear();
  for (const [cuit, v] of Object.entries(agg)) {
    _bcfullMap.set(cuit, { kt: String(v.bov), kv: String(v.vac) });
    // Fuzzy key (primeros 10 dígitos) para CUITs en notación científica
    if (cuit.length >= 10) _bcfullMap.set(cuit.slice(0, 10), { kt: String(v.bov), kv: String(v.vac) });
  }
  console.log('[bcfull] listo: ' + _bcfullMap.size + ' entradas.');
}

function _ensureBcfull(cachedEstab) {
  if (_bcfullState !== 'idle') return;
  // Si tenemos datos del disco cache, usarlos directamente (sin fetch)
  if (cachedEstab) {
    _bcfullState = 'loading';
    try {
      _buildBcfullMap(cachedEstab);
      _bcfullState = 'done';
      console.log('[bcfull] cargado desde disk cache');
    } catch(e) {
      console.error('[bcfull] error al cargar desde disk cache:', e.message);
      _bcfullState = 'idle';
    }
    return;
  }
  _bcfullState = 'loading';
  console.log('[bcfull] iniciando carga background de Q221...');
  fetchMetabaseQuery(221)
    .then(metaEstab => {
      _buildBcfullMap(metaEstab);
      _bcfullState = 'done';
      // Actualizar disk cache con Q221
      const existing = diskCache.readCache();
      if (existing) {
        diskCache.writeCache({ metaBase: existing.metaBase, metaOps: existing.metaOps, metaEstab });
      }
      // Limpiar cache de reportes para que se regeneren con kt/kv correctos
      const cleared = cache.delByPrefix('R12_');
      console.log('[bcfull] cache de reportes limpiado (' + (cleared || 0) + ' entradas). Próximos reportes tendrán kt/kv correctos.');
    })
    .catch(e => {
      console.error('[bcfull] error en Q221:', e.message);
      _bcfullState = 'idle';
    });
}

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
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${y}${m}${dd}`;
}

// dd/MM/yyyy en UTC
function toFmt(val) {
  const d = parseSheetDate(val);
  if (!d) return '';
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
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
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
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
  diskCache.deleteCache(); // elimina el cache en disco
  _bcfullState = 'idle';   // fuerza recarga de Q221 desde Metabase
  _bcfullMap.clear();
  return 'Cache limpiado';
}

// Solo limpia la memoria sin tocar el disco cache (para warmup automático)
function _flushMemoryOnly() {
  cache.flush();
  props.resetR12Ver();
  // NO borra disk cache ni resetea bcfull
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
    const mail = String(g(row, 20) || '').trim().toLowerCase();

    if (nombre && mail) {
      acMap[nombre] = mail;
      acMap[norm(nombre)] = mail;
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
// forceRefresh=true: ignora todos los caches y recarga desde Metabase.
async function loadData(forceRefresh) {
  const cached = cache.get('data');
  if (cached && !forceRefresh) return cached;

  const useRedis = redisCache.isConfigured();

  // ── 1. Redis cache (producción / Vercel) ──────────────────────────────────
  if (useRedis && !forceRefresh) {
    const red = await redisCache.readCache();
    if (red && red.metaBase && red.metaOps && red.bcMapObj) {
      console.log('[logic] loadData: usando Redis cache');
      // Restaurar bcfullMap desde el objeto cacheado
      _bcfullMap.clear();
      for (const [cuit, val] of Object.entries(red.bcMapObj)) {
        _bcfullMap.set(cuit, val);
      }
      _bcfullState = 'done';
      const [comsRaw, agendasRaw, leadsRaw, auxLeadsRaw, sacsRaw, rematesRaw] = await Promise.all([
        getSheetData('Comentarios_CRM'),
        getSheetData('Agenda_CRM'),
        getSheetData('Leads_CRM'),
        getSheetData('aux leads'),
        getSheetData('SAC'),
        getSheetData('REMATES'),
      ]);
      return _processLoadData(red.metaBase, red.metaOps, comsRaw, agendasRaw, leadsRaw, auxLeadsRaw, sacsRaw, rematesRaw, red.ts);
    }
  }

  // ── 2. Disk cache (local dev, sin Redis) ──────────────────────────────────
  if (!useRedis && !forceRefresh) {
    const disk = diskCache.readCache();
    if (disk && disk.metaBase && disk.metaOps) {
      console.log('[logic] loadData: usando disk cache para Q101+Q102');
      _ensureBcfull(disk.metaEstab || null);
      const [comsRaw, agendasRaw, leadsRaw, auxLeadsRaw, sacsRaw, rematesRaw] = await Promise.all([
        getSheetData('Comentarios_CRM'),
        getSheetData('Agenda_CRM'),
        getSheetData('Leads_CRM'),
        getSheetData('aux leads'),
        getSheetData('SAC'),
        getSheetData('REMATES'),
      ]);
      return _processLoadData(disk.metaBase, disk.metaOps, comsRaw, agendasRaw, leadsRaw, auxLeadsRaw, sacsRaw, rematesRaw, disk.ts);
    }
  }

  // ── 3. Fetch fresco desde Metabase ───────────────────────────────────────
  console.log('[logic] loadData: cargando BASE/OPS/Q221 desde Metabase + 6 hojas de Sheets...');
  const [
    metaBaseFetched, metaOpsFetched, metaEstabFetched, comsRaw, agendasRaw,
    leadsRaw, auxLeadsRaw, sacsRaw, rematesRaw,
  ] = await Promise.all([
    fetchMetabaseQuery(101),
    fetchMetabaseQuery(102),
    fetchMetabaseQuery(221),
    getSheetData('Comentarios_CRM'),
    getSheetData('Agenda_CRM'),
    getSheetData('Leads_CRM'),
    getSheetData('aux leads'),
    getSheetData('SAC'),
    getSheetData('REMATES'),
  ]);

  // Construir bcfullMap
  _bcfullMap.clear();
  _buildBcfullMap(metaEstabFetched);
  _bcfullState = 'done';

  // Serializar bcfullMap para guardarlo en cache
  const bcMapObj = Object.fromEntries(_bcfullMap);

  // Guardar en Redis (producción) o disco (local)
  let savedTs;
  if (useRedis) {
    savedTs = await redisCache.writeCache(metaBaseFetched, metaOpsFetched, bcMapObj);
  } else {
    savedTs = diskCache.writeCache({ metaBase: metaBaseFetched, metaOps: metaOpsFetched, metaEstab: metaEstabFetched });
  }

  return _processLoadData(metaBaseFetched, metaOpsFetched, comsRaw, agendasRaw, leadsRaw, auxLeadsRaw, sacsRaw, rematesRaw, savedTs || Date.now());
}

// ─── _processLoadData ─────────────────────────────────────────────────────────
// Procesa los datos raw de Metabase + Sheets y construye los arrays internos.
async function _processLoadData(metaBase, metaOps, comsRaw, agendasRaw, leadsRaw, auxLeadsRaw, sacsRaw, rematesRaw, metaCacheTs) {
  // ── BASE (Metabase Q101) ──
  const base = [];
  const bMap = {};
  (metaBase.headers || []).forEach((h, i) => { if (h) bMap[h] = i; });
  const idxB = {
    ac: bMap['ac_vend'] ?? bMap['ac vendedor'] ?? bMap['asociado_comercial'] ?? bMap['asociado comercial'] ?? 5,
    f: bMap['fecha_publicaciones'] ?? bMap['fecha publicaciones'] ?? bMap['fecha'] ?? bMap['f_crea'] ?? bMap['created_at'] ?? 1,
    soc: bMap['sociedad_vendedora'] ?? bMap['sociedad vendedora'] ?? bMap['soc'] ?? bMap['razon_social'] ?? bMap['razon social'] ?? 2,
    cab: bMap['cabezas'] ?? bMap['q'] ?? 4,
    est: bMap['estado'] ?? 3,
    cot: bMap['cotizada'] ?? bMap['cotizado'] ?? 6,
    id: bMap['id_lote'] ?? bMap['id lote'] ?? bMap['id'] ?? 0,
    cuit: bMap['cuit_vend'] ?? bMap['cuit vend'] ?? bMap['cuit_empresa'] ?? bMap['cuit empresa'] ?? bMap['cuit'] ?? 16,
    un: bMap['un'] ?? bMap['unidad_negocio'] ?? 7,
    repVend: bMap['repre_vendedor'] ?? bMap['repre vendedor'] ?? bMap['repre_vend'] ?? bMap['representante'] ?? 20,
    repComp: bMap['repre_comprador'] ?? bMap['repre comprador'] ?? bMap['repre_comp'] ?? 21,
    rend:    bMap['rend'] ?? -1,
  };
  console.log('[logic] BASE (Q101): headers=', metaBase.headers, '| filas=', (metaBase.rows || []).length, '| idxB=', idxB);
  (metaBase.rows || []).forEach(row => {
    const ac = norm(g(row, idxB.ac));
    const repVend = norm(g(row, idxB.repVend));
    const repComp = norm(g(row, idxB.repComp));
    if (!ac && !repVend && !repComp) return;
    const f = toDateStr(g(row, idxB.f)); if (!f) return;
    const est = String(g(row, idxB.est) || '').trim().toUpperCase();
    let conc = false, pub = false, ofr = false, noConc = false;
    const cotizo = Number(g(row, idxB.cot)) === 1 ? 1 : 0;
    if (est === 'CONCRETADA') { conc = true; }
    else if (est === 'PUBLICADO') { pub = true; }
    else if (est === 'OFRECIMIENTOS') { ofr = true; }
    else if (est === 'NO CONCRETADA' || est === 'NO CONCRETADAS') { noConc = true; }
    else return;
    base.push([
      ac,                           // 0 ac
      f,                            // 1 f
      toDayIdx(g(row, idxB.f)),     // 2 di
      g(row, idxB.soc) || '',       // 3 soc
      Number(g(row, idxB.cab)) || 0,// 4 cab
      conc ? 1 : 0,                 // 5 conc
      pub ? 1 : 0,                 // 6 pub
      ofr ? 1 : 0,                 // 7 ofr
      noConc ? 1 : 0,               // 8 noConc
      cotizo,                       // 9 cotizo
      String(g(row, idxB.id) || ''),// 10 id (col A)
      String(g(row, idxB.cuit) || ''),// 11 CUIT (col Q)
      String(g(row, idxB.un) || ''),// 12 UN (col H)
      toFmt(g(row, idxB.f)),        // 13 fmtFecha
      repVend,                      // 14 rep vend
      repComp,                      // 15 rep comp
      Number(g(row, idxB.rend)) || 0, // 16 rend (solo para CONCRETADA)
    ]);
  });


  // ── OPS (Metabase Q102) ──
  const ops = [];
  const oMap = {};
  // Metabase devuelve headers ya en lowercase — no necesita slice ni trim
  (metaOps.headers || []).forEach((h, i) => { if (h) oMap[h] = i; });
  const idxO = {
    aV: oMap['asoc_com_vend'] ?? oMap['asoc com vend'] ?? 6,
    aC: oMap['asoc_com_compra'] ?? oMap['asoc com compra'] ?? 8,
    rV: oMap['repre_vendedor'] ?? oMap['repre vendedor'] ?? 34,
    rC: oMap['repre_comprador'] ?? oMap['repre comprador'] ?? 35,
    f: oMap['fecha_operacion'] ?? oMap['fecha operacion'] ?? 2,
    cargAc: oMap['op_carga'] ?? oMap['op carga'] ?? 22,
    cargF: oMap['fecha_carga'] ?? oMap['fecha carga'] ?? 18,
    qTot: oMap['q'] ?? oMap['q total'] ?? 9,
    socV: oMap['rs_vendedora'] ?? oMap['rs vendedora'] ?? 5,
    socC: oMap['rs_compradora'] ?? oMap['rs compradora'] ?? 7,
    id: oMap['id'] ?? 0,
    un: oMap['un'] ?? 1,
    cat: oMap['cat'] ?? oMap['categoria'] ?? 10,
    cuitV: oMap['cuit_vend'] ?? oMap['cuit vend'] ?? 20,
    cuitC: oMap['cuit_comp'] ?? oMap['cuit comp'] ?? 21,
    qPart: oMap['q_particular'] ?? oMap['q particular'] ?? 16,
    rend: oMap['rend'] ?? oMap['rendimiento'] ?? 25,
  };
  console.log('[logic] OPS (Q102): headers=', metaOps.headers, '| filas=', (metaOps.rows || []).length, '| idxO=', idxO);
  (metaOps.rows || []).forEach(row => {
    const aV = norm(g(row, idxO.aV)), aC = norm(g(row, idxO.aC));
    const rV = norm(g(row, idxO.rV)), rC = norm(g(row, idxO.rC));
    if (!aV && !aC && !rV && !rC) return;
    const f = toDateStr(g(row, idxO.f)); if (!f) return;
    const cargAcRaw = String(g(row, idxO.cargAc) || '').trim();
    const cargF = g(row, idxO.cargF) ? toDateStr(g(row, idxO.cargF)) : '';
    ops.push([
      aV, aC, f, toDayIdx(g(row, idxO.f)),
      Number(g(row, idxO.qTot)) || 0,    // 4 Q total
      g(row, idxO.socV) || '',             // 5 socV
      g(row, idxO.socC) || '',             // 6 socC
      toFmt(g(row, idxO.f)),               // 7 fmtFecha
      String(g(row, idxO.id) || ''),       // 8 ID
      String(g(row, idxO.un) || ''),       // 9 UN
      String(g(row, idxO.cat) || ''),      // 10 Cat
      norm(cargAcRaw),                     // 11 ac carga normalizado
      cargF,                               // 12 fecha carga
      cargF ? toDayIdx(g(row, idxO.cargF)) : -1, // 13 di carga
      String(g(row, idxO.cuitV) || ''),    // 14 cuitV
      String(g(row, idxO.cuitC) || ''),    // 15 cuitC
      cargAcRaw.toLowerCase(),             // 16 ac carga lowercase
      Number(g(row, idxO.qPart)) || 0,     // 17 Q particular
      rV,                                  // 18 repV
      rC,                                  // 19 repC
      Number(g(row, idxO.rend)) || 0,      // 20 rend (rendimiento decimal, e.g. 0.083 = 8.3%)
    ]);
  });

  // ── COMENTARIOS CRM ──
  // col A(0)=idLead, B(1)=soc, C(2)=mail AC, D(3)=fecha, E(4)=comentario
  const coms = [];
  comsRaw.slice(1).forEach(row => {
    const mail = String(g(row, 2) || '').trim().toLowerCase(); if (!mail) return;
    const f = toDateStr(g(row, 3)); if (!f) return;
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
  // A(0)=ID Lead, B(1)=Titulo Lead (El usuario pidió esto como ID Lead), C(2)=Agendado por, D(3)=Fecha Agendado
  const agendas = [];
  agendasRaw.slice(1).forEach(row => {
    const mail = String(g(row, 2) || '').trim().toLowerCase(); if (!mail) return;
    const originalDate = String(g(row, 3) || '').trim(); // Columna D
    const f = toDateStr(originalDate); if (!f) return;
    const fStr = f.length === 8 ? `${f.slice(6, 8)}/${f.slice(4, 6)}/${f.slice(0, 4)}` : originalDate;

    agendas.push([
      mail,
      f,
      toDayIdx(originalDate),
      g(row, 1) || '',         // 3 soc (Titulo Lead, Col B)
      fStr,                    // 4 comentario (fecha legible DD/MM/YYYY)
      'Agenda',                // 5 tipo
      String(g(row, 1) || ''), // 6 idLead (Titulo Lead, Col B)
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
    const est = String(g(row, 4) || '').trim().toUpperCase();
    const f = toDateStr(g(row, 2));
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
    const f = toDateStr(g(row, 19)); if (!f) return;
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
    const f = toDateStr(g(row, 1)); if (!f) return;
    remates.push([ac, f, String(g(row, 3) || Math.random())]);
  });

  const data = { base, ops, coms, agendas, leads, auxLeads, sacs, remates, metaCacheTs: metaCacheTs || Date.now() };
  cache.set('data', data, cache.TTL.DATA);
  console.log(`[logic] _processLoadData: base=${base.length} ops=${ops.length} auxLeads=${auxLeads.length} metaCacheTs=${new Date(metaCacheTs||0).toISOString()} completado.`);
  return data;
}

// ─── warmup ──────────────────────────────────────────────────────────────────
async function warmup(forceRefresh) {
  await getConfig();
  await loadData(forceRefresh || false);
  return { ok: true };
}

// ─── scheduledWarmup (equivale al trigger horario de Apps Script) ─────────────
function scheduledWarmup() {
  console.log('[logic] scheduledWarmup: solo flush de memoria (disk cache preservado).');
  // Solo borra memoria — el disco cache de 12h se respeta
  _flushMemoryOnly();
  warmup().then(() => console.log('[logic] scheduledWarmup: caché actualizada.'));
  return { ok: true };
}

// ─── debugCacheStatus ─────────────────────────────────────────────────────────
function debugCacheStatus(ac, startTs, endTs) {
  const cfgCached = cache.get('config');
  const dataCached = cache.get('data');
  let rKey = '', rHit = false;
  if (ac && startTs && endTs && cfgCached) {
    const acMail = cfgCached.acMap[ac] || '';
    const ver = getReportCacheVersion();
    if (acMail) {
      rKey = `R12_${ver}_${acMail.replace(/[@.]/g, '_')}_${startTs}_${endTs}`;
      rHit = !!cache.get(rKey);
    }
  }
  return {
    reportCacheVersion: getReportCacheVersion(),
    hasCFG8: !!cfgCached,
    hasDATA10: !!dataCached,
    data10Chunks: '1',
    ac: ac || '',
    acMail: cfgCached ? (cfgCached.acMap[ac] || '') : '',
    reportKey: rKey,
    reportHit: rHit,
  };
}

// ─── refreshCacheAndWarmup ───────────────────────────────────────────────────
async function refreshCacheAndWarmup(ac, startTs, endTs) {
  // Limpiar cache de reportes en memoria
  const clearMsg = clearCache();
  // Limpiar Redis (si está configurado) para forzar reload desde Metabase
  await redisCache.deleteCache();
  // Recargar datos frescos de Metabase con forceRefresh=true
  const warm = await warmup(true);
  const status = debugCacheStatus(ac, startTs, endTs);
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
  const ver = getReportCacheVersion();
  const rMode = skipPrevLookup ? '_raw' : '';
  const rKey = `R12_${ver}_${acMail.replace(/[@.]/g, '_')}_${startTs}_${endTs}${rMode}`;

  const ssgSanitized2 = acMail.replace(/[^a-zA-Z0-9_]/g, '_');
  const ssgStoreKey2 = `SSGN_${ssgSanitized2}_${startTs}`;
  const ssgPrevKey2 = `SSGN_${ssgSanitized2}_${startTs - 604800000}`;

  // ── Cache hit ──
  const hit = cache.get(rKey);
  if (hit) {
    const result = { ...hit };
    // Siempre refrescar metaCacheTs desde disco para que sea preciso
    if (!result.metaCacheTs) result.metaCacheTs = diskCache.getCacheTs() || Date.now();
    const storedPrev = props.getProp(ssgPrevKey2);
    if (storedPrev !== null) result.pSocSinGestNum = parseInt(storedPrev, 10) || 0;
    return result;
  }

  const D = await loadData();

  // ── Rangos de fechas (UTC) ──
  const d0 = new Date(startTs);
  const d1 = new Date(endTs);
  const ini = fmtDateUTC(d0);
  const fin = fmtDateUTC(d1);
  const ini_ = fmtDateUTC(new Date(startTs - 7 * 86400000));
  const fin_ = fmtDateUTC(new Date(endTs - 7 * 86400000));

  // Inicio de mes de d1 (UTC noon)
  const m0 = new Date(Date.UTC(d1.getUTCFullYear(), d1.getUTCMonth(), 1, 12));
  const iniM = fmtDateUTC(m0);

  // Mes anterior (mismo corte de día)
  const prevMonthStart = new Date(Date.UTC(m0.getUTCFullYear(), m0.getUTCMonth() - 1, 1, 12));
  const prevMonthEnd = new Date(Date.UTC(m0.getUTCFullYear(), m0.getUTCMonth(), 0, 12)); // día 0 = último del mes anterior
  const prevCutDay = Math.min(d1.getUTCDate(), prevMonthEnd.getUTCDate());
  const prevMonthCut = new Date(Date.UTC(prevMonthStart.getUTCFullYear(), prevMonthStart.getUTCMonth(), prevCutDay, 12));
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
    ini: fmtDateUTC(new Date(s.s)),
    fin: fmtDateUTC(new Date(s.e)),
  }));

  // Última semana del mes anterior
  const semPrevMes = (cfg.semanas || []).filter(s => {
    if (!s || !s.s || !s.e) return false;
    const we = new Date(s.e);
    return we.getUTCFullYear() === prevMonthStart.getUTCFullYear() &&
      we.getUTCMonth() === prevMonthStart.getUTCMonth();
  }).sort((a, b) => a.e - b.e);
  const ultSemPrevMes = semPrevMes.length ? semPrevMes[semPrevMes.length - 1] : null;
  let semPrevIni = '', semPrevFin = '';
  if (ultSemPrevMes) {
    semPrevIni = fmtDateUTC(new Date(ultSemPrevMes.s));
    semPrevFin = fmtDateUTC(new Date(ultSemPrevMes.e));
  }

  const inS = f => f >= ini && f <= fin;
  const inA = f => f >= ini_ && f <= fin_;
  const inM = f => f >= iniM && f <= fin;
  const inMPrev = f => f >= iniMPrev && f <= finMPrev;

  // ── Resultado ──
  const r = {
    cab: 0, trop: 0, pCab: 0, pTrop: 0, cccNum: 0, cccDen: 0, cabPublicadas: 0,
    cabCompra: 0, pCabCompra: 0, tropCompra: 0, socCompra: 0,
    dT: [0, 0, 0, 0, 0, 0, 0], dCompras: [0, 0, 0, 0, 0, 0, 0],
    cabC: 0, cabCWeekTrop: 0, cabCSocCount: 0, pCabC: 0,
    cabV: 0, cabConc: 0, trConc: 0, pConc: 0,
    cabOperMtd: 0, pCabOperMtd: 0, cabVOperMtd: 0, cabCOperMtd: 0,
    carg: 0, cargProp: 0, cargAjen: 0, dCargas: [0, 0, 0, 0, 0, 0, 0],
    com: 0, age: 0, tSG: 0, pTSG: 0, dGestion: [0, 0, 0, 0, 0, 0, 0],
    nuevas: 0, pNuevas: 0, nuevasFuentes: {}, socSinGestNum: 0, pSocSinGestNum: 0, socSinGestAsigSem: 0,
    sacs: [], sacsTable: [], pSac: 0, sacAprob: 0, sacRech: 0, sacPend: 0,
    rem: 0, pRem: 0, dSacs: [0, 0, 0, 0, 0, 0, 0],
    top5: [], ssgTop5: [], actSemanal: [],
    detOf: [], detC: [], detCarg: [],
    operSemMesLabels: [], operSemMesVals: [], operSemMesDets: [], prevSemOperBase: 0,
    socOf: 0, socOps: 0, ccc: '0%', cotizadas: '0%',
    tSGAsigSem: 0, socSinGestAvgDays: 0, hideCRM: false,
    rankingOfrecidas: [], rankingCompradas: [], rankingOperadas: [],
  };

  // Detectar si hay datos CRM para este AC
  let hasCrmData = D.auxLeads.some(row => row[0] === acMail) ||
    D.coms.some(row => row[0] === acMail) ||
    D.agendas.some(row => row[0] === acMail) ||
    D.leads.some(row => row[0] === acMail);
  r.hideCRM = !hasCrmData;

  r.operSemMesLabels = semMes.map(s => s.label);
  r.operSemMesVals = semMes.map(() => 0);
  r.operSemMesDets = semMes.map(() => []);

  // ── Mapa CUIT → Kt, Kv ──
  const cuitKtKvMap = {};
  const fuzzyCuitMap = {}; // Fallback para CUITs mutilados por Notación Científica en Google Sheets

  function addKtKv(cuit, kt, kv) {
    if (!cuit) return;
    if (!cuitKtKvMap[cuit]) cuitKtKvMap[cuit] = { kt: kt, kv: kv || '-' };
    if (cuit.length >= 10) {
      const fuzzyKey = cuit.slice(0, 10);
      if (!fuzzyCuitMap[fuzzyKey]) fuzzyCuitMap[fuzzyKey] = { kt: kt, kv: kv || '-' };
    }
  }

  // 1. Prioridad: aux leads (por CUIT en col AA = idx 12 del array procesado)
  for (const row of D.auxLeads) {
    addKtKv(String(row[12] || '').trim(), row[4], row[5]);
  }
  // 2. Complemento: _bcfullMap módulo-level (cargado en background desde Q221)
  //    getKtKv lo consulta directamente — no necesita pre-cargar en cuitKtKvMap

  function getKtKv(cuitStr) {
    // a) Q221 (bcfull) — fuente primaria: datos oficiales de Metabase
    if (_bcfullMap.has(cuitStr)) return _bcfullMap.get(cuitStr);
    // b) Fallback fuzzy (CUITs en notación científica de Sheets)
    if (cuitStr.toLowerCase().includes('e')) {
      const parsed = parseFloat(cuitStr);
      if (!isNaN(parsed)) {
        const truncStr = String(Math.trunc(parsed));
        const mapVal = _bcfullMap.get(truncStr.slice(0, 10));
        if (mapVal) return mapVal;
        const fuzzyKtKv = fuzzyCuitMap[truncStr.slice(0, 10)];
        if (fuzzyKtKv) return fuzzyKtKv;
      }
    }
    // c) auxLeads como fallback si Q221 no tiene el CUIT
    const auxKtKv = cuitKtKvMap[cuitStr];
    if (auxKtKv) return auxKtKv;
    return { kt: '-', kv: '-' };
  }

  // ── BASE ──
  const socOf = {};
  let cabConcBase = 0, cabNoConcBase = 0, cabCotizadasCcc = 0;
  let rendOfSumW = 0, rendOfCabW = 0; // para promedio ponderado rend ofrecidas
  const seenBaseId = {};

  for (let i = 0; i < D.base.length; i++) {
    const row = D.base[i];
    // Solo incluir si el AC aparece como vendedor (AC_Vend o repre_vendedor).
    // repre_comprador (row[15]) NO aplica en la BASE de ofrecidas.
    if (row[0] !== acN && row[14] !== acN) continue;
    const baseId = String(row[10] || '').trim();
    const bKey = baseId ? baseId : `b_idx_${i}`;
    if (seenBaseId[bKey]) continue;
    seenBaseId[bKey] = true;

    const f = row[1];
    if (inS(f)) {
      r.cab += row[4]; r.trop++;
      if (row[5]) cabConcBase += row[4];
      if (row[8]) cabNoConcBase += row[4];
      if (row[6] || row[7]) r.cabPublicadas += row[4];
      if ((row[5] || row[8]) && row[9]) cabCotizadasCcc += row[4];
      // Rend ponderado: solo CONCRETADAS con rend != 0
      if (row[5] && row[16]) { rendOfSumW += row[16] * row[4]; rendOfCabW += row[4]; }
      if (row[3]) socOf[row[3]] = 1;
      if (row[2] >= 0) r.dT[row[2]]++;

      const cuitOf = String(row[11] || '').trim();
      const dataOf = getKtKv(cuitOf);
      const fStr = String(f || '');
      const fFmt = fStr.length === 8
        ? `${fStr.slice(6, 8)}/${fStr.slice(4, 6)}/${fStr.slice(0, 4)}`
        : fStr;

      r.detOf.push({
        id: row[10] || '',
        fecha: row[13] ? String(row[13]) : fFmt,
        soc: row[3] || '-',
        q: row[4] || 0,
        un: row[12] || '-',
        kt: dataOf.kt,
        kv: dataOf.kv,
        est: row[5] ? 'C' : (row[8] ? 'NC' : (row[6] ? 'P' : (row[7] ? 'O' : '-'))),
        cot: row[9] || 0,
        rend: row[5] ? (row[16] || 0) : 0, // rend solo para CONCRETADAS
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
  let rendCompSumW = 0, rendCompCabW = 0; // para promedio ponderado rend compradas

  for (let i = 0; i < D.ops.length; i++) {
    const row = D.ops[i];
    const isV = (row[0] === acN) || (row[18] === acN);
    const isC = (row[1] === acN) || (row[19] === acN);
    const isCargForAc = row[11] === acN || row[16] === acMail;

    if (!isV && !isC && !isCargForAc) continue;
    const opIdStr = String(row[8] || '').trim();
    const oKey = opIdStr ? opIdStr : `o_idx_${i}`;
    if (seenOpsId[oKey]) continue;
    seenOpsId[oKey] = true;

    // Cargas
    if (isCargForAc && row[12] && inS(row[12])) {
      r.carg++;
      if (isV) r.cargProp++; else r.cargAjen++;
      if (row[13] >= 0) r.dCargas[row[13]]++;

      const fCS = String(row[12] || '');
      const fCFmt = fCS.length === 8
        ? `${fCS.slice(6, 8)}/${fCS.slice(4, 6)}/${fCS.slice(0, 4)}`
        : fCS;
      const cuitLookupCarg = String(row[14] || '').trim();
      const dataCarg = getKtKv(cuitLookupCarg);
      r.detCarg.push({
        id: String(row[8] || ''),
        fecha: fCFmt,
        soc: String(row[5] || '-') + (isV ? ' (Propia)' : ' (Ajena)'),
        q: Number(row[4]) || 0,
        un: String(row[9] || '-'),
        kt: dataCarg.kt,
        kv: dataCarg.kv,
      });
    }

    if (!isV && !isC) continue;
    const f = row[2];
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
        const dataC = getKtKv(cuitC);
        r.detC.push({ id: row[8], un: row[9], soc: row[6], fecha: row[7], q: row[4], kt: dataC.kt, kv: dataC.kv, rend: row[20] || 0 });
        // Rend ponderado compradas
        if (row[20]) { rendCompSumW += row[20] * row[4]; rendCompCabW += row[4]; }
      }
      // Siempre calcular kt/kv para AMBOS lados: vendedor y comprador
      const ktKvV = getKtKv(String(row[14] || '').trim()); // cuitV → vendedora
      const ktKvC = getKtKv(String(row[15] || '').trim()); // cuitC → compradora
      const tieneCargar = isCargForAc ? 'Sí' : '';
      const acLado = isV && isC ? 'vend/comp' : (isV ? 'vend' : 'comp');
      allOps.push({ q: row[4], kt: ktKvV.kt, kv: ktKvV.kv, ktC: ktKvC.kt, kvC: ktKvC.kv, rend: row[20] || 0, d: [row[8], row[9], row[5], row[0], row[6], row[1], row[7], row[4], tieneCargar, acLado] });
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
        const ktkvW = getKtKv(cuitLkp);
        const l = isV && isC ? 'vend/comp' : (isV ? 'vend' : 'comp');
        r.operSemMesDets[w].push({ id: row[8], un: row[9], soc: String(row[5] || row[6] || '-'), fecha: row[7], q: row[4], kt: ktkvW.kt, kv: ktkvW.kv, lado: l, rend: Number(row[20]) || 0 });
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

  r.ccc = (cabConcBase + cabNoConcBase) > 0
    ? Math.round(cabConcBase / (cabConcBase + cabNoConcBase) * 100) + '%'
    : '0%';
  r.cotizadas = (cabConcBase + cabNoConcBase) > 0
    ? Math.round(cabCotizadasCcc / (cabConcBase + cabNoConcBase) * 100) + '%'
    : '0%';
  r.cabConc   = cabConcBase;
  r.cabNoConc = cabNoConcBase;
  // Promedios ponderados de rendimiento (solo AC — el frontend decide si mostrar)
  r.rendPonderadoOf   = rendOfCabW   > 0 ? rendOfSumW   / rendOfCabW   : 0;
  r.rendPonderadoComp = rendCompCabW > 0 ? rendCompSumW / rendCompCabW : 0;


  // ── CRM (Comentarios + Agenda) ──
  const socGest = {}, pSocGest = {}, gestDia = [{}, {}, {}, {}, {}, {}, {}];
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
    const f = row[1];
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
    const f = row[1];
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
  r.com = Object.keys(comSocGest).length;
  r.age = Object.keys(ageSocGest).length;
  r.tSG = Object.keys(socGest).length;
  r.pTSG = Object.keys(pSocGest).length;
  for (let d = 0; d < 7; d++) r.dGestion[d] = Object.keys(gestDia[d]).length;

  // ── AUX LEADS ──
  const ssgByLead = {};
  const asigSemSoc = {}, asigPrevSoc = {}, asigSemFuenteCount = {};
  const seenAsigSemFuenteSoc = {};
  const socSinGestAsigSemSet = {}, socSinGestSet = {}, prevSocSinGestSet = {};
  const asigSemData = {}, auxByLead = {};

  function saveSsgRow(socKey, rowData) {
    if (!socKey || !rowData) return;
    const prev = ssgByLead[socKey];
    if (!prev) { ssgByLead[socKey] = rowData; return; }
    const prevSem = Number(prev.asigSem) || 0;
    const rowSem = Number(rowData.asigSem) || 0;
    if (rowSem !== prevSem) { if (rowSem > prevSem) ssgByLead[socKey] = rowData; return; }
    const prevW = Number(prev.w) || 0;
    const rowW = Number(rowData.w) || 0;
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
      const sfKey = fuente + '|' + socKey;
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
  r.ssgTop5 = ssgAll.slice(0, 5);
  r.socSinGestNum = Object.keys(socSinGestSet).length;
  r.pSocSinGestNum = Object.keys(prevSocSinGestSet).length;
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
  r.nuevas = Object.keys(asigSemSoc).length;
  r.pNuevas = Object.keys(asigPrevSoc).length;
  r.nuevasFuentes = asigSemFuenteCount;
  r.socSinGestAsigSem = Object.keys(socSinGestAsigSemSet).length;

  let tsgAsig = 0;
  for (const sgKey of Object.keys(socGest)) { if (asigSemSoc[sgKey]) tsgAsig++; }
  r.tSGAsigSem = tsgAsig;

  // Top Soc. Gestionadas
  const actArr = [];
  for (const crmKey of Object.keys(crmGestiones)) {
    const crm = crmGestiones[crmKey];
    const al = auxByLead[crmKey];
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
      if (estSac === 'APROBADO') { estShow = 'APROBADO'; r.sacAprob++; }
      else if (estSac === 'RECHAZADO') { estShow = 'RECHAZADO'; r.sacRech++; }
      else if (estSac === 'PENDIENTE') { estShow = 'PENDIENTE'; r.sacPend++; }
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
    if (inS(row[1])) remIds[row[2]] = 1;
    if (inA(row[1])) pRemIds[row[2]] = 1;
  }
  r.rem = Object.keys(remIds).length;
  r.pRem = Object.keys(pRemIds).length;

  // ── RANKING GLOBAL ──
  const rOfrec = {}, rComp = {}, rOper = {};
  const seenRnkOf = {}, seenRnkOp = {};

  for (let i = 0; i < D.base.length; i++) {
    const row = D.base[i];
    const baseId = String(row[10] || '').trim();
    const bKey = baseId ? baseId : `rb_idx_${i}`;
    if (!seenRnkOf[bKey]) {
      seenRnkOf[bKey] = true;
      if (inS(row[1]) && (row[5] || row[8] || row[6] || row[7])) {
        const q = Number(row[4]) || 0;
        const n = String(row[0] || '').trim();
        const rv = String(row[14] || '').trim();
        // Solo AC_Vend y repre_vendedor cuentan en Ofrecidas — no repre_comprador
        if (n) rOfrec[n] = (rOfrec[n] || 0) + q;
        if (rv && rv !== n) rOfrec[rv] = (rOfrec[rv] || 0) + q;
      }
    }
  }
  for (let i = 0; i < D.ops.length; i++) {
    const row = D.ops[i];
    const opId = String(row[8] || '').trim();
    const oKey = opId ? opId : `ro_idx_${i}`;
    if (!seenRnkOp[oKey]) {
      seenRnkOp[oKey] = true;
      if (inS(row[2])) {
        const q = Number(row[4]) || 0;
        const vNs = [];
        const aV = String(row[0] || '').trim(); if (aV && !vNs.includes(aV)) vNs.push(aV);
        const rV = String(row[18] || '').trim(); if (rV && !vNs.includes(rV)) vNs.push(rV);
        const cNs = [];
        const aC = String(row[1] || '').trim(); if (aC && !cNs.includes(aC)) cNs.push(aC);
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
  r.rankingOperadas = toRnk(rOper);

  // Incluir timestamp del cache de Metabase para mostrarlo en el frontend
  r.metaCacheTs = D.metaCacheTs || diskCache.getCacheTs() || Date.now();

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
