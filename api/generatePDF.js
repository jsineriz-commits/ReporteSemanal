// api/generatePDF.js
// Genera el reporte semanal como PDF usando PDFKit (puro Node.js — sin Chrome ni Puppeteer).
// Recibe { ac, startTs, endTs, semana } y devuelve { ok, pdfBase64, fileName }.

const PDFDocument = require('pdfkit');
const { getReport } = require('./_lib/logic');

// ── Paleta de colores (réplica del diseño web) ──────────────────────────────
const C = {
  bg:         '#f9fafb',
  white:      '#ffffff',
  primary:    '#2563eb',    // azul
  success:    '#059669',    // verde
  danger:     '#dc2626',    // rojo
  warning:    '#d97706',    // ámbar
  purple:     '#7c3aed',
  teal:       '#0891b2',
  textMain:   '#111827',
  textMuted:  '#6b7280',
  textLight:  '#9ca3af',
  border:     '#e5e7eb',
  headerBg:   '#1e293b',   // encabezado oscuro
  rowAlt:     '#f8fafc',   // fila alternada
  greenBg:    '#ecfdf5',
  greenTxt:   '#059669',
  redBg:      '#fef2f2',
  redTxt:     '#dc2626',
  blueBg:     '#eff6ff',
  blueTxt:    '#2563eb',
};

// ── Helpers ──────────────────────────────────────────────────────────────────
function n(val) { return (Number(val) || 0).toLocaleString('es-AR'); }
function pct(val) { return typeof val === 'string' ? val : (Math.round(Number(val) || 0) + '%'); }
function truncate(str, len) { const s = String(str || ''); return s.length > len ? s.slice(0, len - 1) + '…' : s; }

function streamToBuffer(doc) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    doc.on('end',  () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);
  });
}

// ── Componentes de dibujo ────────────────────────────────────────────────────

/** Sección encabezado con fondo de color */
function sectionHeader(doc, title, y, color = C.headerBg) {
  doc.rect(40, y, doc.page.width - 80, 22).fill(color);
  doc.fillColor(C.white).fontSize(9).font('Helvetica-Bold')
     .text(title.toUpperCase(), 48, y + 7, { width: doc.page.width - 96 });
  doc.fillColor(C.textMain);
  return y + 26;
}

/** Tabla genérica con encabezados y filas */
function table(doc, headers, rows, colWidths, startX, startY, opts = {}) {
  const rowH    = opts.rowH    || 16;
  const fontSize= opts.fontSize|| 7.5;
  const headerBg= opts.headerBg|| '#334155';
  const total   = colWidths.reduce((a, b) => a + b, 0);
  let y = startY;

  // Encabezado
  doc.rect(startX, y, total, rowH).fill(headerBg);
  let x = startX;
  headers.forEach((h, i) => {
    const align = (opts.headerAlign && opts.headerAlign[i]) || 'left';
    doc.fillColor(C.white).fontSize(fontSize).font('Helvetica-Bold')
       .text(String(h), x + 4, y + (rowH - fontSize) / 2, { width: colWidths[i] - 8, align });
    x += colWidths[i];
  });
  y += rowH;

  // Filas
  rows.forEach((row, ri) => {
    const bg = ri % 2 === 0 ? C.white : C.rowAlt;
    doc.rect(startX, y, total, rowH).fill(bg);
    // Borde inferior suave
    doc.rect(startX, y + rowH - 0.5, total, 0.5).fill(C.border);

    x = startX;
    row.forEach((cell, ci) => {
      const align = (opts.colAlign && opts.colAlign[ci]) || 'left';
      const color = (opts.colColor && opts.colColor[ci]) || C.textMain;
      doc.fillColor(color).fontSize(fontSize).font('Helvetica')
         .text(truncate(String(cell ?? '-'), 42), x + 4, y + (rowH - fontSize) / 2, { width: colWidths[ci] - 8, align });
      x += colWidths[ci];
    });
    y += rowH;
  });

  // Borde exterior
  doc.rect(startX, startY, total, y - startY).strokeColor(C.border).lineWidth(0.5).stroke();
  doc.fillColor(C.textMain);
  return y + 4;
}

/** Tarjeta KPI grande */
function kpiCard(doc, label, value, subLabel, subValue, x, y, w, h, accentColor) {
  // Fondo card
  doc.rect(x, y, w, h).fill(C.white).rect(x, y, w, h).strokeColor(C.border).lineWidth(0.5).stroke();
  // Borde izquierdo de color
  doc.rect(x, y, 3, h).fill(accentColor);
  // Label
  doc.fillColor(C.textMuted).fontSize(7).font('Helvetica')
     .text(label.toUpperCase(), x + 10, y + 8, { width: w - 14 });
  // Valor
  doc.fillColor(accentColor).fontSize(20).font('Helvetica-Bold')
     .text(value, x + 10, y + 18, { width: w - 14 });
  // Sub
  if (subLabel) {
    doc.fillColor(C.textMuted).fontSize(6.5).font('Helvetica')
       .text(`${subLabel}: `, x + 10, y + h - 14, { continued: true, width: w - 14 })
       .fillColor(C.textMain).font('Helvetica-Bold').text(subValue || '-');
  }
  doc.fillColor(C.textMain);
}

/** Chip de variación (↑ verde / ↓ rojo / — gris) */
function chip(doc, current, previous, x, y, w, suffix = '') {
  const cur = Number(current) || 0;
  const prv = Number(previous) || 0;
  const diff = cur - prv;
  const color = diff > 0 ? C.success : diff < 0 ? C.danger : C.textMuted;
  const arrow = diff > 0 ? '▲' : diff < 0 ? '▼' : '–';
  const bg    = diff > 0 ? C.greenBg : diff < 0 ? C.redBg : '#f3f4f6';
  const label = `${arrow} ${Math.abs(diff)}${suffix} vs sem. ant.`;
  doc.rect(x, y, w, 12).fill(bg);
  doc.fillColor(color).fontSize(6.5).font('Helvetica-Bold')
     .text(label, x + 4, y + 3, { width: w - 8 });
  doc.fillColor(C.textMain);
}

/** Mini barra horizontal */
function barRow(doc, label, value, maxVal, x, y, w, barColor) {
  const labelW = 80;
  const valW   = 30;
  const barW   = w - labelW - valW - 8;
  const fill   = maxVal > 0 ? Math.max(2, (value / maxVal) * barW) : 2;

  doc.fillColor(C.textMain).fontSize(7).font('Helvetica')
     .text(truncate(label, 14), x, y + 1, { width: labelW });
  // Fondo barra
  doc.rect(x + labelW, y + 3, barW, 7).fill('#e5e7eb');
  // Relleno barra
  doc.rect(x + labelW, y + 3, fill, 7).fill(barColor);
  // Valor
  doc.fillColor(C.textMain).fontSize(7).font('Helvetica-Bold')
     .text(n(value), x + labelW + barW + 4, y + 1, { width: valW, align: 'right' });
  doc.fillColor(C.textMain);
  return y + 13;
}

// ── Handler principal ────────────────────────────────────────────────────────
module.exports = async (req, res) => {
  if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'Method not allowed' });

  const { ac, startTs, endTs, semana } = req.body || {};
  if (!ac || !startTs || !endTs) {
    return res.status(400).json({ ok: false, error: 'Faltan parámetros: ac, startTs, endTs' });
  }

  try {
    console.log(`[generatePDF] Obteniendo datos para: ${ac} | sem: ${semana || '?'}`);
    const d = await getReport(ac, Number(startTs), Number(endTs));
    if (!d || d.error) throw new Error(d?.error || 'getReport devolvió datos vacíos');

    console.log(`[generatePDF] Generando PDF con PDFKit...`);

    // Semana y fechas
    const semStr   = semana ? `Semana ${semana}` : '';
    const fIni     = new Date(Number(startTs)).toLocaleDateString('es-AR', { day: '2-digit', month: '2-digit', year: 'numeric', timeZone: 'America/Argentina/Buenos_Aires' });
    const fFin     = new Date(Number(endTs)).toLocaleDateString('es-AR',   { day: '2-digit', month: '2-digit', year: 'numeric', timeZone: 'America/Argentina/Buenos_Aires' });
    const dias     = ['Sáb', 'Dom', 'Lun', 'Mar', 'Mié', 'Jue', 'Vie'];
    const pageW    = 595;
    const margin   = 40;
    const usableW  = pageW - margin * 2;

    const doc = new PDFDocument({ size: 'A4', margin: 0, bufferPages: true });

    // ════════════════════════════════════════════════════════════════════════
    // HEADER
    // ════════════════════════════════════════════════════════════════════════
    doc.rect(0, 0, pageW, 62).fill(C.headerBg);
    doc.fillColor(C.white).fontSize(18).font('Helvetica-Bold')
       .text(ac, margin, 14, { width: usableW - 140 });
    doc.fillColor('#94a3b8').fontSize(9).font('Helvetica')
       .text('Reporte Semanal de Actividad', margin, 36, { width: usableW - 140 });
    // Badge semana
    doc.rect(pageW - margin - 120, 12, 120, 38).fill('#334155');
    doc.fillColor(C.white).fontSize(11).font('Helvetica-Bold')
       .text(semStr, pageW - margin - 120, 18, { width: 120, align: 'center' });
    doc.fillColor('#94a3b8').fontSize(7.5).font('Helvetica')
       .text(`${fIni} — ${fFin}`, pageW - margin - 120, 33, { width: 120, align: 'center' });
    doc.fillColor(C.textMain);

    let y = 72;

    // ════════════════════════════════════════════════════════════════════════
    // KPIs PRINCIPALES
    // ════════════════════════════════════════════════════════════════════════
    y = sectionHeader(doc, 'METRICAS PRINCIPALES', y, '#1e3a5f');

    const cardW = (usableW - 12) / 4;
    const cardH = 58;
    const kpis  = [
      { label: 'Cab. Ofrecidas',   value: n(d.cab),      sub: 'Tropas',  subV: n(d.trop),    color: C.primary  },
      { label: 'Cab. Concretadas', value: n(d.cabConc),  sub: 'No Conc.',subV: n(d.cabNoConc||0), color: C.success  },
      { label: 'Cargas asistidas', value: n(d.carg),     sub: 'Propias', subV: n(d.cargProp), color: C.teal     },
      { label: 'Soc. Gest. CRM',   value: n(d.tSG),      sub: 'Nuevas',  subV: n(d.nuevas),   color: C.purple   },
    ];
    kpis.forEach((k, i) => {
      kpiCard(doc, k.label, k.value, k.sub, k.subV,
        margin + i * (cardW + 4), y, cardW, cardH, k.color);
    });
    y += cardH + 4;

    // KPIs secundarios en fila
    const secKpis = [
      { label: 'CCC',              value: pct(d.ccc) },
      { label: 'Cotizadas',        value: pct(d.cotizadas) },
      { label: 'SACs enviados',    value: n(d.sacs?.length || 0) },
      { label: 'SAC Aprobados',    value: n(d.sacAprob) },
      { label: 'Cab. Concretadas', value: n(d.cabConc) },
      { label: 'Cab. No Conc.',    value: n(d.cabNoConc) },
      { label: 'Soc. Ofrecidas',   value: n(d.socOf) },
      { label: 'Soc. Operadas',    value: n(d.socOps) },
    ];
    const secW = usableW / secKpis.length;
    secKpis.forEach((k, i) => {
      const sx = margin + i * secW;
      doc.rect(sx, y, secW - 1, 30).fill(i % 2 === 0 ? C.white : C.rowAlt)
         .rect(sx, y, secW - 1, 30).strokeColor(C.border).lineWidth(0.3).stroke();
      doc.fillColor(C.textMuted).fontSize(6).font('Helvetica')
         .text(k.label.toUpperCase(), sx + 4, y + 4, { width: secW - 8 });
      doc.fillColor(C.textMain).fontSize(11).font('Helvetica-Bold')
         .text(k.value, sx + 4, y + 13, { width: secW - 8 });
    });
    y += 34;

    // ════════════════════════════════════════════════════════════════════════
    // DISTRIBUCIÓN DIARIA
    // ════════════════════════════════════════════════════════════════════════
    y = sectionHeader(doc, 'DISTRIBUCION DIARIA', y + 4, '#1e3a5f');
    const halfW = (usableW - 8) / 2;

    // Ofrecidas por día
    const maxOf = Math.max(...(d.dT || [0]));
    doc.fillColor(C.textMuted).fontSize(7).font('Helvetica-Bold')
       .text('OFRECIDAS (TROPAS)', margin, y + 2);
    let barY = y + 13;
    dias.forEach((dia, i) => {
      barY = barRow(doc, dia, d.dT?.[i] || 0, maxOf, margin, barY, halfW, C.primary);
    });

    // Cargas por día
    const maxCarg = Math.max(...(d.dCargas || [0]));
    doc.fillColor(C.textMuted).fontSize(7).font('Helvetica-Bold')
       .text('CARGAS', margin + halfW + 8, y + 2);
    let barY2 = y + 13;
    dias.forEach((dia, i) => {
      barY2 = barRow(doc, dia, d.dCargas?.[i] || 0, maxCarg, margin + halfW + 8, barY2, halfW, C.teal);
    });
    y = Math.max(barY, barY2) + 6;

    // ════════════════════════════════════════════════════════════════════════
    // EVOLUCIÓN DE OPERACIONES (últimas semanas)
    // ════════════════════════════════════════════════════════════════════════
    if (d.operSemMesLabels?.length) {
      y = sectionHeader(doc, 'EVOLUCION DE OPERACIONES', y + 2, '#1e3a5f');
      const evoHeaders = [...d.operSemMesLabels, 'TOTAL'];
      const colW = Math.min(60, (usableW - 40) / evoHeaders.length);
      const evoWidths = evoHeaders.map(() => colW);
      const evoRow = [...d.operSemMesVals.map(v => n(v)),
                      n(d.operSemMesVals.reduce((a, b) => a + b, 0))];
      y = table(doc, evoHeaders, [evoRow], evoWidths, margin, y,
        { colAlign: evoHeaders.map(() => 'center'), headerAlign: evoHeaders.map(() => 'center') });
    }

    // ════════════════════════════════════════════════════════════════════════
    // TOP NEGOCIOS
    // ════════════════════════════════════════════════════════════════════════
    if (d.top5?.length) {
      y = sectionHeader(doc, 'PRINCIPALES NEGOCIOS DE LA SEMANA', y + 2, '#1e3a5f');
      const tHeaders = ['ID', 'UN', 'SOC. VENDEDORA', 'SOC. COMPRADORA', 'FECHA', 'Q', 'Kt', 'Kv'];
      const tWidths  = [40, 32, 110, 110, 46, 30, 28, 28];
      const tRows    = d.top5.map(op => {
        const row = op.d || [];
        return [
          truncate(String(row[0] || '-'), 8),
          truncate(String(row[1] || '-'), 6),
          truncate(String(row[2] || '-'), 20),
          truncate(String(row[4] || '-'), 20),
          truncate(String(row[6] || '-'), 10),
          n(row[7] || op.q),
          truncate(String(op.kt || '-'), 6),
          truncate(String(op.kv || '-'), 6),
        ];
      });
      y = table(doc, tHeaders, tRows, tWidths, margin, y,
        { colAlign: ['left','left','left','left','left','right','right','right'],
          headerAlign: ['left','left','left','left','left','right','right','right'] });
    }

    // ════════════════════════════════════════════════════════════════════════
    // CRM
    // ════════════════════════════════════════════════════════════════════════
    if (!d.hideCRM) {
      // Nueva página si queda poco espacio
      if (y > 650) { doc.addPage(); y = 30; }

      y = sectionHeader(doc, 'CRM — GESTION DE SOCIEDADES', y + 4, '#4c1d95');

      // KPIs CRM en fila
      const crmKpis = [
        { label: 'Soc. Gestionadas', value: n(d.tSG) },
        { label: 'Nuevas asignadas',  value: n(d.nuevas) },
        { label: 'Sin gestión',        value: n(d.socSinGestNum) },
        { label: 'Días prom. sin gest.',value: String(d.socSinGestAvgDays || 0) },
        { label: 'Comentarios',        value: n(d.com) },
        { label: 'Agendas',            value: n(d.age) },
      ];
      const crmSecW = usableW / crmKpis.length;
      crmKpis.forEach((k, i) => {
        const sx = margin + i * crmSecW;
        doc.rect(sx, y, crmSecW - 1, 30).fill(i % 2 === 0 ? '#faf5ff' : '#f5f3ff')
           .rect(sx, y, crmSecW - 1, 30).strokeColor('#e9d5ff').lineWidth(0.3).stroke();
        doc.fillColor('#6b21a8').fontSize(6).font('Helvetica')
           .text(k.label.toUpperCase(), sx + 4, y + 4, { width: crmSecW - 8 });
        doc.fillColor('#3b0764').fontSize(11).font('Helvetica-Bold')
           .text(k.value, sx + 4, y + 13, { width: crmSecW - 8 });
      });
      y += 34;

      // Top Soc. Gestionadas
      if (d.actSemanal?.length) {
        doc.fillColor(C.textMuted).fontSize(8).font('Helvetica-Bold')
           .text('Top sociedades gestionadas esta semana', margin, y + 2);
        y += 12;
        const sgHeaders = ['Kt', 'Kv', 'SOCIEDAD', 'FUENTE', 'ESTADO', 'TIPO', 'Ú. ACTIVIDAD'];
        const sgWidths  = [28, 28, 120, 60, 55, 55, 169];
        const sgRows    = d.actSemanal.map(a => [
          truncate(String(a.kt || '-'), 6),
          truncate(String(a.kv || '-'), 6),
          truncate(String(a.soc || '-'), 22),
          truncate(String(a.fuente || '-'), 12),
          truncate(String(a.estado || '-'), 12),
          truncate(String(a.tipo || '-'), 12),
          truncate(String(a.cm || '-'), 30),
        ]);
        y = table(doc, sgHeaders, sgRows, sgWidths, margin, y, { headerBg: '#5b21b6' });
      }

      // Top Soc. Sin Gestión
      if (d.ssgTop5?.length) {
        if (y > 660) { doc.addPage(); y = 30; }
        doc.fillColor(C.textMuted).fontSize(8).font('Helvetica-Bold')
           .text('Sociedades sin gestión esta semana', margin, y + 4);
        y += 14;
        const ssgHeaders = ['Kt', 'Kv', 'SOCIEDAD', 'FUENTE', 'FECHA ASIG.', 'DÍAS SIN GEST.'];
        const ssgWidths  = [28, 28, 140, 80, 70, 169];
        const ssgRows    = d.ssgTop5.map(s => [
          truncate(String(s.kt || '-'), 6),
          truncate(String(s.kv || '-'), 6),
          truncate(String(s.soc || '-'), 25),
          truncate(String(s.fuente || '-'), 14),
          truncate(String(s.fa || '-'), 12),
          String(s.w || 0),
        ]);
        y = table(doc, ssgHeaders, ssgRows, ssgWidths, margin, y,
          { headerBg: '#be123c', colAlign: ['left','left','left','left','left','center'] });
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // SACs
    // ════════════════════════════════════════════════════════════════════════
    if (d.sacsTable?.length) {
      if (y > 660) { doc.addPage(); y = 30; }
      y = sectionHeader(doc, `SACs (${d.sacsTable.length}) — Aprobados: ${d.sacAprob} - Rechazados: ${d.sacRech} - Pendientes: ${d.sacPend}`, y + 4, '#1e4d6b');
      const sacHeaders = ['SOCIEDAD', 'FECHA', 'ESTADO', 'JD Solicitud', 'JD Aprobación', 'UN'];
      const sacWidths  = [175, 55, 65, 80, 80, 60];
      const sacRows    = d.sacsTable.map(s => [
        truncate(String(s.soc || '-'), 30),
        truncate(String(s.fecha || '-'), 10),
        String(s.estado || '-'),
        truncate(String(s.jdSol || '-'), 14),
        truncate(String(s.jdApro || '-'), 14),
        truncate(String(s.un || '-'), 10),
      ]);
      y = table(doc, sacHeaders, sacRows, sacWidths, margin, y, {
        headerBg: '#0f4c75',
        colColor: (row) => row[2] === 'APROBADO' ? C.success : row[2] === 'RECHAZADO' ? C.danger : null,
      });
    }

    // ════════════════════════════════════════════════════════════════════════
    // DETALLE OFRECIDAS
    // ════════════════════════════════════════════════════════════════════════
    if (d.detOf?.length) {
      if (y > 620) { doc.addPage(); y = 30; }
      y = sectionHeader(doc, `DETALLE OFRECIDAS (${d.detOf.length})`, y + 4, '#1e3a5f');
      const ofHeaders = ['ID', 'FECHA', 'SOCIEDAD', 'Q', 'UN', 'EST', 'Kt', 'Kv'];
      const ofWidths  = [40, 50, 160, 30, 50, 28, 44, 113];
      const ofRows    = d.detOf.map(o => [
        truncate(String(o.id || '-'), 8),
        truncate(String(o.fecha || '-'), 10),
        truncate(String(o.soc || '-'), 28),
        n(o.q),
        truncate(String(o.un || '-'), 10),
        String(o.est || '-'),
        truncate(String(o.kt || '-'), 8),
        truncate(String(o.kv || '-'), 8),
      ]);
      y = table(doc, ofHeaders, ofRows, ofWidths, margin, y,
        { colAlign: ['left','left','left','right','left','center','right','right'] });
    }

    // ════════════════════════════════════════════════════════════════════════
    // DETALLE OPERADAS
    // ════════════════════════════════════════════════════════════════════════
    if (d.detC?.length) {
      if (y > 620) { doc.addPage(); y = 30; }
      y = sectionHeader(doc, `DETALLE OPERADAS (${d.detC.length})`, y + 4, '#1e3a5f');
      const dcHeaders = ['ID', 'FECHA', 'SOC. COMPRADORA', 'Q', 'UN', 'Kt', 'Kv'];
      const dcWidths  = [40, 50, 180, 30, 60, 44, 111];
      const dcRows    = d.detC.map(o => [
        truncate(String(o.id || '-'), 8),
        truncate(String(o.fecha || '-'), 10),
        truncate(String(o.soc || '-'), 32),
        n(o.q),
        truncate(String(o.un || '-'), 12),
        truncate(String(o.kt || '-'), 8),
        truncate(String(o.kv || '-'), 8),
      ]);
      y = table(doc, dcHeaders, dcRows, dcWidths, margin, y,
        { colAlign: ['left','left','left','right','left','right','right'] });
    }

    // ════════════════════════════════════════════════════════════════════════
    // DETALLE CARGAS
    // ════════════════════════════════════════════════════════════════════════
    if (d.detCarg?.length) {
      if (y > 620) { doc.addPage(); y = 30; }
      y = sectionHeader(doc, `DETALLE CARGAS (${d.detCarg.length})`, y + 4, '#1e3a5f');
      const cHeaders = ['ID', 'FECHA', 'SOCIEDAD', 'Q', 'UN', 'Kt', 'Kv'];
      const cWidths  = [40, 50, 180, 30, 60, 44, 111];
      const cRows    = d.detCarg.map(o => [
        truncate(String(o.id || '-'), 8),
        truncate(String(o.fecha || '-'), 10),
        truncate(String(o.soc || '-'), 32),
        n(o.q),
        truncate(String(o.un || '-'), 12),
        truncate(String(o.kt || '-'), 8),
        truncate(String(o.kv || '-'), 8),
      ]);
      y = table(doc, cHeaders, cRows, cWidths, margin, y,
        { colAlign: ['left','left','left','right','left','right','right'] });
    }

    // ════════════════════════════════════════════════════════════════════════
    // FOOTER en todas las páginas
    // ════════════════════════════════════════════════════════════════════════
    const totalPages = doc.bufferedPageRange().count;
    for (let i = 0; i < totalPages; i++) {
      doc.switchToPage(i);
      const pageH = doc.page.height;
      doc.rect(0, pageH - 24, pageW, 24).fill(C.headerBg);
      doc.fillColor('#94a3b8').fontSize(7).font('Helvetica')
         .text(`${ac} · ${semStr} · ${fIni} – ${fFin}`, margin, pageH - 16, { width: usableW - 60 })
         .text(`Pág. ${i + 1} / ${totalPages}`, margin, pageH - 16, { width: usableW, align: 'right' });
    }

    doc.end();
    const pdfBuffer = await streamToBuffer(doc);
    const pdfBase64 = pdfBuffer.toString('base64');
    const acSlug    = ac.replace(/[^a-zA-Z0-9]/g, '_');
    const fileName  = `Reporte_${acSlug}_S${semana || 'X'}.pdf`;

    console.log(`[generatePDF] OK: ${fileName} | ${Math.round(pdfBase64.length / 1024)}KB base64`);
    res.json({ ok: true, pdfBase64, fileName });

  } catch (err) {
    console.error('[generatePDF] Error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
};
