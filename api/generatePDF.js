// api/generatePDF.js
// Genera el reporte semanal como PDF usando PDFKit (puro Node.js — sin Chrome ni Puppeteer).
// Recibe { ac, startTs, endTs, semana } y devuelve { ok, pdfBase64, fileName }.

const PDFDocument = require('pdfkit');
const { getReport } = require('./_lib/logic');
const path = require('path');
const fs   = require('fs');

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
  headerBg:   '#f8fafc',   // encabezado (fondo suave)
  rowAlt:     '#f8fafc',   // fila alternada
  greenBg:    '#ecfdf5',
  greenTxt:   '#059669',
  redBg:      '#fef2f2',
  redTxt:     '#dc2626',
  blueBg:     '#eff6ff',
  blueTxt:    '#2563eb',
};

// ── Fuente activa (se sobreescribe con Inter si está disponible) ───────────────
let F  = 'Helvetica';      // Regular
let FB = 'Helvetica-Bold'; // Bold

// ── Helpers ──────────────────────────────────────────────────────────────────
function n(val) { return (Number(val) || 0).toLocaleString('es-AR'); }
function pct(val) { return typeof val === 'string' ? val : (Math.round(Number(val) || 0) + '%'); }
function truncate(str, len) { const s = String(str || ''); return s.length > len ? s.slice(0, len - 1) + '…' : s; }

/**
 * Colores de badge para Kt / Kv — replica exacta del frontend (getKtBgColor / getKtFgColor).
 * Paleta por rango de valor numérico:
 *   ≥ 10.001 → negro     / texto amarillo
 *   ≥  5.001 → amarillo  / texto negro
 *   ≥  1.001 → gris      / texto blanco
 *   ≥    501 → naranja   / texto blanco
 *   ≥      1 → rojo      / texto blanco
 *       0/—  → gris claro / texto gris
 */
function ktBg(raw) {
  const v = parseInt(String(raw || '').replace(/\D/g, '')) || 0;
  if (v >= 10001) return '#000000';
  if (v >=  5001) return '#fbbf24';
  if (v >=  1001) return '#6b7280';
  if (v >=   501) return '#d97706';
  if (v >=     1) return '#dc143c';
  return '#e5e7eb';
}
function ktFg(raw) {
  const v = parseInt(String(raw || '').replace(/\D/g, '')) || 0;
  if (v >= 10001) return '#fbbf24';
  if (v >=  5001) return '#000000';
  if (v >=     1) return '#ffffff';
  return '#6b7280';
}
/** Formatea un valor kt/kv: ≥1000 → "2.1k", <1000 → "500", 0/— → "—" */
function fmtK(raw) {
  const s = String(raw || '').trim();
  if (!s || s === '-' || s === '—') return '—';
  const v = parseInt(s.replace(/\D/g, '')) || 0;
  if (v === 0) return '—';
  if (v >= 1000) return (Math.round(v / 100) / 10).toLocaleString('es-AR') + 'k';
  return String(v);
}


function streamToBuffer(doc) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    doc.on('end',  () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);
  });
}

// ── Componentes de dibujo ────────────────────────────────────────────────────

/** Título de sección limpio — solo texto bold, sin bordes */
function sectionHeader(doc, title, y, _color) {
  doc.fillColor(C.textMain).fontSize(9).font(FB)
     .text(title, 48, y + 9, { width: doc.page.width - 96 });
  doc.fillColor(C.textMain);
  return y + 24;
}

/** Tabla genérica con encabezados y filas */
function table(doc, headers, rows, colWidths, startX, startY, opts = {}) {
  const rowH    = opts.rowH    || 16;
  const fontSize= opts.fontSize|| 7.5;
  const headerBg= opts.headerBg|| '#f8fafc';
  const total   = colWidths.reduce((a, b) => a + b, 0);
  let y = startY;

  // Encabezado
  doc.rect(startX, y, total, rowH).fill(headerBg);
  // Línea inferior del header
  doc.rect(startX, y + rowH - 0.5, total, 0.5).fill('#e5e7eb');
  let x = startX;
  headers.forEach((h, i) => {
    const align = (opts.headerAlign && opts.headerAlign[i]) || 'left';
    doc.fillColor(C.textMuted).fontSize(fontSize).font(FB)
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
      doc.fillColor(color).fontSize(fontSize).font(F)
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

/**
 * Dibuja un chip de etiqueta con fondo de color y texto.
 * @param {string} bg - Color de fondo
 * @param {string} fg - Color de texto
 * @param {string} text - Texto del chip
 * @returns {number} ancho real del chip dibujado
 */
function drawChip(doc, text, x, y, bg, fg, h = 13) {
  const fontSize = 6.5;
  // Estimación de ancho (PDFKit no tiene measure antes de dibujar)
  const est = Math.min(Math.max(text.length * 4.5 + 10, 28), 120);
  doc.roundedRect(x, y, est, h, 3).fill(bg);
  doc.fillColor(fg).fontSize(fontSize).font(FB)
     .text(text, x + 5, y + (h - fontSize) / 2, { width: est - 10, lineBreak: false });
  doc.fillColor(C.textMain);
  return est + 4; // gap
}

/**
 * Chip de variación vs semana anterior.
 * Devuelve { diff, label, bg, fg } para usarlo con drawChip.
 */
function varChipProps(current, previous, suffix = '') {
  const diff = (Number(current) || 0) - (Number(previous) || 0);
  const sign  = diff > 0 ? '+' : '';
  const label = diff === 0
    ? `= vs sem ant`
    : `${sign}${diff}${suffix} vs sem ant`;
  const bg = diff > 0 ? C.greenBg  : diff < 0 ? C.redBg   : '#eff6ff';
  const fg = diff > 0 ? C.greenTxt : diff < 0 ? C.redTxt  : C.blueTxt;
  return { diff, label, bg, fg };
}

/**
 * Tarjeta KPI grande (estilo imagen de referencia).
 *
 * Layout:
 *   ┌─────────────────────────────────────────┐
 *   │ LABEL (gris, 7pt)                       │
 *   │ 1.035    (num grande)    [+140 vs s.a.] │  ← número izq, varChip derecha
 *   │ [530 Publicadas/Ofrec.]                 │  ← chips apilados bajo el número
 *   │ [370 Concretadas      ]                 │
 *   │ [135 No Concretadas   ]                 │
 *   │ 11 Tropas · 10 Soc · 73% CCC  (footer) │
 *   └─────────────────────────────────────────┘
 *
 * infoChips: chips apilados verticalmente bajo el número
 * varChip:   chip único a la derecha (mismo row que el número)
 */
function kpiCardV2(doc, label, value, infoChips, footer, x, y, w, h, varChip) {
  // Fondo + borde
  doc.roundedRect(x, y, w, h, 4).fill(C.white);
  doc.roundedRect(x, y, w, h, 4).strokeColor(C.border).lineWidth(0.5).stroke();

  const pad = 10;

  // ── Fila 1: label ──────────────────────────────────────────────────────────
  doc.fillColor(C.textMuted).fontSize(7).font(FB)
     .text(label.toUpperCase(), x + pad, y + 8, { width: w - pad * 2, lineBreak: false });

  // ── Fila 2: número grande (izq) + varChip (derecha, centrado en esa fila) ──
  const numY    = y + 18;
  const numSize = 20;
  const numW    = w * 0.45;

  doc.fillColor(C.textMain).fontSize(numSize).font(FB)
     .text(value, x + pad, numY, { width: numW, lineBreak: false });

  if (varChip) {
    const chipH = 15;
    const vcW   = Math.min(Math.max(varChip.text.length * 4.5 + 16, 65), w * 0.46);
    const vcX   = x + w - vcW - pad;
    const vcY   = numY + (numSize - chipH) / 2 + 3; // alineado con el número
    doc.roundedRect(vcX, vcY, vcW, chipH, 3).fill(varChip.bg);
    doc.fillColor(varChip.fg).fontSize(6.5).font(FB)
       .text(varChip.text, vcX + 4, vcY + (chipH - 6.5) / 2,
         { width: vcW - 8, align: 'center', lineBreak: false });
  }

  // ── Filas 3+: chips informativos apilados bajo el número ───────────────────
  const chipH   = 13;
  const chipGap = 2;
  const chipMaxW = w * 0.55;
  let chipY = numY + numSize + 4;  // empieza DESPUÉS del número grande

  for (const chip of (infoChips || [])) {
    const chipW = Math.min(Math.max(chip.text.length * 4.3 + 14, 50), chipMaxW);
    doc.roundedRect(x + pad, chipY, chipW, chipH, 3).fill(chip.bg);
    doc.fillColor(chip.fg).fontSize(6).font(FB)
       .text(chip.text, x + pad + 5, chipY + (chipH - 6) / 2,
         { width: chipW - 6, lineBreak: false });
    chipY += chipH + chipGap;
  }

  // ── Footer ─────────────────────────────────────────────────────────────────
  if (footer) {
    doc.fillColor(C.textMuted).fontSize(6.5).font(F)
       .text(footer, x + pad, y + h - 11, { width: w - pad * 2, lineBreak: false });
  }

  doc.fillColor(C.textMain);
}


/**
 * Tarjeta KPI pequeña (fila inferior, sin número grande central).
 */
function kpiCardSmall(doc, label, value, chip, footer, x, y, w, h) {
  doc.roundedRect(x, y, w, h, 4).fill(C.white);
  doc.roundedRect(x, y, w, h, 4).strokeColor(C.border).lineWidth(0.5).stroke();

  // Título
  doc.fillColor(C.textMuted).fontSize(7).font(FB)
     .text(label.toUpperCase(), x + 10, y + 8, { width: w - 20 });

  // Valor
  doc.fillColor(C.textMain).fontSize(20).font(FB)
     .text(value, x + 10, y + 18, { width: w * 0.5, lineBreak: false });

  // Chip (derecha, alineado con valor)
  if (chip) {
    const chipX = x + w - chip.estW - 10;
    drawChip(doc, chip.text, chipX, y + 20, chip.bg, chip.fg, 13);
  }

  // Footer
  if (footer) {
    doc.fillColor(C.textMuted).fontSize(6.5).font(F)
       .text(footer, x + 10, y + h - 13, { width: w - 20, lineBreak: false });
  }

  doc.fillColor(C.textMain);
}

/** Chip de variación (↑ verde / ↓ rojo / — gris) — legacy, kept for other uses */
function chip(doc, current, previous, x, y, w, suffix = '') {
  const cur = Number(current) || 0;
  const prv = Number(previous) || 0;
  const diff = cur - prv;
  const color = diff > 0 ? C.success : diff < 0 ? C.danger : C.textMuted;
  const arrow = diff > 0 ? '▲' : diff < 0 ? '▼' : '–';
  const bg    = diff > 0 ? C.greenBg : diff < 0 ? C.redBg : '#f3f4f6';
  const label = `${arrow} ${Math.abs(diff)}${suffix} vs sem. ant.`;
  doc.rect(x, y, w, 12).fill(bg);
  doc.fillColor(color).fontSize(6.5).font(FB)
     .text(label, x + 4, y + 3, { width: w - 8 });
  doc.fillColor(C.textMain);
}

/** Mini barra horizontal */
function barRow(doc, label, value, maxVal, x, y, w, barColor) {
  const labelW = 80;
  const valW   = 30;
  const barW   = w - labelW - valW - 8;
  const fill   = maxVal > 0 ? Math.max(2, (value / maxVal) * barW) : 2;

  doc.fillColor(C.textMain).fontSize(7).font(F)
     .text(truncate(label, 14), x, y + 1, { width: labelW });
  // Fondo barra
  doc.rect(x + labelW, y + 3, barW, 7).fill('#e5e7eb');
  // Relleno barra
  doc.rect(x + labelW, y + 3, fill, 7).fill(barColor);
  // Valor
  doc.fillColor(C.textMain).fontSize(7).font(FB)
     .text(n(value), x + labelW + barW + 4, y + 1, { width: valW, align: 'right' });
  doc.fillColor(C.textMain);
  return y + 13;
}


// ── Función de renderizado del contenido ──────────────────────────────────────
// Dibuja todo el contenido en `doc` y retorna el y final (para medir altura).
// ctx: { ac, semStr, fIni, fFin, dias, pageW, margin, usableW, d }
function drawContent(doc, ctx) {
  const { ac, semStr, fIni, fFin, dias, pageW, margin, usableW, d } = ctx;

  // ════════════════════════════════════════════════════════════════════════
  // HEADER
  // ════════════════════════════════════════════════════════════════════════
  // Header: fondo blanco, línea inferior sutil
  doc.rect(0, 0, pageW, 68).fill(C.white);
  doc.fillColor(C.textMain).fontSize(20).font(FB)
     .text(ac, margin, 14, { width: usableW - 140 });
  doc.fillColor(C.textMuted).fontSize(9).font(F)
     .text('Reporte Semanal de Actividad', margin, 38, { width: usableW - 140 });
  // Badge semana (gris neutro)
  doc.roundedRect(pageW - margin - 118, 14, 118, 38, 6).fill('#f1f5f9');
  doc.fillColor(C.textMain).fontSize(11).font(FB)
     .text(semStr, pageW - margin - 118, 20, { width: 118, align: 'center' });
  doc.fillColor(C.textMuted).fontSize(7.5).font(F)
     .text(`${fIni} — ${fFin}`, pageW - margin - 118, 35, { width: 118, align: 'center' });
  // Línea separadora bajo el header
  doc.rect(0, 66, pageW, 1).fill('#e5e7eb');
  doc.fillColor(C.textMain);

  let y = 72;

    // ════════════════════════════════════════════════════════════════════════
    // KPIs PRINCIPALES  (nuevo diseño — inspirado en imagen de referencia)
    // ════════════════════════════════════════════════════════════════════════
    y = sectionHeader(doc, 'METRICAS PRINCIPALES', y, '#1e3a5f');
    y += 4;

    // ── Fila 1: 2 tarjetas grandes ─────────────────────────────────────────
    const bigCardH = 100;
    const bigGap   = 8;
    const bigW     = (usableW - bigGap) / 2;

    // Helper para generar props del chip de variación
    function vcProps(cur, prev, suffix = '') {
      const p = varChipProps(cur, prev, suffix);
      const estW = Math.min(Math.max(p.label.length * 4.5 + 10, 28), 110);
      return { text: p.label, bg: p.bg, fg: p.fg, estW };
    }
    function staticChip(text, bg, fg) {
      const estW = Math.min(Math.max(text.length * 4.5 + 10, 28), 110);
      return { text, bg, fg, estW };
    }

    // CABEZAS OFRECIDAS — chips izquierda (info) + chip derecha (variación)
    const cabPub  = Number(d.cabPublicadas || 0);
    const cabConc = Number(d.cabConc       || 0);
    const cabNoC  = Number(d.cabNoConc     || 0);
    const cabInfoChips = [];
    if (cabPub  > 0) cabInfoChips.push(staticChip(`${n(cabPub)} Publicadas/Ofrec.`, C.blueBg,  C.blueTxt));
    if (cabConc > 0) cabInfoChips.push(staticChip(`${n(cabConc)} Concretadas`,      C.greenBg, C.greenTxt));
    if (cabNoC  > 0) cabInfoChips.push(staticChip(`${n(cabNoC)} No Concretadas`,    C.redBg,   C.redTxt));
    const cabVarChip = vcProps(d.cab, d.pCab);  // chip de variación (derecha)
    const cabFooter = [
      d.trop      ? `${n(d.trop)} Tropas`              : null,
      d.socOf     ? `${n(d.socOf)} Sociedades`          : null,
      d.ccc       ? `${pct(d.ccc)} CCC`                : null,
      d.cotizadas ? `${pct(d.cotizadas)} Cotizadas`     : null,
    ].filter(Boolean).join(' · ');
    kpiCardV2(doc, 'Cabezas Ofrecidas', n(d.cab), cabInfoChips, cabFooter,
      margin, y, bigW, bigCardH, cabVarChip);

    // CABEZAS COMPRADAS
    const compVarChip = vcProps(d.cabC, d.pCabC);
    const compFooter  = [
      d.cabCWeekTrop ? `${n(d.cabCWeekTrop)} Tropas`    : null,
      d.cabCSocCount ? `${n(d.cabCSocCount)} Sociedades` : null,
    ].filter(Boolean).join(' · ');
    kpiCardV2(doc, 'Cabezas Compradas', n(d.cabC || 0), [], compFooter,
      margin + bigW + bigGap, y, bigW, bigCardH, compVarChip);

    y += bigCardH + 6;

    // ── Fila 2: 3 tarjetas pequeñas ────────────────────────────────────────
    const smCardH = 52;
    const smGap   = 6;
    const smW     = (usableW - smGap * 2) / 3;

    // CARGAS ASISTIDAS
    const cargVC   = vcProps(d.carg, d.cargPrevSem);
    const cargFoot = `${n(d.cargProp || 0)} Propias / ${n((d.carg || 0) - (d.cargProp || 0))} Ajenas`;
    kpiCardSmall(doc, 'Cargas Asistidas', n(d.carg), cargVC, cargFoot,
      margin, y, smW, smCardH);

    // SACs ENVIADOS
    const sacsCount = d.sacs?.length || 0;
    const sacsVC    = vcProps(sacsCount, d.sacsPrevSem);
    kpiCardSmall(doc, 'SACs Enviados', n(sacsCount), sacsVC, null,
      margin + smW + smGap, y, smW, smCardH);

    // SOC. GESTIONADAS
    const sgVC   = vcProps(d.tSG, d.tSGPrevSem);
    const sgFoot = `${n(d.com || 0)} Comentario / ${n(d.age || 0)} Agenda`;
    kpiCardSmall(doc, 'Soc. Gestionadas', n(d.tSG), sgVC, sgFoot,
      margin + (smW + smGap) * 2, y, smW, smCardH);

    y += smCardH + 6;

    // ════════════════════════════════════════════════════════════════════════
    // DISTRIBUCIÓN DIARIA  —  gráfico de barras agrupadas
    // ════════════════════════════════════════════════════════════════════════
    y = sectionHeader(doc, 'Como se distribuyeron las actividades en tu semana', y + 4, '#1e3a5f');
    y += 6;

    {
      // ── Series ────────────────────────────────────────────────────────────
      const SERIES = [
        { label: 'Tropas Ofr.',  data: d.dT        || Array(7).fill(0), color: C.success  }, // verde
        { label: 'Tropas Comp.', data: d.dCompras   || Array(7).fill(0), color: '#7c3aed' }, // púrpura
        { label: 'Cargas Asis.', data: d.dCargas    || Array(7).fill(0), color: C.primary  }, // azul
        { label: 'Soc. Gest.',   data: d.dGestion   || Array(7).fill(0), color: '#f59e0b' }, // ámbar
        { label: 'SACs',         data: d.dSacs      || Array(7).fill(0), color: C.teal    }, // teal
      ];
      const DAYS  = ['Sáb', 'Dom', 'Lun', 'Mar', 'Mié', 'Jue', 'Vie'];
      const nDays = 7;
      const nSer  = SERIES.length;

      // ── Dimensiones ────────────────────────────────────────────────────────
      const chartX   = margin + 22;       // espacio para eje Y
      const chartW   = usableW - 24;
      const chartH   = 90;
      const chartY   = y + 22;            // debajo de la leyenda
      const baseY    = chartY + chartH;   // línea base (eje X)

      const groupW   = chartW / nDays;    // ancho por día
      const barGap   = 1;                 // gap entre barras de la misma serie
      const groupPad = groupW * 0.15;     // padding a cada lado del grupo
      const barW     = Math.max(3, (groupW - groupPad * 2 - barGap * (nSer - 1)) / nSer);

      // ── Máximo global ──────────────────────────────────────────────────────
      let globalMax = 0;
      SERIES.forEach(s => s.data.forEach(v => { if (v > globalMax) globalMax = v; }));
      if (globalMax === 0) globalMax = 1; // evitar división por cero

      // ── Líneas guía horizontales (eje Y) ───────────────────────────────────
      const gridLines = 4;
      const step = Math.ceil(globalMax / gridLines) || 1;
      for (let gi = 0; gi <= gridLines; gi++) {
        const val  = gi * step;
        const gy   = baseY - (val / (step * gridLines)) * chartH;
        if (gy < chartY - 2) break;
        // Línea punteada gris
        doc.moveTo(chartX, gy).lineTo(chartX + chartW, gy)
           .strokeColor('#e5e7eb').lineWidth(0.4).dash(3, { space: 3 }).stroke().undash();
        // Etiqueta eje Y
        doc.fillColor(C.textLight).fontSize(5.5).font(F)
           .text(String(val), margin, gy - 3, { width: 20, align: 'right' });
      }

      // ── Barras por día ─────────────────────────────────────────────────────
      for (let di = 0; di < nDays; di++) {
        const groupX = chartX + di * groupW + groupPad;

        for (let si = 0; si < nSer; si++) {
          const val = Number(SERIES[si].data[di]) || 0;
          if (val === 0) continue; // no dibujar barra vacía

          const bx  = groupX + si * (barW + barGap);
          const bh  = Math.max(2, (val / (step * gridLines)) * chartH);
          const by  = baseY - bh;

          // Barra
          doc.roundedRect(bx, by, barW, bh, 1).fill(SERIES[si].color);

          // Etiqueta de valor encima
          doc.fillColor(C.textMain).fontSize(5.5).font(FB)
             .text(String(val), bx - 2, by - 8, { width: barW + 4, align: 'center', lineBreak: false });
        }

        // Etiqueta día (eje X)
        doc.fillColor(C.textMuted).fontSize(6.5).font(F)
           .text(DAYS[di], chartX + di * groupW, baseY + 3, { width: groupW, align: 'center' });
      }

      // ── Línea base (eje X) ──────────────────────────────────────────────────
      doc.moveTo(chartX, baseY).lineTo(chartX + chartW, baseY)
         .strokeColor(C.border).lineWidth(0.5).stroke();

      // ── Leyenda (arriba del gráfico) ────────────────────────────────────────
      const legDotR = 5;
      let lx = chartX;
      const ly = y + 3;
      SERIES.forEach(s => {
        // Círculo de color
        doc.circle(lx + legDotR / 2, ly + 4, legDotR / 2).fill(s.color);
        // Texto
        doc.fillColor(C.textMuted).fontSize(6.5).font(F)
           .text(s.label, lx + legDotR + 3, ly + 1, { lineBreak: false });
        lx += legDotR + 3 + s.label.length * 4 + 14;
      });

      y = baseY + 18; // avanzar debajo del gráfico + etiquetas X
    }

    // ════════════════════════════════════════════════════════════════════════
    // EVOLUCIÓN DE OPERACIONES (últimas semanas) — gráfico de línea
    // ════════════════════════════════════════════════════════════════════════
    if (d.operSemMesLabels?.length) {
      y = sectionHeader(doc, 'Evolución de tus operaciones en las últimas semanas', y + 2, '#1e3a5f');
      y += 6;

      {
        const labels  = d.operSemMesLabels;       // e.g. ['S17','S18','S19','S20']
        const vals    = d.operSemMesVals.map(v => Number(v) || 0);
        const nPts    = labels.length;

        // ── Dimensiones ──────────────────────────────────────────────────────
        const chartX  = margin + 28;              // margen izq para eje Y
        const chartW  = usableW - 30;
        const chartH  = 80;
        const chartY  = y + 8;                    // top del área del gráfico
        const baseY   = chartY + chartH;          // eje X

        const maxVal  = Math.max(...vals, 1);
        // Techo redondeado al siguiente múltiplo "limpio"
        const rawStep = maxVal / 4;
        const mag     = Math.pow(10, Math.floor(Math.log10(rawStep || 1)));
        const niceStep= Math.ceil(rawStep / mag) * mag || 1;
        const yMax    = niceStep * 4;

        // Función de proyección Y
        const py = v => baseY - Math.max(0, Math.min(1, v / yMax)) * chartH;
        // Función de proyección X (puntos equidistantes)
        const px = i => chartX + (i / (nPts - 1 || 1)) * chartW;

        // ── Grid horizontal ───────────────────────────────────────────────────
        for (let gi = 0; gi <= 4; gi++) {
          const gv  = gi * niceStep;
          const gy  = py(gv);
          doc.moveTo(chartX, gy).lineTo(chartX + chartW, gy)
             .strokeColor('#e5e7eb').lineWidth(0.4).dash(3, { space: 3 }).stroke().undash();
          doc.fillColor(C.textLight).fontSize(5.5).font(F)
             .text(String(gv), margin, gy - 3, { width: 26, align: 'right' });
        }

        // ── Área rellena bajo la línea ────────────────────────────────────────
        // Construir polígono: puntos de izq a der, luego vuelta por la base
        if (nPts >= 2) {
          doc.save();
          doc.moveTo(px(0), py(vals[0]));
          for (let i = 1; i < nPts; i++) doc.lineTo(px(i), py(vals[i]));
          doc.lineTo(px(nPts - 1), baseY)
             .lineTo(px(0), baseY)
             .closePath()
             .fillColor('#eff6ff')   // azul muy claro
             .fill();
          doc.restore();
        }

        // ── Línea principal ────────────────────────────────────────────────────
        if (nPts >= 2) {
          doc.moveTo(px(0), py(vals[0]));
          for (let i = 1; i < nPts; i++) doc.lineTo(px(i), py(vals[i]));
          doc.strokeColor(C.primary).lineWidth(1.8).stroke();
        }

        // ── Línea de promedio punteada ─────────────────────────────────────────
        const avg = vals.reduce((a, b) => a + b, 0) / (nPts || 1);
        const avgY = py(avg);
        doc.moveTo(chartX, avgY).lineTo(chartX + chartW, avgY)
           .strokeColor('#93c5fd').lineWidth(0.8).dash(4, { space: 4 }).stroke().undash();

        // ── Puntos + etiquetas ────────────────────────────────────────────────
        for (let i = 0; i < nPts; i++) {
          const cx   = px(i);
          const cy   = py(vals[i]);
          const isLast = i === nPts - 1;
          const r    = isLast ? 4.5 : 3.5;

          if (isLast) {
            // Punto actual: círculo relleno azul
            doc.circle(cx, cy, r).fill(C.primary);
            doc.circle(cx, cy, r + 1.5).strokeColor(C.white).lineWidth(1).stroke();
            doc.circle(cx, cy, r + 2.5).strokeColor(C.primary).lineWidth(0.8).stroke();
          } else {
            // Semanas pasadas: círculo hueco
            doc.circle(cx, cy, r).fill(C.white)
               .circle(cx, cy, r).strokeColor(C.primary).lineWidth(1.2).stroke();
          }

          // Etiqueta de valor (arriba del punto)
          doc.fillColor(isLast ? C.primary : C.textMain)
             .fontSize(isLast ? 7 : 6.5).font(FB)
             .text(String(vals[i]), cx - 14, cy - 13, { width: 28, align: 'center', lineBreak: false });
        }

        // ── Eje X — etiquetas de semana ────────────────────────────────────────
        for (let i = 0; i < nPts; i++) {
          const cx     = px(i);
          const isLast = i === nPts - 1;
          doc.fillColor(isLast ? C.primary : C.textMuted)
             .fontSize(6.5).font(isLast ? 'Helvetica-Bold' : 'Helvetica')
             .text(labels[i], cx - 18, baseY + 4, { width: 36, align: 'center', lineBreak: false });
          if (isLast) {
            doc.fillColor(C.primary).fontSize(6).font(FB)
               .text('Actual', cx - 18, baseY + 13, { width: 36, align: 'center', lineBreak: false });
          }
        }

        // ── Línea base eje X ────────────────────────────────────────────────────
        doc.moveTo(chartX, baseY).lineTo(chartX + chartW, baseY)
           .strokeColor(C.border).lineWidth(0.5).stroke();

        y = baseY + 26;
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // TOP NEGOCIOS  —  tabla rediseñada con badges y chips de color
    // ════════════════════════════════════════════════════════════════════════
    if (d.top5?.length) {
      y = sectionHeader(doc, 'Top Negocios de la Semana', y + 2, '#1e3a5f');
      y += 4;

      {
        // ── Columnas ─────────────────────────────────────────────────────────
        // KT_V | KV_V | ID | UN | SOC. VENDEDORA | FECHA | Q | KT_C | KV_C | SOC. COMPRADORA | CARGA
        const cols = [
          { label: 'KT',              w: 28 },
          { label: 'KV',              w: 24 },
          { label: 'ID',              w: 38 },
          { label: 'UN',              w: 46 },
          { label: 'SOC. VENDEDORA',  w: 100 },
          { label: 'FECHA',           w: 50 },
          { label: 'Q',               w: 24 },
          { label: 'KT',              w: 24 },
          { label: 'KV',              w: 24 },
          { label: 'SOC. COMPRADORA', w: 100 },
          { label: 'CARGA',           w: 0  },  // ancho calculado al final
        ];
        // El último toma el espacio restante
        const fixedW = cols.slice(0, -1).reduce((a, c) => a + c.w, 0);
        cols[cols.length - 1].w = usableW - fixedW;

        const rowH    = 18;
        const hdrH    = 14;
        const fontSize = 6.5;

        // Función helper para badge pequeño
        function badge(doc, text, bx, by, bg, fg, bw, bh = 12) {
          const tw = bw - 4;
          doc.roundedRect(bx, by, bw, bh, 2).fill(bg);
          doc.fillColor(fg).fontSize(5.5).font(FB)
             .text(truncate(String(text || '-'), 8), bx + 2, by + (bh - 5.5) / 2,
               { width: tw, align: 'center', lineBreak: false });
          doc.fillColor(C.textMain);
        }

        // Paleta para KT/KV (colores warm)
        // Colores de KT/KV: dinámicos por valor (replica frontend)

        // Paleta para UN — replica exacta de getUnTagStyle + getUnBase + getSideSuffix del frontend
        const UN_MAP = {
          'CRIA':       { bg: '#e39a3a', fg: '#111827' },
          'CRIA VEND':  { bg: '#e9bf8c', fg: '#111827' },
          'CRIA COMP':  { bg: '#ff7a00', fg: '#111827' },
          'CRIA DOBLE': { bg: '#d88a2d', fg: '#111827' },
          'INV':        { bg: '#d90000', fg: '#ffffff' },
          'INV VEND':   { bg: '#cf4125', fg: '#ffffff' },
          'INV COMP':   { bg: '#e6c4bd', fg: '#111827' },
          'INV DOBLE':  { bg: '#a80000', fg: '#ffffff' },
          'FAE':        { bg: '#7ea6e8', fg: '#ffffff' },
          'FAE VEND':   { bg: '#6d9eeb', fg: '#ffffff' },
          'FAE COMP':   { bg: '#3d85c6', fg: '#ffffff' },
          'FAE DOBLE':  { bg: '#4f81d8', fg: '#ffffff' },
          'MAG':        { bg: '#6aa84f', fg: '#ffffff' },
        };
        function getUnBase(raw) {
          const u = String(raw || '').toUpperCase();
          if (u.includes('MAG'))                       return 'MAG';
          if (u.includes('FAE') || u.includes('FAENA')) return 'FAE';
          if (u.includes('INV') || u.includes('INVER')) return 'INV';
          if (u.includes('CRIA'))                       return 'CRIA';
          return '';
        }
        function unStyle(unRaw, lado) {
          const base = getUnBase(unRaw);
          if (!base) return { bg: '#e5e7eb', fg: '#374151', label: unRaw || '-' };
          if (base === 'MAG') return { ...UN_MAP['MAG'], label: 'MAG' };
          let suf = '';
          if (lado === 'vend/comp') suf = 'DOBLE';
          else if (lado === 'vend') suf = 'VEND';
          else if (lado === 'comp') suf = 'COMP';
          const key = suf ? `${base} ${suf}` : base;
          return { ...(UN_MAP[key] || UN_MAP[base] || { bg: '#e5e7eb', fg: '#374151' }), label: key };
        }

        // ── Encabezado ────────────────────────────────────────────────────────
        let hx = margin;
        doc.rect(margin, y, usableW, hdrH).fill('#f8fafc');
        doc.rect(margin, y + hdrH - 0.5, usableW, 0.5).fill('#e5e7eb');
        cols.forEach(col => {
          doc.fillColor(C.textMuted).fontSize(fontSize - 0.5).font(FB)
             .text(col.label, hx + 3, y + (hdrH - (fontSize - 0.5)) / 2,
               { width: col.w - 6, lineBreak: false });
          hx += col.w;
        });
        y += hdrH;

        // ── Filas ─────────────────────────────────────────────────────────────
        d.top5.forEach((op, ri) => {
          const row  = op.d || [];
          const bg   = ri % 2 === 0 ? C.white : C.rowAlt;

          doc.rect(margin, y, usableW, rowH).fill(bg);
          // Separador inferior
          doc.rect(margin, y + rowH - 0.5, usableW, 0.5).fill(C.border);

          const id     = truncate(String(row[0] || '-'), 7);
          const unRaw  = String(row[1] || '-');
          const socV   = truncate(String(row[2] || '-'), 18);
          const socC   = truncate(String(row[4] || '-'), 18);
          const fecha  = truncate(String(row[6] || '-'), 10);
          const q      = n(row[7] ?? op.q);
          // Valores raw para colores, fmtK para display
          const ktVraw = op.kt;  const ktV = fmtK(op.kt);
          const kvVraw = op.kv;  const kvV = fmtK(op.kv);
          const ktCraw = op.ktC; const ktC = fmtK(op.ktC);
          const kvCraw = op.kvC; const kvC = fmtK(op.kvC);
          const carga  = String(row[8] || '').trim();

          const bPadY = (rowH - 12) / 2; // centrado vertical de badge

          let cx = margin;

          // KT vendedor (badge naranja)
          badge(doc, ktV, cx + 1, y + bPadY, ktBg(ktVraw), ktFg(ktVraw), cols[0].w - 2);
          cx += cols[0].w;

          // KV vendedor (badge gris)
          badge(doc, kvV, cx + 1, y + bPadY, ktBg(kvVraw), ktFg(kvVraw), cols[1].w - 2);
          cx += cols[1].w;

          // ID (texto simple)
          doc.fillColor(C.textMuted).fontSize(fontSize).font(F)
             .text(id, cx + 3, y + (rowH - fontSize) / 2, { width: cols[2].w - 6, lineBreak: false });
          cx += cols[2].w;

          // UN (badge color con sufijo de lado)
          const lado  = String(row[9] || '');
          const unSt  = unStyle(unRaw, lado);
          badge(doc, unSt.label, cx + 2, y + bPadY, unSt.bg, unSt.fg, cols[3].w - 4);
          cx += cols[3].w;

          // SOC. VENDEDORA
          doc.fillColor(C.textMain).fontSize(fontSize).font(F)
             .text(socV, cx + 3, y + (rowH - fontSize) / 2, { width: cols[4].w - 6, lineBreak: false });
          cx += cols[4].w;

          // FECHA
          doc.fillColor(C.textMuted).fontSize(fontSize).font(F)
             .text(fecha, cx + 3, y + (rowH - fontSize) / 2, { width: cols[5].w - 6, lineBreak: false });
          cx += cols[5].w;

          // Q (negrita)
          doc.fillColor(C.textMain).fontSize(fontSize).font(FB)
             .text(q, cx + 3, y + (rowH - fontSize) / 2, { width: cols[6].w - 6, align: 'right', lineBreak: false });
          cx += cols[6].w;

          // KT comprador
          badge(doc, ktC, cx + 1, y + bPadY, ktBg(ktCraw), ktFg(ktCraw), cols[7].w - 2);
          cx += cols[7].w;

          // KV comprador
          badge(doc, kvC, cx + 1, y + bPadY, ktBg(kvCraw), ktFg(kvCraw), cols[8].w - 2);
          cx += cols[8].w;

          // SOC. COMPRADORA
          doc.fillColor(C.textMain).fontSize(fontSize).font(F)
             .text(socC, cx + 3, y + (rowH - fontSize) / 2, { width: cols[9].w - 6, lineBreak: false });
          cx += cols[9].w;

          // CARGA: checkmark verde o X roja
          const hasCargas = carga === 'Sí' || carga === 'Si' || carga === '✓' || carga === 'true';
          const cargaSymbol = hasCargas ? '✓' : '✗';
          const cargaColor  = hasCargas ? C.success : C.danger;
          doc.fillColor(cargaColor).fontSize(8).font(FB)
             .text(cargaSymbol, cx, y + (rowH - 8) / 2, { width: cols[10].w, align: 'center', lineBreak: false });

          y += rowH;
        });

        // Borde exterior
        const totalH = hdrH + d.top5.length * rowH;
        doc.rect(margin, y - totalH, usableW, totalH).strokeColor(C.border).lineWidth(0.5).stroke();
        y += 4;
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // CRM — SOCIEDADES GESTIONADAS
    // ════════════════════════════════════════════════════════════════════════
    if (!d.hideCRM) {
      // Nueva página si queda poco espacio

      y = sectionHeader(doc, 'Sociedades Gestionadas - CRM', y + 4, '#4c1d95');
      y += 6;

      {
        // 3 tarjetas iguales
        const crmCardH = 70;
        const crmGap   = 8;
        const crmW     = (usableW - crmGap * 2) / 3;

        // Helper variación con estW
        function crmVC(cur, prev) {
          const p    = varChipProps(cur, prev);
          const estW = Math.min(Math.max(p.label.length * 4.5 + 10, 28), 90);
          return { text: p.label, bg: p.bg, fg: p.fg, estW };
        }

        // ── Tarjeta 1: SOC. ASIGNADAS ──────────────────────────────────────
        const asigVC   = crmVC(d.nuevas, d.pNuevas);
        const asigFoot = `${n(d.tSGAsigSem || 0)} asig. esta semana`;
        kpiCardSmall(doc, 'Soc. Asignadas', n(d.nuevas), asigVC, asigFoot,
          margin, y, crmW, crmCardH);

        // ── Tarjeta 2: SOC. GESTIONADAS ────────────────────────────────────
        {
          const cx = margin + crmW + crmGap;
          // Fondo + borde
          doc.roundedRect(cx, y, crmW, crmCardH, 4).fill(C.white);
          doc.roundedRect(cx, y, crmW, crmCardH, 4).strokeColor(C.border).lineWidth(0.5).stroke();

          // Título
          doc.fillColor(C.textMuted).fontSize(7).font(FB)
             .text('SOC. GESTIONADAS', cx + 10, y + 8, { width: crmW - 20 });

          // Número grande (izquierda)
          doc.fillColor(C.textMain).fontSize(20).font(FB)
             .text(n(d.tSG), cx + 10, y + 18, { width: crmW * 0.40, lineBreak: false });

          // Chip variación (DERECHA, mismo row que el número)
          const sgVC   = crmVC(d.tSG, d.pTSG);
          const vcW    = Math.min(Math.max(sgVC.text.length * 4.5 + 16, 28), crmW * 0.38);
          const vcX    = cx + crmW - vcW - 10;
          const vcY    = y + 18;
          doc.roundedRect(vcX, vcY, vcW, 15, 3).fill(sgVC.bg);
          doc.fillColor(sgVC.fg).fontSize(6.5).font(FB)
             .text(sgVC.text, vcX + 4, vcY + (15 - 6.5) / 2,
               { width: vcW - 8, align: 'center', lineBreak: false });

          // Chip "X Comentario / Y Agenda" (DEBAJO del número, izquierda)
          const comAgeText = `${n(d.com || 0)} Comentario / ${n(d.age || 0)} Agenda`;
          const comAgeW    = Math.min(Math.max(comAgeText.length * 4.0 + 14, 60), crmW * 0.62);
          const comAgeY    = y + 40;
          doc.roundedRect(cx + 10, comAgeY, comAgeW, 14, 3).fill('#dbeafe');
          doc.fillColor('#1d4ed8').fontSize(6).font(FB)
             .text(comAgeText, cx + 14, comAgeY + (14 - 6) / 2,
               { width: comAgeW - 8, lineBreak: false });
          doc.fillColor(C.textMain);

          // Footer
          const sgFoot = `${n(d.tSGAsigSem || 0)} asig. esta semana`;
          doc.fillColor(C.textMuted).fontSize(6.5).font(F)
             .text(sgFoot, cx + 10, y + crmCardH - 13, { width: crmW - 20, lineBreak: false });
        }

        // ── Tarjeta 3: SOC. SIN GESTIÓN ────────────────────────────────────
        const ssgVC   = crmVC(d.socSinGestNum, d.pSocSinGestNum);
        const ssgFoot = `${n(d.socSinGestAsigSem || 0)} asig. esta semana`;
        kpiCardSmall(doc, 'Soc. Sin Gestión', n(d.socSinGestNum), ssgVC, ssgFoot,
          margin + (crmW + crmGap) * 2, y, crmW, crmCardH);

        y += crmCardH + 6;
      }


      // ── Top Soc. Gestionadas ─────────────────────────────────────────────
      if (d.actSemanal?.length) {

        // Título con flecha
        doc.fillColor(C.textMain).fontSize(9).font(FB)
           .text('Top Soc Gestionadas de la Semana', margin, y + 2, { continued: true })
           .fillColor(C.textMuted).fontSize(9).font(F).text('  ›');
        y += 16;

        // Columnas: KT | KV | USUARIO-SOCIEDAD | FUENTE | TIPO | COMENTARIO
        const sgCols = [
          { label: 'KT',                w: 26 },
          { label: 'KV',                w: 26 },
          { label: 'USUARIO - SOCIEDAD',w: 90 },
          { label: 'FUENTE',            w: 46 },
          { label: 'TIPO',              w: 52 },
          { label: 'COMENTARIO',        w: 0  },
        ];
        const sgFixedW = sgCols.slice(0, -1).reduce((a, c) => a + c.w, 0);
        sgCols[sgCols.length - 1].w = usableW - sgFixedW;

        const sgHdrH = 13;
        const sgFontSz = 6.5;
        // Colores dinámicos por valor

        // Helper badge (reutilizable localmente)
        function sgBadge(text, bx, by, bg, fg, bw, bh = 13) {
          doc.roundedRect(bx, by, bw, bh, 2).fill(bg);
          doc.fillColor(fg).fontSize(5.5).font(FB)
             .text(truncate(String(text || '-'), 7), bx + 2, by + (bh - 5.5) / 2,
               { width: bw - 4, align: 'center', lineBreak: false });
          doc.fillColor(C.textMain);
        }

        // Encabezado gris muy suave (no pesado)
        doc.rect(margin, y, usableW, sgHdrH).fill('#f8fafc');
        doc.rect(margin, y + sgHdrH - 0.5, usableW, 0.5).fill(C.border);
        let hx2 = margin;
        sgCols.forEach(col => {
          doc.fillColor(C.textMuted).fontSize(5.8).font(FB)
             .text(col.label, hx2 + 4, y + (sgHdrH - 5.8) / 2,
               { width: col.w - 8, lineBreak: false });
          hx2 += col.w;
        });
        y += sgHdrH;

        // Filas con altura variable (por comentario)
        d.actSemanal.forEach((a, ri) => {
          const bg = ri % 2 === 0 ? C.white : C.rowAlt;

          const ktVraw = a.kt;  const ktV = fmtK(a.kt);
          const kvVraw = a.kv;  const kvV = fmtK(a.kv);
          const soc  = String(a.soc    || '-');
          const fue  = truncate(String(a.fuente || '-'), 8);
          const tipo = truncate(String(a.tipo   || '-'), 10);
          const cm   = String(a.cm || '-');

          // Calcular altura necesaria para el comentario (aprox 3.8px/char, ancho col)
          const comW    = sgCols[5].w - 8;
          const comChPL = Math.floor(comW / 3.8); // chars por línea aprox
          const comLines= Math.max(1, Math.ceil(cm.length / comChPL));
          const socLines= Math.max(1, Math.ceil(soc.length / 14));
          const minLines= Math.max(comLines, socLines, 1);
          const rowH2   = Math.max(20, minLines * 7.5 + 7);

          doc.rect(margin, y, usableW, rowH2).fill(bg);
          doc.rect(margin, y + rowH2 - 0.5, usableW, 0.5).fill(C.border);

          const bPadY2 = 4; // padding top badge
          let cx2 = margin;

          // KT
          sgBadge(ktV, cx2 + 2, y + bPadY2, ktBg(ktVraw), ktFg(ktVraw), sgCols[0].w - 4);
          cx2 += sgCols[0].w;
          // KV
          sgBadge(kvV, cx2 + 2, y + bPadY2, ktBg(kvVraw), ktFg(kvVraw), sgCols[1].w - 4);
          cx2 += sgCols[1].w;
          // USUARIO - SOCIEDAD (bold, multilinea)
          doc.fillColor(C.textMain).fontSize(sgFontSz).font(FB)
             .text(soc, cx2 + 4, y + 4, { width: sgCols[2].w - 8, lineBreak: true });
          cx2 += sgCols[2].w;
          // FUENTE
          doc.fillColor(C.textMuted).fontSize(sgFontSz).font(F)
             .text(fue, cx2 + 4, y + 4, { width: sgCols[3].w - 8, lineBreak: false });
          cx2 += sgCols[3].w;
          // TIPO
          doc.fillColor(C.textMuted).fontSize(sgFontSz).font(F)
             .text(tipo, cx2 + 4, y + 4, { width: sgCols[4].w - 8, lineBreak: false });
          cx2 += sgCols[4].w;
          // COMENTARIO (multilínea, texto normal)
          doc.fillColor(C.textMain).fontSize(sgFontSz).font(F)
             .text(cm, cx2 + 4, y + 4, { width: comW, lineBreak: true });

          y += rowH2;
        });

        // Borde exterior
        const sgTotalRows = d.actSemanal.length;
        // (solo línea superior e inferior, los laterales son implícitos)
        y += 6;
      }

      // ── Top Soc. Sin Gestión ─────────────────────────────────────────────
      if (d.ssgTop5?.length) {

        // Título con flecha
        doc.fillColor(C.textMain).fontSize(9).font(FB)
           .text('Top Soc. Sin Gestión', margin, y + 2, { continued: true })
           .fillColor(C.textMuted).fontSize(9).font(F).text('  ›');
        y += 16;

        // Columnas: USUARIO-SOCIEDAD | F. ASIG. | FUENTE | ULT ACT.
        const ssgCols = [
          { label: 'USUARIO - SOCIEDAD', w: 0   },  // flex
          { label: 'F. ASIG.',           w: 70  },
          { label: 'FUENTE',             w: 70  },
          { label: 'ULT ACT.',           w: 100 },
        ];
        const ssgFixedW = ssgCols.slice(1).reduce((a, c) => a + c.w, 0);
        ssgCols[0].w = usableW - ssgFixedW;

        const ssgHdrH  = 13;
        const ssgFontSz= 6.5;

        // Encabezado
        doc.rect(margin, y, usableW, ssgHdrH).fill('#f8fafc');
        doc.rect(margin, y + ssgHdrH - 0.5, usableW, 0.5).fill(C.border);
        let shx = margin;
        ssgCols.forEach(col => {
          doc.fillColor(C.textMuted).fontSize(5.8).font(FB)
             .text(col.label, shx + 4, y + (ssgHdrH - 5.8) / 2,
               { width: col.w - 8, lineBreak: false });
          shx += col.w;
        });
        y += ssgHdrH;

        // Filas
        d.ssgTop5.forEach((s, ri) => {
          const bg   = ri % 2 === 0 ? C.white : C.rowAlt;
          const rowH3= 18;
          doc.rect(margin, y, usableW, rowH3).fill(bg);
          doc.rect(margin, y + rowH3 - 0.5, usableW, 0.5).fill(C.border);

          const soc  = truncate(String(s.soc || '-'), 35);
          const fa   = truncate(String(s.fa  || '-'), 12);
          const fue  = truncate(String(s.fuente || '-'), 12);
          // Última actividad: usamos campo ug si existe, sino '-'
          const ug   = truncate(String(s.ug || s.sg || '-'), 12);

          const cy3 = y + (rowH3 - ssgFontSz) / 2;
          let scx = margin;

          // USUARIO - SOCIEDAD (bold)
          doc.fillColor(C.textMain).fontSize(ssgFontSz).font(FB)
             .text(soc, scx + 4, cy3, { width: ssgCols[0].w - 8, lineBreak: false });
          scx += ssgCols[0].w;
          // F. ASIG.
          doc.fillColor(C.textMuted).fontSize(ssgFontSz).font(F)
             .text(fa, scx + 4, cy3, { width: ssgCols[1].w - 8, lineBreak: false });
          scx += ssgCols[1].w;
          // FUENTE
          doc.fillColor(C.textMuted).fontSize(ssgFontSz).font(F)
             .text(fue, scx + 4, cy3, { width: ssgCols[2].w - 8, lineBreak: false });
          scx += ssgCols[2].w;
          // ULT ACT.
          doc.fillColor(C.textMuted).fontSize(ssgFontSz).font(F)
             .text(ug === '-' ? '-' : ug, scx + 4, cy3, { width: ssgCols[3].w - 8, lineBreak: false });

          y += rowH3;
        });
        y += 6;
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // SACs
    // ════════════════════════════════════════════════════════════════════════
    if (d.sacsTable?.length) {
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
    // DETALLE OPERADAS
    // ════════════════════════════════════════════════════════════════════════
    if (d.detC?.length) {
      y = sectionHeader(doc, `DETALLE OPERADAS (${d.detC.length})`, y + 4, '#1e3a5f');
      const dcHeaders = ['ID', 'FECHA', 'SOC. COMPRADORA', 'Q', 'UN', 'Kt', 'Kv'];
      const dcWidths  = [40, 50, 180, 30, 60, 44, 111];
      const dcRows    = d.detC.map(o => [
        truncate(String(o.id || '-'), 8),
        truncate(String(o.fecha || '-'), 10),
        truncate(String(o.soc || '-'), 32),
        n(o.q),
        truncate(String(o.un || '-'), 12),
        fmtK(o.kt),
        fmtK(o.kv),
      ]);
      y = table(doc, dcHeaders, dcRows, dcWidths, margin, y,
        { colAlign: ['left','left','left','right','left','right','right'] });
    }

    // ════════════════════════════════════════════════════════════════════════
    // DETALLE CARGAS
    // ════════════════════════════════════════════════════════════════════════
    if (d.detCarg?.length) {
      y = sectionHeader(doc, `DETALLE CARGAS (${d.detCarg.length})`, y + 4, '#1e3a5f');
      const cHeaders = ['ID', 'FECHA', 'SOCIEDAD', 'Q', 'UN', 'Kt', 'Kv'];
      const cWidths  = [40, 50, 180, 30, 60, 44, 111];
      const cRows    = d.detCarg.map(o => [
        truncate(String(o.id || '-'), 8),
        truncate(String(o.fecha || '-'), 10),
        truncate(String(o.soc || '-'), 32),
        n(o.q),
        truncate(String(o.un || '-'), 12),
        fmtK(o.kt),
        fmtK(o.kv),
      ]);
      y = table(doc, cHeaders, cRows, cWidths, margin, y,
        { colAlign: ['left','left','left','right','left','right','right'] });
    }

  y += 30; // padding inferior antes del footer
  return y;
} // ── fin drawContent ──────────────────────────────────────────────────────────

// ── Helper: crear doc con fuentes Inter registradas ───────────────────────────
function makeDoc(pageH, fontsDir, hasInter) {
  const doc = new PDFDocument({ size: [595, pageH], margin: 0, bufferPages: true });
  if (hasInter) {
    doc.registerFont('Inter-Regular', path.join(fontsDir, 'Inter-Regular.ttf'));
    doc.registerFont('Inter-Bold',    path.join(fontsDir, 'Inter-Bold.ttf'));
  }
  return doc;
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

    // Constantes de layout
    const semStr  = semana ? `Semana ${semana}` : '';
    const fIni    = new Date(Number(startTs)).toLocaleDateString('es-AR', { day: '2-digit', month: '2-digit', year: 'numeric', timeZone: 'America/Argentina/Buenos_Aires' });
    const fFin    = new Date(Number(endTs)).toLocaleDateString('es-AR',   { day: '2-digit', month: '2-digit', year: 'numeric', timeZone: 'America/Argentina/Buenos_Aires' });
    const dias    = ['Sáb', 'Dom', 'Lun', 'Mar', 'Mié', 'Jue', 'Vie'];
    const pageW   = 595;
    const margin  = 40;
    const usableW = pageW - margin * 2;
    const footerH = 28; // altura del footer

    // Configurar fuentes
    const fontsDir = path.join(__dirname, '_fonts');
    const hasInter = fs.existsSync(path.join(fontsDir, 'Inter-Regular.ttf'));
    if (hasInter) { F = 'Inter-Regular'; FB = 'Inter-Bold'; }
    else          { F = 'Helvetica';     FB = 'Helvetica-Bold'; }

    const ctx = { ac, semStr, fIni, fFin, dias, pageW, margin, usableW, d };

    // ── Pasada 1: medir la altura real del contenido ─────────────────────────
    const { PassThrough } = require('stream');
    const measureDoc = makeDoc(9999, fontsDir, hasInter);
    measureDoc.pipe(new PassThrough()); // descartar output
    const finalY = drawContent(measureDoc, ctx);
    measureDoc.end();

    // ── Pasada 2: renderizar en el doc con la altura exacta ──────────────────
    const pageH = Math.max(finalY + footerH, 400); // mínimo razonable
    const doc   = makeDoc(pageH, fontsDir, hasInter);

    drawContent(doc, ctx);

    // Footer: borde superior sutil, fondo muy suave
    doc.rect(0, pageH - footerH, pageW, 0.5).fill('#e5e7eb');
    doc.rect(0, pageH - footerH + 0.5, pageW, footerH - 0.5).fill('#f8fafc');
    doc.fillColor(C.textMuted).fontSize(7).font(F)
       .text(`${ac} · ${semStr} · ${fIni} – ${fFin}`, margin, pageH - footerH + 9, { width: usableW - 60 });
    doc.fillColor(C.textMuted).fontSize(7).font(F)
       .text(`Generado el ${new Date().toLocaleDateString('es-AR')}`, margin, pageH - footerH + 9, { width: usableW, align: 'right' });

    doc.end();
    const pdfBuffer = await streamToBuffer(doc);
    const pdfBase64 = pdfBuffer.toString('base64');
    const acSlug    = ac.replace(/[^a-zA-Z0-9]/g, '_');
    const fileName  = `Reporte_${acSlug}_S${semana || 'X'}.pdf`;

    console.log(`[generatePDF] OK: ${fileName} | ${Math.round(pdfBase64.length / 1024)}KB base64 | pageH: ${Math.round(pageH)}px`);
    res.json({ ok: true, pdfBase64, fileName });

  } catch (err) {
    console.error('[generatePDF] Error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
};
