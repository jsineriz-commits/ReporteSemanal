// api/generatePDF.js
// Genera el PDF de un reporte semanal navegando la propia app con Puppeteer.
// Recibe { ac, startTs, endTs, semana } y devuelve { ok, pdfBase64, fileName }.
// Usa @sparticuz/chromium (~45MB comprimido) optimizado para entornos serverless.

const chromium = require('@sparticuz/chromium');
const puppeteer = require('puppeteer-core');

module.exports = async (req, res) => {
  if (req.method !== 'POST') return res.status(405).end();

  const { ac, startTs, endTs, semana } = req.body || {};

  if (!ac || !startTs || !endTs) {
    return res.status(400).json({
      ok: false,
      error: 'Faltan parámetros: ac, startTs, endTs',
    });
  }

  const APP_URL = process.env.APP_URL || (() => {
    // Auto-detectar la URL base desde el propio request
    // → funciona en local (http://localhost:4000) y en Vercel (https://tu-app.vercel.app)
    const proto = req.headers['x-forwarded-proto'] || (req.socket?.encrypted ? 'https' : 'http');
    const host  = req.headers['x-forwarded-host'] || req.headers.host || 'localhost:4000';
    return `${proto}://${host}`;
  })();

  let browser;
  try {
    console.log(`[generatePDF] Iniciando para: ${ac} | sem: ${semana || '?'}`);

    browser = await puppeteer.launch({
      args: chromium.args,
      defaultViewport: { width: 1280, height: 900 },
      executablePath: await chromium.executablePath(),
      headless: chromium.headless,
    });

    const page = await browser.newPage();

    // Construir URL con parámetros para pre-selección automática
    // ?pdfMode=1 oculta la navbar y los controles en el PDF
    const url = `${APP_URL}/?ac=${encodeURIComponent(ac)}&startTs=${startTs}&endTs=${endTs}&pdfMode=1`;
    console.log(`[generatePDF] Navegando a: ${url}`);

    await page.goto(url, { waitUntil: 'networkidle0', timeout: 45000 });

    // Esperar que el reporte esté visible (autoGen() lo renderiza al detectar los params)
    await page.waitForFunction(
      () => {
        const rpt = document.getElementById('rpt');
        return rpt && rpt.style.display !== 'none' && rpt.innerHTML.trim().length > 100;
      },
      { timeout: 30000 }
    );

    // Esperar render de los gráficos Canvas (requestAnimationFrame cycles)
    await new Promise(r => setTimeout(r, 2500));

    console.log(`[generatePDF] Reporte renderizado, capturando PDF...`);

    const pdfBuffer = await page.pdf({
      printBackground: true,
      width: '1030px',   // PDF_WIDTH (980px) + márgenes
      height: 'auto',
      margin: { top: '10px', right: '10px', bottom: '10px', left: '10px' },
      pageRanges: '1',
    });

    const semStr = semana ? String(semana) : (() => {
      // Inferir número de semana del año desde endTs si no se pasó
      const d = new Date(Number(endTs));
      const startOfYear = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
      return String(Math.ceil(((d - startOfYear) / 86400000 + startOfYear.getUTCDay() + 1) / 7));
    })();

    const fileName = `Reporte_${ac.replace(/\s+/g, '_')}_S${semStr}.pdf`;
    const pdfBase64 = pdfBuffer.toString('base64');

    console.log(`[generatePDF] OK: ${fileName} | ${Math.round(pdfBase64.length / 1024)}KB base64`);
    res.json({ ok: true, pdfBase64, fileName });

  } catch (e) {
    console.error(`[generatePDF] ERROR [${ac}]:`, e.message);
    res.status(500).json({ ok: false, error: e.message });
  } finally {
    if (browser) {
      await browser.close();
    }
  }
};
