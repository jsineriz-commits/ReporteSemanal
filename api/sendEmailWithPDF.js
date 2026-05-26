// api/sendEmailWithPDF.js
// Reemplaza sendEmailWithPDF() de Apps Script.
// Recibe el PDF ya generado en el frontend (base64) y lo envía por email.
// Si hay folderId, también lo guarda en Drive.
//
// MODO TEST: si el body incluye `testEmail`, el mail se redirige a esa
// dirección (ignorando email/cc reales) y el asunto lleva [TEST].
// Útil para probar el flujo de n8n sin enviar emails a los comerciales.

const { sendEmail, saveToDrive } = require('./_lib/mailer');

// Extrae el ID de carpeta aunque se pase el link completo de Drive
function extractFolderId(raw) {
  if (!raw) return '';
  const s = String(raw).trim();
  if (!s.includes('/')) return s;
  const m = s.match(/\/folders\/([a-zA-Z0-9_-]+)/);
  return m ? m[1] : s;
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') return res.status(405).end();

  const { comercial, email, cc, folderId, pdfBase64, fileName, bodyText, testEmail } = req.body || {};

  if (!email || !String(email).trim()) {
    return res.status(400).json({ ok: false, error: 'Email vacío para ' + (comercial || '?') });
  }
  if (!pdfBase64) {
    return res.status(400).json({ ok: false, error: 'pdfBase64 vacío.' });
  }

  // ── Modo test: redirigir a testEmail ─────────────────────────────────────
  const isTest     = !!testEmail && String(testEmail).includes('@');
  const toFinal    = isTest ? String(testEmail).trim() : String(email).trim();
  const ccFinal    = isTest ? ''                       : (cc ? String(cc).trim() : '');
  const subjectPfx = isTest ? '[TEST] '               : '';

  if (isTest) {
    console.log(`[api/sendEmailWithPDF] MODO TEST → redirigiendo email de "${email}" a "${toFinal}"`);
  }

  try {
    const pdfBuffer = Buffer.from(pdfBase64, 'base64');

    // Guardar en Drive (en modo test se omite para no crear archivos de prueba)
    let driveOk = false, driveError = null;
    const cleanFolderId = extractFolderId(folderId);
    if (cleanFolderId && !isTest) {
      try {
        await saveToDrive(cleanFolderId, fileName, pdfBuffer);
        driveOk = true;
        console.log('[api/sendEmailWithPDF] Drive OK: guardado en carpeta', cleanFolderId, 'para', comercial);
      } catch (driveErr) {
        driveError = driveErr.message;
        console.warn('[api/sendEmailWithPDF] Drive error (no crítico):', driveErr.message, '| folderId:', cleanFolderId);
      }
    } else if (!cleanFolderId) {
      driveError = 'folderId vacío o inválido';
      console.warn('[api/sendEmailWithPDF] Sin folderId para', comercial);
    } else {
      driveError = 'omitido en modo test';
    }

    // Enviar email
    await sendEmail({
      to:      toFinal,
      cc:      ccFinal,
      subject: `${subjectPfx}Reporte Semanal - ${comercial || ''}`,
      text:    bodyText || '',
      pdfBuffer,
      fileName: fileName || 'reporte.pdf',
    });

    console.log(`[api/sendEmailWithPDF] OK: mail enviado a ${toFinal} (${isTest ? 'TEST' : 'real'}) para ${comercial}`);
    res.json({ ok: true, driveOk, driveError, isTest });
  } catch (e) {
    console.error('[api/sendEmailWithPDF] ERROR [' + (comercial || '') + ']:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
};
