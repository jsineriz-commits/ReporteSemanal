// api/sendEmailWithPDF.js
// Reemplaza sendEmailWithPDF() de Apps Script.
// Recibe el PDF ya generado en el frontend (base64) y lo envía por email.
// Si hay folderId, también lo guarda en Drive.

const { sendEmail, saveToDrive } = require('./_lib/mailer');

module.exports = async (req, res) => {
  if (req.method !== 'POST') return res.status(405).end();

  const { comercial, email, cc, folderId, pdfBase64, fileName, bodyText } = req.body || {};

  if (!email || !String(email).trim()) {
    return res.status(400).json({ ok: false, error: 'Email vacío para ' + (comercial || '?') });
  }
  if (!pdfBase64) {
    return res.status(400).json({ ok: false, error: 'pdfBase64 vacío.' });
  }

  try {
    const pdfBuffer = Buffer.from(pdfBase64, 'base64');

    // Guardar en Drive (no-op si no hay folderId o falla)
    if (folderId && String(folderId).trim()) {
      try {
        await saveToDrive(String(folderId).trim(), fileName, pdfBuffer);
      } catch (driveErr) {
        console.warn('[api/sendEmailWithPDF] Drive error (no crítico):', driveErr.message);
      }
    }

    // Enviar email
    await sendEmail({
      to:         String(email).trim(),
      cc:         cc ? String(cc).trim() : '',
      subject:    'Reporte Semanal - ' + (comercial || ''),
      text:       bodyText || '',
      pdfBuffer,
      fileName:   fileName || 'reporte.pdf',
    });

    console.log('[api/sendEmailWithPDF] OK: mail enviado a', email, 'para', comercial);
    res.json({ ok: true });
  } catch (e) {
    console.error('[api/sendEmailWithPDF] ERROR [' + (comercial || '') + ']:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
};
