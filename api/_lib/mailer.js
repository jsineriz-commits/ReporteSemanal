// api/_lib/mailer.js
// Reemplaza GmailApp.sendEmail() y DriveApp de Apps Script.
//
// Variables de entorno:
//   SMTP_USER   — dirección Gmail para envío (ej: reportes@empresa.com)
//   SMTP_PASS   — App Password de Gmail (no la contraseña real)
//              Activar en: Google Account → Security → App passwords
//
// Para guardar en Drive, la Service Account debe tener acceso a las carpetas.
// Compartir cada carpeta de Drive con el email de la SA (Editor).

const nodemailer = require('nodemailer');
const { google }  = require('googleapis');
const { getAuthClient } = require('./sheets');

// ─── Transporter SMTP ─────────────────────────────────────────────────────────

let _transporter = null;

function getTransporter() {
  if (_transporter) return _transporter;
  _transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  });
  return _transporter;
}

/**
 * Envía un email con adjunto PDF.
 * Equivale a GmailApp.sendEmail() en Apps Script.
 *
 * @param {object} opts
 * @param {string} opts.to
 * @param {string} [opts.cc]
 * @param {string} opts.subject
 * @param {string} opts.text
 * @param {Buffer} opts.pdfBuffer
 * @param {string} opts.fileName
 */
async function sendEmail({ to, cc, subject, text, pdfBuffer, fileName }) {
  if (!to || !to.trim()) throw new Error('Destinatario vacío.');

  const mailOptions = {
    from:        process.env.SMTP_USER,
    to:          to.trim(),
    subject,
    text,
    attachments: [
      {
        filename:    fileName,
        content:     pdfBuffer,
        contentType: 'application/pdf',
      },
    ],
  };
  if (cc && cc.trim()) mailOptions.cc = cc.trim();

  await getTransporter().sendMail(mailOptions);
}

// ─── Drive API ────────────────────────────────────────────────────────────────

/**
 * Guarda un PDF en una carpeta de Drive.
 *
 * Usa OAuth2 con refresh token si la variable GOOGLE_DRIVE_REFRESH_TOKEN está
 * configurada en Vercel (funciona con carpetas de Drive personal).
 * Si no, usa el Service Account (solo funciona con Shared Drives / Unidades Compartidas).
 *
 * Para obtener el refresh token:
 *   1. https://developers.google.com/oauthplayground
 *   2. Gear icon → check "Use your own OAuth credentials"
 *      → ingresar GOOGLE_DRIVE_CLIENT_ID y GOOGLE_DRIVE_CLIENT_SECRET
 *   3. Step 1: seleccionar "Drive API v3" → https://www.googleapis.com/auth/drive
 *   4. Step 2: Exchange → copiar el "Refresh token"
 *   5. Agregar GOOGLE_DRIVE_REFRESH_TOKEN en Vercel → Settings → Environment Variables
 *
 * @param {string} folderId
 * @param {string} fileName
 * @param {Buffer} pdfBuffer
 */
async function saveToDrive(folderId, fileName, pdfBuffer) {
  if (!folderId || !folderId.trim()) return;

  let auth;

  if (process.env.GOOGLE_DRIVE_REFRESH_TOKEN) {
    // ── Camino OAuth2 (Drive personal, cualquier carpeta compartida) ──────────
    const client = new google.auth.OAuth2(
      process.env.GOOGLE_DRIVE_CLIENT_ID,
      process.env.GOOGLE_DRIVE_CLIENT_SECRET,
      'urn:ietf:wg:oauth:2.0:oob'
    );
    client.setCredentials({ refresh_token: process.env.GOOGLE_DRIVE_REFRESH_TOKEN });
    auth = client;
  } else {
    // ── Camino Service Account (solo Shared Drives / Unidades Compartidas) ────
    auth = await getAuthClient();
  }

  const drive = google.drive({ version: 'v3', auth });

  const { Readable } = require('stream');
  const stream = new Readable();
  stream.push(pdfBuffer);
  stream.push(null);

  await drive.files.create({
    supportsAllDrives: true, // necesario para Shared Drives
    requestBody: {
      name:    fileName,
      parents: [folderId.trim()],
    },
    media: {
      mimeType: 'application/pdf',
      body:     stream,
    },
    fields: 'id',
  });
}

module.exports = { sendEmail, saveToDrive };
