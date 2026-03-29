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
 * Equivale a DriveApp.getFolderById(id).createFile() en Apps Script.
 *
 * @param {string} folderId
 * @param {string} fileName
 * @param {Buffer} pdfBuffer
 */
async function saveToDrive(folderId, fileName, pdfBuffer) {
  if (!folderId || !folderId.trim()) return; // sin carpeta → no-op

  const auth  = await getAuthClient();
  const drive = google.drive({ version: 'v3', auth });

  const { Readable } = require('stream');
  const stream = new Readable();
  stream.push(pdfBuffer);
  stream.push(null);

  await drive.files.create({
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
