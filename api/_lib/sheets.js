// api/_lib/sheets.js
// Autenticación con Google Sheets API via Service Account.
// Variable de entorno: GOOGLE_SERVICE_ACCOUNT_KEY (JSON completo de la SA)
//                      SPREADSHEET_ID (ID del spreadsheet activo)

const { google } = require('googleapis');

const SPREADSHEET_ID = process.env.SPREADSHEET_ID || process.env.GOOGLE_SHEET_ID || '';

let _authClient = null;

async function getAuthClient() {
  if (_authClient) return _authClient;
  let credentials;
  const rawKey = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (rawKey && rawKey.trim().startsWith('{')) {
    credentials = JSON.parse(rawKey);
  } else {
    const emailVar = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || '';
    const privateKey = (process.env.GOOGLE_PRIVATE_KEY || '').trim().replace(/\\n/g, '\n').replace(/"/g, '');
    if (emailVar && privateKey) {
      credentials = { client_email: emailVar.trim(), private_key: privateKey };
    } else {
      credentials = {};
    }
  }

  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: [
      'https://www.googleapis.com/auth/spreadsheets.readonly',
      'https://www.googleapis.com/auth/drive',
    ],
  });
  _authClient = await auth.getClient();
  return _authClient;
}

/**
 * Lee todos los valores de una hoja (desde fila 1).
 * Equivale a sheet.getRange(1, 1, lastRow, lastCol).getValues() en Apps Script.
 * Fechas devueltas como número de serie (SERIAL_NUMBER).
 */
async function getSheetData(sheetName) {
  const auth   = await getAuthClient();
  const sheets = google.sheets({ version: 'v4', auth });
  const quoted = sheetName.includes(' ') ? `'${sheetName}'` : sheetName;
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId:     SPREADSHEET_ID,
      range:             quoted,
      valueRenderOption: 'UNFORMATTED_VALUE',
      dateTimeRenderOption: 'SERIAL_NUMBER',
    });
    return res.data.values || [];
  } catch (e) {
    console.error(`[sheets] getSheetData error ("${sheetName}"):`, e.message);
    return [];
  }
}

/**
 * Acceso seguro a una celda (las filas pueden ser más cortas que el máximo).
 */
function g(row, idx, def) {
  const v = (row && row.length > idx) ? row[idx] : undefined;
  if (v !== null && v !== undefined && v !== '') return v;
  return def !== undefined ? def : '';
}

module.exports = { getSheetData, g, getAuthClient, SPREADSHEET_ID };
