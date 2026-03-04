/**
 * Vercel Serverless API - Reporte Semanal 5.2 (Diagnóstico)
 */
const { google } = require('googleapis');

module.exports = async function handler(req, res) {
    // CORS
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    if (req.method === "OPTIONS") return res.status(200).end();

    const { op, ac, startTs, endTs } = req.query;

    // DIAGNÓSTICO INICIAL
    const vars = {
        MB_URL: !!process.env.METABASE_URL,
        MB_USER: !!process.env.METABASE_USER,
        SHEET_ID: !!(process.env.GOOGLE_SHEET_ID || process.env.SPREADSHEET_ID),
        G_EMAIL: !!process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
        G_KEY: !!process.env.GOOGLE_PRIVATE_KEY
    };

    try {
        if (op === "config") {
            const api = getSheetsApi();
            if (!api) throw new Error("No se pudo inicializar Google Auth. Revisá las variables G_EMAIL y G_KEY.");
            const sId = process.env.GOOGLE_SHEET_ID || process.env.SPREADSHEET_ID;
            if (!sId) throw new Error("GOOGLE_SHEET_ID no configurada.");

            const response = await api.spreadsheets.values.get({
                spreadsheetId: sId,
                range: 'Comentarios_CRM!F2:F'
            });
            const rows = response.data.values || [];
            const acSet = new Set();
            for (let i = 0; i < rows.length; i++) {
                if (rows[i][0]) acSet.add(rows[i][0].trim());
            }
            const acs = Array.from(acSet).sort();

            // Semanas UA 2026
            const semanas = [];
            const startOf2026 = new Date("2026-01-01T00:00:00Z").getTime();
            for (let i = 1; i <= 52; i++) {
                const s = startOf2026 + (i - 1) * 604800000;
                const e = s + 518400000;
                semanas.push({ n: i, s: s, e: e, y: 2026 });
            }
            return res.status(200).json({ acs, semanas, debug: vars });
        }

        // Si no hay OP o es report, devolvemos error amigable para debug
        return res.status(400).json({ error: "Operación no soportada o faltando", vars });

    } catch (e) {
        console.error("CRITICAL ERROR:", e);
        return res.status(500).json({
            error: "Error en Servidor: " + e.message,
            vars: vars,
            stack: e.stack ? "SÍ" : "NO"
        });
    }
};

function getSheetsApi() {
    const email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
    let key = process.env.GOOGLE_PRIVATE_KEY;
    if (!email || !key) return null;

    try {
        // Limpiador de clave privada (Vercel suele romper los saltos de línea)
        key = key.trim();
        if (key.startsWith('"') && key.endsWith('"')) key = key.slice(1, -1);
        key = key.replace(/\\n/g, '\n');

        const auth = new google.auth.GoogleAuth({
            credentials: { client_email: email, private_key: key },
            scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
        });
        return google.sheets({ version: 'v4', auth });
    } catch (e) {
        return null;
    }
}
