/**
 * Vercel Serverless API - Reporte Semanal 5.6 (Idiot-Proof Version)
 */
const { google } = require('googleapis');

module.exports = async function handler(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    if (req.method === "OPTIONS") return res.status(200).end();

    const { op, ac, startTs, endTs } = req.query;

    try {
        if (op === "config") {
            const api = getSheetsApi();
            const sId = process.env.GOOGLE_SHEET_ID || process.env.SPREADSHEET_ID;

            if (!api) throw new Error("No se pudo conectar con Google. Revisá tus variables de entorno.");
            if (!sId) throw new Error("Falta la variable GOOGLE_SHEET_ID en Vercel.");

            const response = await api.spreadsheets.values.get({
                spreadsheetId: sId,
                range: 'Comentarios_CRM!F2:F'
            });

            const rows = response.data.values || [];
            const acSet = new Set();
            for (let i = 0; i < rows.length; i++) {
                if (rows[i][0]) acSet.add(rows[i][0].trim());
            }

            const semanas = [];
            const startOf2026 = new Date("2026-01-01T00:00:00Z").getTime();
            for (let i = 1; i <= 52; i++) {
                const s = startOf2026 + (i - 1) * 604800000;
                const e = s + 518400000;
                semanas.push({ n: i, s: s, e: e, y: 2026 });
            }

            return res.status(200).json({ acs: Array.from(acSet).sort(), semanas });
        }

        // Lógica de Reporte simplificada para test
        return res.status(400).json({ error: "Especifique op=config" });

    } catch (e) {
        return res.status(500).json({
            error: "Error: " + e.message,
            tip: "Asegúrate de que la Service Account tenga permiso de Lector en el Google Sheet."
        });
    }
};

function getSheetsApi() {
    let email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || "";
    let key = process.env.GOOGLE_PRIVATE_KEY || "";

    // DETECTOR INTELIGENTE: Si pegó el JSON entero en el email
    if (email.trim().startsWith('{')) {
        try {
            const parsed = JSON.parse(email);
            if (parsed.client_email) email = parsed.client_email;
            if (parsed.private_key) key = parsed.private_key;
        } catch (e) {
            // Si no es JSON válido, probamos con Regex para extraer el email
            const emailMatch = email.match(/"client_email":\s*"([^"]+)"/);
            if (emailMatch) email = emailMatch[1];
            const keyMatch = email.match(/"private_key":\s*"([^"]+)"/);
            if (keyMatch) key = keyMatch[1];
        }
    }

    if (!email || !key) return null;

    try {
        email = email.trim().replace(/^"|"$/g, '');
        key = key.trim().replace(/^"|"$/g, '').replace(/\\n/g, '\n');

        if (!key.includes("---")) {
            key = "-----BEGIN PRIVATE KEY-----\n" + key + "\n-----END PRIVATE KEY-----";
        }

        const auth = new google.auth.GoogleAuth({
            credentials: { client_email: email, private_key: key },
            scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
        });
        return google.sheets({ version: 'v4', auth });
    } catch (e) {
        return null;
    }
}
