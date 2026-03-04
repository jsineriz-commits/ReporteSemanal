/**
 * Vercel Serverless API - Reporte Semanal 5.3 (Key Fixer)
 */
const { google } = require('googleapis');

module.exports = async function handler(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    if (req.method === "OPTIONS") return res.status(200).end();

    const { op } = req.query;

    const dns = {
        G_EMAIL: !!process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
        G_KEY: !!process.env.GOOGLE_PRIVATE_KEY,
        S_ID: !!(process.env.GOOGLE_SHEET_ID || process.env.SPREADSHEET_ID)
    };

    try {
        if (op === "config") {
            const api = getSheetsApi();
            const sId = process.env.GOOGLE_SHEET_ID || process.env.SPREADSHEET_ID;

            if (!api) throw new Error("Error crítico: La clave privada (GOOGLE_PRIVATE_KEY) no pudo ser decodificada. Revisá que esté completa en Vercel.");
            if (!sId) throw new Error("Falta la variable GOOGLE_SHEET_ID.");

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

        return res.status(400).json({ error: "Especifique op=config para empezar", debug: dns });

    } catch (e) {
        const email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || "";
        return res.status(500).json({
            error: "Error en Servidor: " + e.message,
            email_debug: email ? (email.slice(0, 4) + "..." + email.slice(-10)) : "Vacío",
            ayuda: "Verificá que el email coincida con el client_email de tu JSON de Google."
        });
    }
};

function getSheetsApi() {
    let email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
    let key = process.env.GOOGLE_PRIVATE_KEY;
    if (!email || !key) return null;

    try {
        // LIMPIEZA DE EMAIL
        email = email.trim();
        if (email.startsWith('"') && email.endsWith('"')) email = email.slice(1, -1);
        if (email.startsWith("'") && email.endsWith("'")) email = email.slice(1, -1);

        // SUPER LIMPIADOR DE CLAVE RSA/PKCS8
        key = key.trim();

        // Quitar comillas si el usuario copió el valor del JSON con comillas
        if (key.startsWith('"') && key.endsWith('"')) key = key.slice(1, -1);
        if (key.startsWith("'") && key.endsWith("'")) key = key.slice(1, -1);

        // Corregir escapes de saltos de línea (el error técnico venía de aquí)
        // Reemplazamos tanto la cadena literal "\n" como los escapados "\\"
        key = key.replace(/\\n/g, '\n');

        // Asegurar encabezados correctos si faltan
        if (!key.includes("---")) {
            key = "-----BEGIN PRIVATE KEY-----\n" + key + "\n-----END PRIVATE KEY-----";
        }

        const auth = new google.auth.GoogleAuth({
            credentials: {
                client_email: email,
                private_key: key
            },
            scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
        });
        return google.sheets({ version: 'v4', auth });
    } catch (e) {
        console.error("Auth init failed:", e.message);
        return null;
    }
}
