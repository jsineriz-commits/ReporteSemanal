/**
 * Vercel Serverless API - Reporte Semanal 11.0 (Direct Raw Proxy)
 * Deja de filtrar en el servidor para diagnosticar si el problema es el filtro o la conexión.
 */
const { google } = require('googleapis');

const METABASE_URL = (process.env.METABASE_URL || "https://bi.decampoacampo.com").replace(/\/$/, "");
const METABASE_USER = process.env.METABASE_USER || "";
const METABASE_PASS = process.env.METABASE_PASS || "";
const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID || process.env.SPREADSHEET_ID || "";

let cachedSession = { id: null, expiry: 0 };

module.exports = async function handler(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    if (req.method === "OPTIONS") return res.status(200).end();

    const { op, ac, cardId } = req.query;

    try {
        if (op === "config") {
            const api = getSheetsApi();
            const response = await api.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'Comentarios_CRM!F2:F' });
            const rows = response.data.values || [];
            const acSet = new Set();
            for (let i = 0; i < rows.length; i++) if (rows[i][0]) acSet.add(rows[i][0].trim());

            const semanas = [];
            const startOf2026 = new Date("2026-01-01T00:00:00Z").getTime();
            for (let i = 1; i <= 52; i++) {
                const s = startOf2026 + (i - 1) * 604800000;
                const e = s + 518400000;
                semanas.push({ n: i, s: s, e: e, y: 2026 });
            }
            return res.status(200).json({ acs: Array.from(acSet).sort(), semanas });
        }

        if (op === "fetchCard") {
            const sessId = await getMetabaseSession();
            // Traemos los datos crudos para ver qué está mandando Metabase
            const rows = await queryCard(sessId, cardId);

            // Enviamos una muestra y el total para debuggear en la consola del navegador
            return res.status(200).json({
                data: rows,
                cardId,
                _debug: {
                    totalRows: rows.length,
                    firstRow: rows[0] || null,
                    sampleNames: rows.slice(0, 5).map(r => r.AC_Vend || r.asoc_com_vend || r.AC || "N/A")
                }
            });
        }

        if (op === "fetchSheets") {
            const api = getSheetsApi();
            const data = await api.spreadsheets.values.batchGet({
                spreadsheetId: SPREADSHEET_ID,
                ranges: ['Comentarios_CRM!A2:H', 'Agenda_CRM!A2:E', 'Leads_CRM!A2:L', 'aux leads!A2:AS', 'SAC!A2:T', 'REMATES!A2:D']
            });
            return res.status(200).json({ vR: data.data.valueRanges });
        }

    } catch (e) {
        return res.status(200).json({ error: e.message, stack: e.stack });
    }
};

function getSheetsApi() {
    let e = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || "";
    let k = process.env.GOOGLE_PRIVATE_KEY || "";
    if (e.startsWith('{')) { try { const p = JSON.parse(e); e = p.client_email; k = p.private_key; } catch (x) { } }
    k = k.trim().replace(/\\n/g, '\n');
    if (!k.includes("---")) k = "-----BEGIN PRIVATE KEY-----\n" + k + "\n-----END PRIVATE KEY-----";
    return google.sheets({ version: 'v4', auth: new google.auth.GoogleAuth({ credentials: { client_email: e, private_key: k }, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] }) });
}

async function getMetabaseSession() {
    if (cachedSession.id && Date.now() < cachedSession.expiry) return cachedSession.id;
    const res = await fetch(`${METABASE_URL}/api/session`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: METABASE_USER, password: METABASE_PASS })
    });
    if (!res.ok) throw new Error("Metabase Login Failed: " + res.status);
    const d = await res.json();
    cachedSession = { id: d.id, expiry: Date.now() + 3600000 };
    return d.id;
}

async function queryCard(sessionId, cardId) {
    const res = await fetch(`${METABASE_URL}/api/card/${cardId}/query/json`, {
        method: "POST",
        headers: { "X-Metabase-Session": sessionId, "Content-Type": "application/json" }
    });
    if (!res.ok) throw new Error("Metabase Card " + cardId + " Failed: " + res.status);
    return res.json();
}
