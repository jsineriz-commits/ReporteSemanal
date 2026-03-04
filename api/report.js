/**
 * Vercel Serverless API - Reporte Semanal 9.0 (Parallel Chunked Loading)
 * Optimizado para velocidad: Carga individual por Card y filtrado en el servidor.
 */
const { google } = require('googleapis');

const METABASE_URL = (process.env.METABASE_URL || "https://bi.decampoacampo.com").replace(/\/$/, "");
const METABASE_USER = process.env.METABASE_USER || "";
const METABASE_PASS = process.env.METABASE_PASS || "";
const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID || process.env.SPREADSHEET_ID || "";

// Cache mínima para la sesión (evita re-login constante)
let cachedSession = { id: null, expiry: 0 };

module.exports = async function handler(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    if (req.method === "OPTIONS") return res.status(200).end();

    const { op, ac, startTs, endTs, cardId } = req.query;

    try {
        const api = getSheetsApi();

        // 1. Configuración inicial
        if (op === "config") {
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

        // 2. Fetch de una sola Card (Optimizado)
        if (op === "fetchCard") {
            if (!ac || !cardId) throw new Error("Faltan parámetros");
            const sessId = await getMetabaseSession();
            const rawData = await queryCard(sessId, cardId);

            // Filtramos en el servidor para enviar menos datos al navegador
            const filtered = filterDataByAC(rawData, cardId, ac);
            return res.status(200).json({ data: filtered, cardId });
        }

        // 3. Fetch de Google Sheets
        if (op === "fetchSheets") {
            const data = await api.spreadsheets.values.batchGet({
                spreadsheetId: SPREADSHEET_ID,
                ranges: ['Comentarios_CRM!A2:H', 'Agenda_CRM!A2:E', 'Leads_CRM!A2:L', 'aux leads!A2:AS', 'SAC!A2:T', 'REMATES!A2:D']
            });
            return res.status(200).json({ vR: data.data.valueRanges });
        }

    } catch (e) {
        return res.status(500).json({ error: e.message });
    }
};

/**
 * Filtra los datos masivos de Metabase para un solo AC antes de enviarlos.
 * Esto reduce el tamaño de la respuesta de megabytes a kilobytes.
 */
function filterDataByAC(rows, cardId, ac) {
    if (!Array.isArray(rows)) return [];
    const target = ac.toLowerCase().trim();

    return rows.filter(row => {
        // Mapeo de columnas según cada card
        if (cardId === "3588") { // BASE
            const acRow = String(row.AC_Vend || row.ac_vend || row.ac || "").trim().toLowerCase();
            return acRow === target;
        }
        if (cardId === "3584") { // OPS
            const aV = String(row.asoc_com_vend || "").trim().toLowerCase();
            const aC = String(row.asoc_com_compra || "").trim().toLowerCase();
            return aV === target || aC === target;
        }
        if (cardId === "3480") { // ULT_ACT
            const acRow = String(row.AC || row.ac || "").trim().toLowerCase();
            return acRow === target;
        }
        if (cardId === "3507") { // BCFULL (Esta suele ser por CUIT, dejamos todo por ahora o filtramos por AC si existe)
            return true;
        }
        return true;
    });
}

// Helpers
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
    const res = await fetch(`${METABASE_URL}/api/session`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ username: METABASE_USER, password: METABASE_PASS }) });
    const d = await res.json();
    cachedSession = { id: d.id, expiry: Date.now() + 3600000 };
    return d.id;
}

async function queryCard(sessionId, cardId) {
    const res = await fetch(`${METABASE_URL}/api/card/${cardId}/query/json`, {
        method: "POST",
        headers: { "X-Metabase-Session": sessionId, "Content-Type": "application/json" }
    });
    return res.ok ? res.json() : [];
}
