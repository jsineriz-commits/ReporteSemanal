/**
 * Vercel Serverless API - Reporte Semanal 10.0 (Global Turbo Cache + Fuzzy Match)
 * Caching global sin filtros y emparejamiento inteligente de ACs.
 */
const { google } = require('googleapis');

const METABASE_URL = (process.env.METABASE_URL || "https://bi.decampoacampo.com").replace(/\/$/, "");
const METABASE_USER = process.env.METABASE_USER || "";
const METABASE_PASS = process.env.METABASE_PASS || "";
const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID || process.env.SPREADSHEET_ID || "";

// --- CACHÉ GLOBAL EN MEMORIA (Persiste entre lambdas calientes) ---
let globalMemory = {
    session: { id: null, expiry: 0 },
    cards: {}, // Guarda el JSON completo de cada card
    lastFetch: {}, // Timestamp por card
};

const CARD_TTL = 15 * 60 * 1000; // 15 minutos de caché total

module.exports = async function handler(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    if (req.method === "OPTIONS") return res.status(200).end();

    const { op, ac, cardId } = req.query;

    try {
        const api = getSheetsApi();

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

        if (op === "fetchCard") {
            if (!ac || !cardId) throw new Error("Parámetros insuficientes");

            const now = Date.now();
            let rows = [];

            // 1. Revisar si la card está en memoria y es reciente
            if (globalMemory.cards[cardId] && (now - globalMemory.lastFetch[cardId] < CARD_TTL)) {
                console.log(`Serving Card ${cardId} from Global Cache`);
                rows = globalMemory.cards[cardId];
            } else {
                console.log(`Fetching Card ${cardId} from Metabase (Cache Expired/Empty)`);
                const sessId = await getMetabaseSession();
                rows = await queryCard(sessId, cardId);

                // Guardar en memoria global
                globalMemory.cards[cardId] = rows;
                globalMemory.lastFetch[cardId] = now;
            }

            // 2. Filtrar usando Fuzzy Match (Email vs Nombre)
            const filtered = filterDataFuzzy(rows, cardId, ac);

            return res.status(200).json({
                data: filtered,
                cardId,
                _cache: true,
                _age: Math.round((now - globalMemory.lastFetch[cardId]) / 1000) + "s",
                _totalRows: rows.length,
                _matchRows: filtered.length
            });
        }

        if (op === "fetchSheets") {
            const data = await api.spreadsheets.values.batchGet({
                spreadsheetId: SPREADSHEET_ID,
                ranges: ['Comentarios_CRM!A2:H', 'Agenda_CRM!A2:E', 'Leads_CRM!A2:L', 'aux leads!A2:AS', 'SAC!A2:T', 'REMATES!A2:D']
            });
            return res.status(200).json({ vR: data.data.valueRanges });
        }

    } catch (e) {
        console.error("API Error:", e);
        return res.status(500).json({ error: e.message });
    }
};

/**
 * Filtro inteligente que entiende que 'aacuna@decampoacampo.com' puede ser 'Alejandro Acuña'
 */
function filterDataFuzzy(rows, cardId, acEmail) {
    if (!Array.isArray(rows)) return [];
    const targetEmail = acEmail.toLowerCase().trim();
    const targetAlias = targetEmail.split('@')[0]; // 'aacuna'

    return rows.filter(row => {
        let nameInRow = "";
        if (cardId === "3588") nameInRow = row.AC_Vend || row.ac_vend || row.ac || "";
        else if (cardId === "3584") return fuzzyMatch(row.asoc_com_vend, targetEmail) || fuzzyMatch(row.asoc_com_compra, targetEmail);
        else if (cardId === "3480") nameInRow = row.AC || row.ac || "";
        else if (cardId === "3507") return true; // Stock suele ser general o por CUIT

        return fuzzyMatch(nameInRow, targetEmail);
    });
}

function fuzzyMatch(name, email) {
    if (!name || !email) return false;
    const n = name.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").trim();
    const e = email.toLowerCase().trim();

    // 1. Match Directo
    if (n === e) return true;

    // 2. Match de Prefijo (aacuna@... -> aacuna)
    const alias = e.split('@')[0];
    if (n.includes(alias) || alias.includes(n)) return true;

    // 3. Match de Apellido (Si el alias contiene el apellido)
    // Ejemplo: 'acuna' está en 'Alejandro Acuña'
    const nameParts = n.split(/\s+/);
    for (let p of nameParts) {
        if (p.length > 3 && alias.includes(p)) return true;
    }

    return false;
}

// Helpers unchanged
function getSheetsApi() {
    let e = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || "";
    let k = process.env.GOOGLE_PRIVATE_KEY || "";
    if (e.startsWith('{')) { try { const p = JSON.parse(e); e = p.client_email; k = p.private_key; } catch (x) { } }
    k = k.trim().replace(/\\n/g, '\n');
    if (!k.includes("---")) k = "-----BEGIN PRIVATE KEY-----\n" + k + "\n-----END PRIVATE KEY-----";
    return google.sheets({ version: 'v4', auth: new google.auth.GoogleAuth({ credentials: { client_email: e, private_key: k }, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] }) });
}

async function getMetabaseSession() {
    if (globalMemory.session.id && Date.now() < globalMemory.session.expiry) return globalMemory.session.id;
    const res = await fetch(`${METABASE_URL}/api/session`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ username: METABASE_USER, password: METABASE_PASS }) });
    const d = await res.json();
    globalMemory.session = { id: d.id, expiry: Date.now() + 3600000 };
    return d.id;
}

async function queryCard(sessionId, cardId) {
    const res = await fetch(`${METABASE_URL}/api/card/${cardId}/query/json`, { method: "POST", headers: { "X-Metabase-Session": sessionId, "Content-Type": "application/json" } });
    return res.ok ? res.json() : [];
}
