/**
 * Vercel Serverless API - Reporte Semanal Híbrido 5.0 (Service Account Edition)
 * Fuentes: Metabase (BASE, OPS) + Google Sheets API (CRM, SAC, REMATES)
 */

const { google } = require('googleapis');

// ============ ENV VARS ============
const METABASE_URL = (process.env.METABASE_URL || "https://bi.decampoacampo.com").replace(/\/$/, "");
const METABASE_USER = process.env.METABASE_USER || "";
const METABASE_PASS = process.env.METABASE_PASS || "";
const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID || process.env.SPREADSHEET_ID || "";
const GOOGLE_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || "";
const GOOGLE_KEY = process.env.GOOGLE_PRIVATE_KEY || "";

// ============ GOOGLE AUTH ============
let sheetsApi = null;
if (GOOGLE_EMAIL && GOOGLE_KEY) {
    const auth = new google.auth.GoogleAuth({
        credentials: {
            client_email: GOOGLE_EMAIL,
            private_key: GOOGLE_KEY.replace(/\\n/g, '\n')
        },
        scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
    });
    sheetsApi = google.sheets({ version: 'v4', auth });
}

// ============ METABASE AUTH ============
async function getMetabaseSession() {
    const res = await fetch(`${METABASE_URL}/api/session`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: METABASE_USER, password: METABASE_PASS })
    });
    if (!res.ok) {
        const txt = await res.text();
        throw new Error(`Metabase auth failed (${res.status}): ${txt.slice(0, 100)}`);
    }
    const data = await res.json();
    return data.id;
}

async function queryCard(sessionId, cardId) {
    const res = await fetch(`${METABASE_URL}/api/card/${cardId}/query/json`, {
        method: "POST",
        headers: { "X-Metabase-Session": sessionId, "Content-Type": "application/json" }
    });
    if (!res.ok) throw new Error(`Metabase card ${cardId} query failed: ${res.status}`);
    return res.json();
}

// ============ DATE HELPERS ============
function toYMD(dateStr) {
    if (!dateStr) return "";
    const d = new Date(dateStr);
    if (isNaN(d.getTime())) return "";
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${y}${m}${dd}`;
}

function fmtDate(dateStr) {
    if (!dateStr) return "";
    const d = new Date(dateStr);
    if (isNaN(d.getTime())) return "";
    return `${String(d.getDate()).padStart(2, "0")}/${String(d.getMonth() + 1).padStart(2, "0")}/${d.getFullYear()}`;
}

function dayOfWeekIdx(dateStr) {
    const d = new Date(dateStr);
    const day = d.getDay();
    return day === 0 ? 6 : day - 1;
}

function parseSheetDate(val) {
    if (!val) return null;
    // Attempt to parse standard date or serial number
    const d = new Date(val);
    if (!isNaN(d.getTime())) return d;
    return null;
}

function formatDateYMD(date) {
    if (!date || isNaN(date.getTime())) return "";
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, "0");
    const d = String(date.getDate()).padStart(2, "0");
    return `${y}${m}${d}`;
}

// ============ MAIN HANDLER ============
module.exports = async function handler(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
    if (req.method === "OPTIONS") return res.status(200).end();

    const { op, ac, startTs, endTs } = req.query;

    try {
        if (op === "config") {
            return await handleConfig(res);
        } else if (op === "report") {
            return await handleReport(res, ac, Number(startTs), Number(endTs));
        } else {
            return res.status(400).json({ error: "Falta parámetro op (config|report)" });
        }
    } catch (e) {
        console.error(e);
        return res.status(500).json({ error: e.message || String(e) });
    }
};

// ============ CONFIG (Static or Dynamic) ============
async function handleConfig(res) {
    if (!sheetsApi || !SPREADSHEET_ID) {
        let missing = [];
        if (!sheetsApi) missing.push("Credenciales Google (Email/Private Key)");
        if (!SPREADSHEET_ID) missing.push("GOOGLE_SHEET_ID");
        return res.status(500).json({ error: "Faltan variables en Vercel: " + missing.join(", ") });
    }

    try {
        // Fetch ACs from Comentarios_CRM Column F (Index 5)
        // AND fetch Weeks from the Sheets somehow - assuming standard UA weeks
        // For now, let's fetch a list of ACs from the sheet
        const response = await sheetsApi.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: 'Comentarios_CRM!F2:F'
        });
        const rows = response.data.values || [];
        const acSet = new Set();
        for (let r of rows) if (r[0]) acSet.add(r[0].trim());
        const acs = Array.from(acSet).sort();

        // Hardcoded UA Weeks fallback or dynamic fetch if available
        // UA standard weeks for 2026 approx
        const semanas = [];
        const startOf2026 = new Date("2026-01-01T00:00:00Z").getTime();
        for (let i = 1; i <= 52; i++) {
            const s = startOf2026 + (i - 1) * 604800000;
            const e = s + 518400000; // +6 days
            semanas.push({ n: i, s: s, e: e, y: 2026 });
        }

        return res.status(200).json({ acs, semanas });
    } catch (e) {
        return res.status(500).json({ error: "Error de Google Sheets: " + e.message });
    }
}

// ============ REPORT (Metabase + Sheets API) ============
async function handleReport(res, ac, startTs, endTs) {
    if (!ac || !startTs || !endTs) return res.status(400).json({ error: "Faltan parámetros" });
    if (!sheetsApi || !SPREADSHEET_ID) return res.status(500).json({ error: "Google Sheets no configurado" });

    const dIn = new Date(startTs); dIn.setUTCHours(0, 0, 0, 0);
    const dFi = new Date(endTs); dFi.setUTCHours(23, 59, 59, 999);
    const dInAnt = new Date(startTs - 604800000); dInAnt.setUTCHours(0, 0, 0, 0);
    const dFiAnt = new Date(endTs - 604800000); dFiAnt.setUTCHours(23, 59, 59, 999);

    const dInicio = formatDateYMD(dIn);
    const dFin = formatDateYMD(dFi);
    const dInicioAnt = formatDateYMD(dInAnt);
    const dFinAnt = formatDateYMD(dFiAnt);

    const r = {
        cab: 0, trop: 0, dT: [0, 0, 0, 0, 0, 0, 0], pCab: 0, pTrop: 0, cccNum: 0,
        cabV: 0, cabC: 0, trConc: 0, pConc: 0, socOps: 0, top5: [],
        tSG: 0, pTSG: 0, com: 0, age: 0, nuevas: 0, pNuevas: 0,
        socSinGestNum: 0, ssgTop5: [], actSemanal: [],
        sacs: [], pSac: 0, rem: 0, pRem: 0,
        carg: 0, cargProp: 0, cargAjen: 0
    };

    // Parallel Fetching
    const sessId = await getMetabaseSession();
    const [baseRows, opsRows, sheetsData] = await Promise.all([
        queryCard(sessId, 3588), // BASE
        queryCard(sessId, 3584), // OPS
        sheetsApi.spreadsheets.values.batchGet({
            spreadsheetId: SPREADSHEET_ID,
            ranges: [
                'Comentarios_CRM!A2:H',
                'Agenda_CRM!A2:E',
                'Leads_CRM!A2:L',
                'aux leads!A2:AS',
                'SAC!A2:T',
                'REMATES!A2:D'
            ]
        })
    ]);

    const valueRanges = sheetsData.data.valueRanges;
    const dCom = valueRanges[0].values || [];
    const dAge = valueRanges[1].values || [];
    const dLeads = valueRanges[2].values || [];
    const dAux = valueRanges[3].values || [];
    const dSac = valueRanges[4].values || [];
    const dRem = valueRanges[5].values || [];

    // METABASE: BASE (Card 3588)
    const socS = {};
    for (const row of baseRows) {
        const rowAC = String(row.ac || row['AC'] || row[Object.keys(row)[5]] || "").trim();
        if (rowAC !== ac) continue;
        const rawFecha = row.fecha || row['Fecha'] || row[Object.keys(row)[1]];
        const fechaStr = toYMD(rawFecha);
        if (!fechaStr) continue;

        const est = String(row.estado || row['Estado'] || row[Object.keys(row)[3]] || "").trim().toUpperCase();
        const gF = Number(row.gf || row['GF'] || row[Object.keys(row)[6]]) || 0;
        const mot = String(row.motivo || row['Motivo'] || row[Object.keys(row)[15]] || "").trim();
        const cab = Number(row.cab || row['Cab'] || row[Object.keys(row)[4]]) || 0;

        let ok = false, esCCC = false;
        if (est === "CONCRETADA") { ok = true; esCCC = true; }
        else if (est === "PUBLICADO") { ok = true; esCCC = true; }
        else if (est === "NO CONCRETADAS" && mot !== "No la comercializo" && gF === 1) { ok = true; }
        if (!ok) continue;

        if (fechaStr >= dInicio && fechaStr <= dFin) {
            r.cab += cab; r.trop++;
            if (esCCC) r.cccNum++;
            const soc = row.sociedad || row['Sociedad'] || row[Object.keys(row)[2]];
            if (soc) socS[soc] = 1;
            r.dT[dayOfWeekIdx(rawFecha)]++;
        }
        if (fechaStr >= dInicioAnt && fechaStr <= dFinAnt) { r.pCab += cab; r.pTrop++; }
    }
    r.socOf = Object.keys(socS).length;
    r.ccc = r.trop > 0 ? Math.round((r.cccNum / r.trop) * 100) + "%" : "0%";

    // METABASE: OPS (Card 3584)
    const socOps = {};
    const allOps = [];
    for (const row of opsRows) {
        const rawFecha = row.fecha || row['Fecha'] || row[Object.keys(row)[2]];
        const fechaStr = toYMD(rawFecha);
        if (!fechaStr) continue;

        const aV = String(row.acv || row['ACV'] || row[Object.keys(row)[6]] || "").trim();
        const aC = String(row.acc || row['ACC'] || row[Object.keys(row)[8]] || "").trim();
        const q = Number(row.q || row['Q'] || row[Object.keys(row)[9]]) || 0;

        if (aV === ac || aC === ac) {
            if (fechaStr >= dInicio && fechaStr <= dFin) {
                if (aV === ac) { r.cabV += q; if (row.socv || row[Object.keys(row)[5]]) socOps[row.socv || row[Object.keys(row)[5]]] = 1; }
                if (aC === ac) { r.cabC += q; if (row.socc || row[Object.keys(row)[7]]) socOps[row.socc || row[Object.keys(row)[7]]] = 1; }
                r.trConc++;
                const colsVal = Object.values(row);
                allOps.push({ q: q, d: [colsVal[0] || "", colsVal[1] || "", colsVal[5] || "", colsVal[6] || "", colsVal[7] || "", colsVal[8] || "", fmtDate(rawFecha), q, colsVal[22] || "", colsVal[20] || "", colsVal[10] || ""] });
            }
            if (fechaStr >= dInicioAnt && fechaStr <= dFinAnt) r.pConc += q;
        }

        const rawCarga = row.fecha_carga || row['Fecha Carga'] || row[Object.keys(row)[18]];
        const fCarStr = toYMD(rawCarga);
        if (fCarStr && String(row.ac_carga || row[Object.keys(row)[22]] || "").trim() === ac) {
            if (fCarStr >= dInicio && fCarStr <= dFin) {
                r.carg++;
                if (aV === ac) r.cargProp++; else r.cargAjen++;
            }
        }
    }
    allOps.sort((a, b) => b.q - a.q);
    r.top5 = allOps.slice(0, 5);
    r.socOps = Object.keys(socOps).length;

    // SHEETS: CRM & SOCIALS
    const socGestSet = {};
    const psocGestSet = {};
    for (let row of dCom) {
        if (String(row[5] || "").trim() !== ac) continue;
        const f = parseSheetDate(row[3]); if (!f) continue;
        const fStr = formatDateYMD(f);
        if (fStr >= dInicio && fStr <= dFin) {
            if (String(row[7] || "").trim() === "") r.com++;
            if (row[0]) socGestSet[row[0]] = 1;
        }
        if (fStr >= dInicioAnt && fStr <= dFinAnt) {
            if (row[0]) psocGestSet[row[0]] = 1;
        }
    }
    for (let row of dAge) {
        if (String(row[3] || "").trim() !== ac) continue;
        const f = parseSheetDate(row[4]); if (!f) continue;
        const fStr = formatDateYMD(f);
        if (fStr >= dInicio && fStr <= dFin) {
            r.age++;
            if (row[1]) socGestSet[row[1]] = 1;
        }
        if (fStr >= dInicioAnt && fStr <= dFinAnt) {
            if (row[1]) psocGestSet[row[1]] = 1;
        }
    }
    r.tSG = Object.keys(socGestSet).length;
    r.pTSG = Object.keys(psocGestSet).length;

    // LEADS & SSG
    for (let row of dLeads) {
        if (String(row[2] || "").trim() !== ac) continue;
        if (String(row[3] || "").trim() !== "UA" || String(row[11] || "").trim() === "NO HABILITADO") continue;
        const f = parseSheetDate(row[1]); if (!f) continue;
        const fStr = formatDateYMD(f);
        if (fStr >= dInicio && fStr <= dFin) r.nuevas++;
        if (fStr >= dInicioAnt && fStr <= dFinAnt) r.pNuevas++;
    }

    const ssgAll = [];
    for (let row of dAux) {
        if (String(row[1] || "").trim() !== ac) continue; // Col B
        const isNuevo = (String(row[4] || "").trim().toUpperCase() === "NUEVO");
        if (isNuevo) r.socSinGestNum++;

        const faDate = parseSheetDate(row[2]);
        const obj = {
            kt: row[31] || "", kv: row[36] || "", soc: row[29] || "",
            fa: faDate ? fmtDate(faDate) : String(row[2] || ""),
            fu: row[37] || "", ug: row[40] || "", ua: row[39] || "", sg: row[38] || "",
            w: Number(row[22]) || 0,
            cDateStr: formatDateYMD(faDate)
        };
        if (isNuevo) ssgAll.push(obj);
        if (obj.cDateStr >= dInicio && obj.cDateStr <= dFin) {
            if (Number(row[32]) <= 6 && String(row[40] || "").toLowerCase() !== "sin gestión") r.actSemanal.push(obj);
        }
    }
    ssgAll.sort((a, b) => b.w - a.w);
    r.ssgTop5 = ssgAll.slice(0, 5);

    // SAC & REMATES
    for (let row of dSac) {
        if (String(row[18] || "").trim() !== ac) continue;
        const f = parseSheetDate(row[19]); if (!f) continue;
        const fStr = formatDateYMD(f);
        if (fStr >= dInicio && fStr <= dFin) r.sacs.push({ s: row[1] || "", f: f.getTime(), e: row[3] || "" });
        if (fStr >= dInicioAnt && fStr <= dFinAnt) r.pSac++;
    }
    const remIds = {}, pRemIds = {};
    for (let i = 0; i < dRem.length; i++) {
        let row = dRem[i]; if (String(row[2] || "").trim() !== ac) continue;
        const f = parseSheetDate(row[1]); if (!f) continue;
        const fStr = formatDateYMD(f);
        const id = row[3] || i;
        if (fStr >= dInicio && fStr <= dFin) remIds[id] = 1;
        if (fStr >= dInicioAnt && fStr <= dFinAnt) pRemIds[id] = 1;
    }
    r.rem = Object.keys(remIds).length; r.pRem = Object.keys(pRemIds).length;

    return res.status(200).json(r);
}
