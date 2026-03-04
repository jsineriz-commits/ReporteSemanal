/**
 * Vercel Serverless API - Reporte Semanal 6.6 (Query Mapping Fix)
 * Basado en la query SQL del usuario para Card 3588 (BASE)
 */
const { google } = require('googleapis');

const METABASE_URL = (process.env.METABASE_URL || "https://bi.decampoacampo.com").replace(/\/$/, "");
const METABASE_USER = process.env.METABASE_USER || "";
const METABASE_PASS = process.env.METABASE_PASS || "";
const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID || process.env.SPREADSHEET_ID || "";

module.exports = async function handler(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    if (req.method === "OPTIONS") return res.status(200).end();

    const { op, ac, startTs, endTs } = req.query;

    try {
        const api = getSheetsApi();
        if (!api) throw new Error("Error de Auth Google");

        if (op === "config") {
            const response = await api.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: 'Comentarios_CRM!F2:F'
            });
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

        } else if (op === "report") {
            const sessId = await getMetabaseSession();
            // Fetch everything in parallel (Added 3480: ULT_ACT)
            const [baseRows, opsRows, actRows, sheetsData] = await Promise.all([
                queryCard(sessId, 3588), // BASE
                queryCard(sessId, 3584), // OPS
                queryCard(sessId, 3480), // ULT_ACT
                api.spreadsheets.values.batchGet({
                    spreadsheetId: SPREADSHEET_ID,
                    ranges: ['Comentarios_CRM!A2:H', 'Agenda_CRM!A2:E', 'Leads_CRM!A2:L', 'aux leads!A2:AS', 'SAC!A2:T', 'REMATES!A2:D']
                })
            ]);

            const vR = sheetsData.data.valueRanges;
            const r = processReportFixed(baseRows, opsRows, actRows, vR, ac, Number(startTs), Number(endTs));
            return res.status(200).json(r);
        }
    } catch (e) {
        return res.status(500).json({ error: e.message });
    }
};

function processReportFixed(baseRows, opsRows, actRows, vR, ac, startTs, endTs) {
    const dIn = new Date(startTs); dIn.setUTCHours(0, 0, 0, 0);
    const dFi = new Date(endTs); dFi.setUTCHours(23, 59, 59, 999);
    const dInAnt = new Date(startTs - 604800000); dInAnt.setUTCHours(0, 0, 0, 0);
    const dFiAnt = new Date(endTs - 604800000); dFiAnt.setUTCHours(23, 59, 59, 999);

    const fYMD = (d) => `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(d.getDate()).padStart(2, "0")}`;
    const dInicio = fYMD(dIn), dFin = fYMD(dFi), dInicioAnt = fYMD(dInAnt), dFinAnt = fYMD(dFiAnt);

    const r = {
        cab: 0, trop: 0, dT: [0, 0, 0, 0, 0, 0, 0], pCab: 0, pTrop: 0, cccNum: 0,
        cabV: 0, cabC: 0, trConc: 0, pConc: 0, socOps: 0, top5: [],
        tSG: 0, pTSG: 0, com: 0, age: 0, nuevas: 0, pNuevas: 0,
        socSinGestNum: 0, ssgTop5: [], actSemanal: [],
        sacs: [], pSac: 0, rem: 0, pRem: 0,
        carg: 0, cargProp: 0, cargAjen: 0
    };

    // BASE (Query 3588) Mapping
    const socS = {};
    for (const row of baseRows) {
        // Nombres de columna según el SQL del usuario
        const rowAC = String(row.AC_Vend || row.ac_vend || row.ac || "").trim();
        if (rowAC !== ac) continue;

        const f = row.fecha_publicaciones || row.fecha;
        const fs = f ? new Date(f).toISOString().split('T')[0].replace(/-/g, '') : "";
        if (!fs) continue;

        const est = String(row.ESTADO || row.estado || "").toUpperCase();
        const cab = Number(row.Cabezas || row.cabezas || row.cab || 0);

        if (fs >= dInicio && fs <= dFin) {
            r.cab += cab;
            r.trop++;
            if (est === "CONCRETADA" || est === "CONCRETADAS") r.cccNum++;
            const soc = row.sociedad_vendedora || row.sociedad;
            if (soc) socS[soc] = 1;
            const dIdx = new Date(f).getDay();
            r.dT[dIdx === 0 ? 6 : dIdx - 1]++;
        }
        if (fs >= dInicioAnt && fs <= dFinAnt) {
            r.pCab += cab;
            r.pTrop++;
        }
    }
    r.socOf = Object.keys(socS).length;
    r.ccc = r.trop > 0 ? Math.round((r.cccNum / r.trop) * 100) + "%" : "0%";

    // OPS (3584) - Mapping from User SQL
    const socOps = {}, allOps = [];
    for (const row of opsRows) {
        // Nombres de columna según el SQL de OPS (3584)
        const f = row.fecha_operacion || row.fecha;
        const fs = f ? new Date(f).toISOString().split('T')[0].replace(/-/g, '') : "";
        if (!fs) continue;

        const aV = String(row.asoc_com_vend || row.acv || "").trim();
        const aC = String(row.asoc_com_compra || row.acc || "").trim();
        const q = Number(row.Q || row.q || 0);

        if (aV === ac || aC === ac) {
            if (fs >= dInicio && fs <= dFin) {
                if (aV === ac) {
                    r.cabV += q;
                    const sV = row.RS_Vendedora || row.socv;
                    if (sV) socOps[sV] = 1;
                }
                if (aC === ac) {
                    r.cabC += q;
                    const sC = row.RS_Compradora || row.socc;
                    if (sC) socOps[sC] = 1;
                }
                r.trConc++;

                const fmtDate = (v) => {
                    let d = new Date(v);
                    return d.getDate().toString().padStart(2, '0') + '/' + (d.getMonth() + 1).toString().padStart(2, '0');
                };

                // ID, UN, RS_Vend, AC_Vend, RS_Comp, AC_Comp, Fecha, Q ... Cat
                allOps.push({
                    q,
                    d: [
                        row.ID || row.id || "-",
                        row.UN || row.un || "-",
                        row.RS_Vendedora || row.socv || "-",
                        aV,
                        row.RS_Compradora || row.socc || "-",
                        aC,
                        fmtDate(f),
                        q,
                        "",
                        "",
                        row.Cat || row.cat || "-"
                    ]
                });
            }
            if (fs >= dInicioAnt && fs <= dFinAnt) r.pConc += q;
        }
    }
    allOps.sort((a, b) => b.q - a.q); r.top5 = allOps.slice(0, 5); r.socOps = Object.keys(socOps).length;

    // SHEETS: CRM & OTHERS
    const dCom = vR[0].values || [], dAge = vR[1].values || [], dLeads = vR[2].values || [], dAux = vR[3].values || [], dSac = vR[4].values || [], dRem = vR[5].values || [];

    const parseSDate = v => { let d = new Date(v); return isNaN(d) ? null : fYMD(d); };
    const sG = {}, psG = {};
    for (const row of dCom) {
        if (String(row[5] || "").trim() !== ac) continue;
        const fs = parseSDate(row[3]);
        if (fs >= dInicio && fs <= dFin) { r.com++; if (row[0]) sG[row[0]] = 1; }
        if (fs >= dInicioAnt && fs <= dFinAnt) { if (row[0]) psG[row[0]] = 1; }
    }
    for (const row of dAge) {
        if (String(row[3] || "").trim() !== ac) continue;
        const fs = parseSDate(row[4]);
        if (fs >= dInicio && fs <= dFin) { r.age++; if (row[1]) sG[row[1]] = 1; }
        if (fs >= dInicioAnt && fs <= dFinAnt) { if (row[1]) psG[row[1]] = 1; }
    }
    r.tSG = Object.keys(sG).length; r.pTSG = Object.keys(psG).length;

    // Extras (SAC, REM)
    for (const row of dSac) {
        if (String(row[18] || "").trim() !== ac) continue;
        const fs = parseSDate(row[19]);
        if (fs >= dInicio && fs <= dFin) r.sacs.push({ s: row[1], f: new Date(row[19]).getTime(), e: row[3] });
        if (fs >= dInicioAnt && fs <= dFinAnt) r.pSac++;
    }
    const remIds = {}, premIds = {};
    for (const row of dRem) {
        if (String(row[2] || "").trim() !== ac) continue;
        const fs = parseSDate(row[1]);
        if (fs >= dInicio && fs <= dFin) remIds[row[3] || row[0]] = 1;
        if (fs >= dInicioAnt && fs <= dFinAnt) premIds[row[3] || row[0]] = 1;
    }
    r.rem = Object.keys(remIds).length; r.pRem = Object.keys(premIds).length;

    return r;
}

// Global Helpers (G-Auth, MB-Auth)
function getSheetsApi() {
    let email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || "";
    let key = process.env.GOOGLE_PRIVATE_KEY || "";
    if (email.startsWith('{')) {
        try { const p = JSON.parse(email); email = p.client_email; key = p.private_key; } catch (e) { }
    }
    if (!email || !key) return null;
    key = key.trim().replace(/\\n/g, '\n');
    if (!key.includes("---")) key = "-----BEGIN PRIVATE KEY-----\n" + key + "\n-----END PRIVATE KEY-----";
    const auth = new google.auth.GoogleAuth({ credentials: { client_email: email, private_key: key }, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    return google.sheets({ version: 'v4', auth });
}

async function getMetabaseSession() {
    const res = await fetch(`${METABASE_URL}/api/session`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ username: METABASE_USER, password: METABASE_PASS }) });
    if (!res.ok) throw new Error("MB Auth Fail");
    const d = await res.json(); return d.id;
}

async function queryCard(sessionId, cardId) {
    const res = await fetch(`${METABASE_URL}/api/card/${cardId}/query/json`, { method: "POST", headers: { "X-Metabase-Session": sessionId, "Content-Type": "application/json" } });
    if (!res.ok) throw new Error("MB Card " + cardId + " Fail");
    return res.json();
}
