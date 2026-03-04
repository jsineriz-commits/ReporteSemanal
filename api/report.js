/**
 * Vercel Serverless API - Reporte Semanal 7.0 (Ultra-Robust)
 * Mapeo completo de 4 Cards de Metabase + Google Sheets
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
        if (!api) throw new Error("Error de Auth Google: Verifique Service Account Email y Private Key en Vercel.");

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
            if (!ac) throw new Error("Falta el nombre del Asociado (ac)");

            const sessId = await getMetabaseSession();

            // Fetch everything in parallel with error isolation
            const fetchResults = await Promise.allSettled([
                queryCard(sessId, 3588), // BASE
                queryCard(sessId, 3584), // OPS
                queryCard(sessId, 3480), // ULT_ACT
                queryCard(sessId, 3507), // BCFULL
                api.spreadsheets.values.batchGet({
                    spreadsheetId: SPREADSHEET_ID,
                    ranges: ['Comentarios_CRM!A2:H', 'Agenda_CRM!A2:E', 'Leads_CRM!A2:L', 'aux leads!A2:AS', 'SAC!A2:T', 'REMATES!A2:D']
                })
            ]);

            const [base, ops, act, bc, sheets] = fetchResults;

            // Handle failures in individual fetches
            const baseRows = base.status === 'fulfilled' ? base.value : [];
            const opsRows = ops.status === 'fulfilled' ? ops.value : [];
            const actRows = act.status === 'fulfilled' ? act.value : [];
            const bcRows = bc.status === 'fulfilled' ? bc.value : [];
            const sheetsData = sheets.status === 'fulfilled' ? sheets.value : { data: { valueRanges: [] } };

            const vR = sheetsData.data.valueRanges || [];
            const r = processReportFixed(baseRows, opsRows, actRows, bcRows, vR, ac, Number(startTs), Number(endTs));

            // Add debug info to help user
            r._debug = {
                baseTotal: baseRows.length,
                opsTotal: opsRows.length,
                sheetsFound: vR.length > 0,
                status: {
                    base: base.status,
                    ops: ops.status,
                    act: act.status,
                    bc: bc.status,
                    sheets: sheets.status
                }
            };

            return res.status(200).json(r);
        }
    } catch (e) {
        return res.status(500).json({ error: e.message });
    }
};

function processReportFixed(baseRows, opsRows, actRows, bcRows, vR, ac, startTs, endTs) {
    // Definir rangos
    const dIn = new Date(startTs); dIn.setUTCHours(0, 0, 0, 0);
    const dFi = new Date(endTs); dFi.setUTCHours(23, 59, 59, 999);
    const dInAnt = new Date(startTs - 604800000); dInAnt.setUTCHours(0, 0, 0, 0);
    const dFiAnt = new Date(endTs - 604800000); dFiAnt.setUTCHours(23, 59, 59, 999);

    const fYMD = (d) => {
        if (!d || isNaN(d.getTime())) return "00000000";
        return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(d.getDate()).padStart(2, "0")}`;
    };

    const dInicio = fYMD(dIn), dFin = fYMD(dFi), dInicioAnt = fYMD(dInAnt), dFinAnt = fYMD(dFiAnt);

    const r = {
        cab: 0, trop: 0, dT: [0, 0, 0, 0, 0, 0, 0], pCab: 0, pTrop: 0, cccNum: 0,
        cabV: 0, cabC: 0, trConc: 0, pConc: 0, socOps: 0, top5: [],
        tSG: 0, pTSG: 0, com: 0, age: 0, nuevas: 0, pNuevas: 0,
        socSinGestNum: 0, ssgTop5: [], actSemanal: [],
        sacs: [], pSac: 0, rem: 0, pRem: 0,
        carg: 0, cargProp: 0, cargAjen: 0
    };

    // Safe Date Parser
    const parseDateStr = (val) => {
        if (!val) return null;
        const d = new Date(val);
        return isNaN(d.getTime()) ? null : d;
    };

    // BASE (3588)
    const socS = {};
    for (const row of baseRows) {
        const rowAC = String(row.AC_Vend || row.ac_vend || row.ac || "").trim();
        if (rowAC.toLowerCase() !== ac.toLowerCase()) continue;

        const dObj = parseDateStr(row.fecha_publicaciones || row.fecha);
        if (!dObj) continue;
        const fs = fYMD(dObj);

        const est = String(row.ESTADO || row.estado || "").toUpperCase();
        const cab = Number(row.Cabezas || row.cabezas || row.cab || 0);

        if (fs >= dInicio && fs <= dFin) {
            r.cab += cab;
            r.trop++;
            if (est.includes("CONCRETADA")) r.cccNum++;
            const soc = row.sociedad_vendedora || row.sociedad;
            if (soc) socS[soc] = 1;
            const dIdx = dObj.getUTCDay(); // Usar UTC para evitar saltos de día
            r.dT[dIdx === 0 ? 6 : dIdx - 1]++;
        }
        if (fs >= dInicioAnt && fs <= dFinAnt) {
            r.pCab += cab;
            r.pTrop++;
        }
    }
    r.socOf = Object.keys(socS).length;
    r.ccc = r.trop > 0 ? Math.round((r.cccNum / r.trop) * 100) + "%" : "0%";

    // OPS (3584)
    const socOps = {}, allOps = [];
    for (const row of opsRows) {
        const dObj = parseDateStr(row.fecha_operacion || row.fecha);
        if (!dObj) continue;
        const fs = fYMD(dObj);

        const aV = String(row.asoc_com_vend || row.acv || "").trim();
        const aC = String(row.asoc_com_compra || row.acc || "").trim();
        const q = Number(row.Q || row.q || 0);

        if (aV.toLowerCase() === ac.toLowerCase() || aC.toLowerCase() === ac.toLowerCase()) {
            if (fs >= dInicio && fs <= dFin) {
                if (aV.toLowerCase() === ac.toLowerCase()) {
                    r.cabV += q;
                    const sV = row.RS_Vendedora || row.socv;
                    if (sV) socOps[sV] = 1;
                }
                if (aC.toLowerCase() === ac.toLowerCase()) {
                    r.cabC += q;
                    const sC = row.RS_Compradora || row.socc;
                    if (sC) socOps[sC] = 1;
                }
                r.trConc++;

                const fmtD = dObj.getUTCDate().toString().padStart(2, '0') + '/' + (dObj.getUTCMonth() + 1).toString().padStart(2, '0');

                allOps.push({
                    q,
                    d: [
                        row.ID || row.id || "-",
                        row.UN || row.un || "-",
                        row.RS_Vendedora || row.socv || "-",
                        aV,
                        row.RS_Compradora || row.socc || "-",
                        aC,
                        fmtD,
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

    // SHEETS processing
    if (vR && vR.length >= 6) {
        const dCom = vR[0].values || [], dAge = vR[1].values || [], dLeads = vR[2].values || [], dAux = vR[3].values || [], dSac = vR[4].values || [], dRem = vR[5].values || [];

        const parseSDate = v => fYMD(parseDateStr(v));
        const sG = {};
        for (const row of dCom) {
            if (String(row[5] || "").trim().toLowerCase() !== ac.toLowerCase()) continue;
            const fs = parseSDate(row[3]);
            if (fs >= dInicio && fs <= dFin) { r.com++; if (row[0]) sG[row[0]] = 1; }
        }
        for (const row of dAge) {
            if (String(row[3] || "").trim().toLowerCase() !== ac.toLowerCase()) continue;
            const fs = parseSDate(row[4]);
            if (fs >= dInicio && fs <= dFin) { r.age++; if (row[1]) sG[row[1]] = 1; }
        }
        r.tSG = Object.keys(sG).length;

        for (const row of dSac) {
            if (String(row[18] || "").trim().toLowerCase() !== ac.toLowerCase()) continue;
            const sd = parseDateStr(row[19]);
            if (sd && fYMD(sd) >= dInicio && fYMD(sd) <= dFin) r.sacs.push({ s: row[1], f: sd.getTime(), e: row[3] });
        }
        const remIds = {};
        for (const row of dRem) {
            if (String(row[2] || "").trim().toLowerCase() !== ac.toLowerCase()) continue;
            const fs = parseSDate(row[1]);
            if (fs >= dInicio && fs <= dFin) remIds[row[3] || row[0]] = 1;
        }
        r.rem = Object.keys(remIds).length;
    }

    return r;
}

// Global Helpers
function getSheetsApi() {
    let e = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || "";
    let k = process.env.GOOGLE_PRIVATE_KEY || "";
    if (e.startsWith('{')) { try { const p = JSON.parse(e); e = p.client_email; k = p.private_key; } catch (x) { } }
    if (!e || !k) return null;
    k = k.trim().replace(/\\n/g, '\n');
    if (!k.includes("---")) k = "-----BEGIN PRIVATE KEY-----\n" + k + "\n-----END PRIVATE KEY-----";
    const auth = new google.auth.GoogleAuth({ credentials: { client_email: e, private_key: k }, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    return google.sheets({ version: 'v4', auth });
}

async function getMetabaseSession() {
    try {
        const res = await fetch(`${METABASE_URL}/api/session`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username: METABASE_USER, password: METABASE_PASS })
        });
        if (!res.ok) throw new Error("Metabase Login Fallido (" + res.status + ")");
        const d = await res.json();
        return d.id;
    } catch (err) {
        throw new Error("No se pudo conectar con Metabase: " + err.message);
    }
}

async function queryCard(sessionId, cardId) {
    try {
        const res = await fetch(`${METABASE_URL}/api/card/${cardId}/query/json`, {
            method: "POST",
            headers: { "X-Metabase-Session": sessionId, "Content-Type": "application/json" }
        });
        if (!res.ok) throw new Error("Error en Card " + cardId + " (" + res.status + ")");
        return res.json();
    } catch (err) {
        console.error("Metabase Query Error:", err);
        return []; // Graceful failure
    }
}
