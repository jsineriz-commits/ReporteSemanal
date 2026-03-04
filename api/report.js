/**
 * Vercel Serverless API - Reporte Semanal 12.0 (Google Sheets Only)
 * ================================================================
 * CORRECCIÓN FUNDAMENTAL: El sistema original (Codigo.js) lee de pestañas
 * de Google Sheets (BASE, OPS, etc.) que ya tienen los datos de Metabase
 * pre-cargados. NO necesitamos consultar Metabase directamente.
 *
 * Pestañas usadas:
 *   - BASE              (16 cols A-P): Ofrecidas
 *   - OPS               (23 cols A-W): Concretadas + Cargas
 *   - Comentarios_CRM    (8 cols A-H): Comentarios
 *   - Agenda_CRM         (5 cols A-E): Agendas
 *   - Leads_CRM         (12 cols A-L): Leads Nuevas
 *   - aux leads         (45 cols A-AS): Soc. Sin Gestión + Actividad
 *   - SAC               (20 cols A-T): SACs
 *   - REMATES            (4 cols A-D): Remates
 *   - Config 2.0        (1 col  A):   Lista de ACs
 */
const { google } = require('googleapis');

const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID || process.env.SPREADSHEET_ID || "";

module.exports = async function handler(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    if (req.method === "OPTIONS") return res.status(200).end();

    const { op, ac, startTs, endTs } = req.query;

    try {
        const api = getSheetsApi();
        if (!api) return res.status(500).json({ error: "Google Auth falló. Revisar Service Account." });

        if (op === "config") {
            // Traer ACs y semanas
            const response = await api.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: 'Comentarios_CRM!F2:F'
            });
            const rows = response.data.values || [];
            const acSet = new Set();
            for (const row of rows) if (row[0]) acSet.add(row[0].trim());

            const semanas = [];
            const s2026 = new Date("2026-01-01T00:00:00Z").getTime();
            for (let i = 1; i <= 52; i++) {
                const s = s2026 + (i - 1) * 604800000;
                semanas.push({ n: i, s, e: s + 518400000, y: 2026 });
            }
            return res.status(200).json({ acs: Array.from(acSet).sort(), semanas });
        }

        if (op === "report") {
            if (!ac || !startTs || !endTs) return res.status(400).json({ error: "Faltan parámetros" });

            // TRAER TODO DE GOOGLE SHEETS EN UNA SOLA LLAMADA
            const batchRes = await api.spreadsheets.values.batchGet({
                spreadsheetId: SPREADSHEET_ID,
                ranges: [
                    'BASE!A2:P',           // 0 - Ofrecidas  (16 cols)
                    'OPS!A2:W',            // 1 - Concretadas (23 cols)
                    'Comentarios_CRM!A2:H', // 2 - Comentarios (8 cols)
                    'Agenda_CRM!A2:E',      // 3 - Agendas    (5 cols)
                    'Leads_CRM!A2:L',       // 4 - Leads      (12 cols)
                    'aux leads!A2:AS',      // 5 - Aux Leads  (45 cols)
                    'SAC!A2:T',             // 6 - SACs       (20 cols)
                    'REMATES!A2:D'          // 7 - Remates    (4 cols)
                ]
            });

            const vR = batchRes.data.valueRanges;
            const dBase = vR[0].values || [];
            const dOps = vR[1].values || [];
            const dCom = vR[2].values || [];
            const dAge = vR[3].values || [];
            const dLeads = vR[4].values || [];
            const dAux = vR[5].values || [];
            const dSac = vR[6].values || [];
            const dRem = vR[7].values || [];

            const r = processReport(ac, Number(startTs), Number(endTs), dBase, dOps, dCom, dAge, dLeads, dAux, dSac, dRem);
            r._rows = { base: dBase.length, ops: dOps.length, com: dCom.length };
            return res.status(200).json(r);
        }

        return res.status(400).json({ error: "Especifique op=config o op=report" });
    } catch (e) {
        return res.status(500).json({ error: e.message, stack: e.stack ? e.stack.substring(0, 300) : "" });
    }
};

/**
 * Procesamiento idéntico al Codigo.js original (Apps Script).
 * Mapeo de columnas por ÍNDICE (igual que getSheetData):
 *
 * BASE: [0]=?, [1]=fecha, [2]=sociedad, [3]=estado, [4]=cabezas, [5]=AC, [6]=gF, [15]=motivo
 * OPS:  [0]=ID, [1]=UN, [2]=fecha, [5]=RS_Vend, [6]=AC_Vend, [7]=RS_Comp, [8]=AC_Comp,
 *       [9]=Q, [10]=Cat, [18]=fecha_carga, [20]=?, [22]=AC_carga
 */
function processReport(ac, startTs, endTs, dBase, dOps, dCom, dAge, dLeads, dAux, dSac, dRem) {
    const dIn = new Date(startTs); dIn.setUTCHours(0, 0, 0, 0);
    const dFi = new Date(endTs); dFi.setUTCHours(23, 59, 59, 999);
    const dInAnt = new Date(startTs - 604800000); dInAnt.setUTCHours(0, 0, 0, 0);
    const dFiAnt = new Date(endTs - 604800000); dFiAnt.setUTCHours(23, 59, 59, 999);

    const toYMD = (val) => {
        if (!val) return "";
        const d = new Date(val);
        if (isNaN(d.getTime())) return "";
        return d.getUTCFullYear() + String(d.getUTCMonth() + 1).padStart(2, "0") + String(d.getUTCDate()).padStart(2, "0");
    };

    const dInicio = toYMD(dIn), dFin = toYMD(dFi);
    const dInicioAnt = toYMD(dInAnt), dFinAnt = toYMD(dFiAnt);

    const r = {
        cab: 0, trop: 0, dT: [0, 0, 0, 0, 0, 0, 0], pCab: 0, pTrop: 0, cccNum: 0,
        cabV: 0, cabC: 0, trConc: 0, pConc: 0, socOps: 0, top5: [],
        tSG: 0, pTSG: 0, com: 0, age: 0, nuevas: 0, pNuevas: 0,
        socSinGestNum: 0, ssgTop5: [], actSemanal: [],
        sacs: [], pSac: 0, rem: 0, pRem: 0,
        carg: 0, cargProp: 0, cargAjen: 0
    };

    // =========================================
    // 1. BASE (Ofrecidas) — Columnas por índice
    // =========================================
    const socS = {};
    for (const row of dBase) {
        if (String(row[5] || "").trim() !== ac) continue;
        const fStr = toYMD(row[1]);
        if (!fStr) continue;

        const est = String(row[3] || "").trim().toUpperCase();
        const gF = Number(row[6]) || 0;
        const mot = String(row[15] || "").trim();
        const cab = Number(row[4]) || 0;

        let ok = false, esCCC = false;
        if (est === "CONCRETADA") { ok = true; esCCC = true; }
        else if (est === "PUBLICADO") { ok = true; esCCC = true; }
        else if (est === "NO CONCRETADAS" && mot !== "No la comercializo" && gF === 1) { ok = true; }
        if (!ok) continue;

        if (fStr >= dInicio && fStr <= dFin) {
            r.cab += cab; r.trop++;
            if (esCCC) r.cccNum++;
            if (row[2]) socS[row[2]] = 1;
            const d = new Date(row[1]);
            const dIdx = d.getUTCDay() === 0 ? 6 : d.getUTCDay() - 1;
            r.dT[dIdx]++;
        }
        if (fStr >= dInicioAnt && fStr <= dFinAnt) { r.pCab += cab; r.pTrop++; }
    }
    r.socOf = Object.keys(socS).length;
    r.ccc = r.trop > 0 ? Math.round((r.cccNum / r.trop) * 100) + "%" : "0%";

    // =============================================
    // 2. OPS (Concretadas + Cargas) — Por índice
    // =============================================
    const socOps = {};
    const allOps = [];
    for (const row of dOps) {
        const fStr = toYMD(row[2]);
        if (!fStr) continue;

        const aV = String(row[6] || "").trim();
        const aC = String(row[8] || "").trim();
        const q = Number(row[9]) || 0;

        if (aV === ac || aC === ac) {
            if (fStr >= dInicio && fStr <= dFin) {
                if (aV === ac) { r.cabV += q; if (row[5]) socOps[row[5]] = 1; }
                if (aC === ac) { r.cabC += q; if (row[7]) socOps[row[7]] = 1; }
                r.trConc++;
                const fD = new Date(row[2]);
                const fmtD = fD.getUTCDate().toString().padStart(2, '0') + '/' + (fD.getUTCMonth() + 1).toString().padStart(2, '0');
                allOps.push({
                    q,
                    d: [row[0] || "", row[1] || "", row[5] || "", aV, row[7] || "", aC, fmtD, q, row[22] || "", row[20] || "", row[10] || ""]
                });
            }
            if (fStr >= dInicioAnt && fStr <= dFinAnt) r.pConc += q;
        }

        // Cargas
        const fCarStr = toYMD(row[18]);
        if (fCarStr && String(row[22] || "").trim() === ac) {
            if (fCarStr >= dInicio && fCarStr <= dFin) {
                r.carg++;
                if (aV === ac) r.cargProp++; else r.cargAjen++;
            }
        }
    }
    allOps.sort((a, b) => b.q - a.q);
    r.top5 = allOps.slice(0, 5);
    r.socOps = Object.keys(socOps).length;

    // =========================================
    // 3. CRM (Comentarios + Agendas)
    // =========================================
    const socGest = {}, pSocGest = {};
    for (const row of dCom) {
        if (String(row[5] || "").trim() !== ac) continue;
        const fStr = toYMD(row[3]);
        if (!fStr) continue;
        if (fStr >= dInicio && fStr <= dFin) {
            if (String(row[7] || "").trim() === "") r.com++;
            if (row[0]) socGest[row[0]] = 1;
        }
        if (fStr >= dInicioAnt && fStr <= dFinAnt) {
            if (row[0]) pSocGest[row[0]] = 1;
        }
    }
    for (const row of dAge) {
        if (String(row[3] || "").trim() !== ac) continue;
        const fStr = toYMD(row[4]);
        if (!fStr) continue;
        if (fStr >= dInicio && fStr <= dFin) { r.age++; if (row[1]) socGest[row[1]] = 1; }
        if (fStr >= dInicioAnt && fStr <= dFinAnt) { if (row[1]) pSocGest[row[1]] = 1; }
    }
    r.tSG = Object.keys(socGest).length;
    r.pTSG = Object.keys(pSocGest).length;

    // =========================================
    // 4. Leads (Nuevas)
    // =========================================
    for (const row of dLeads) {
        if (String(row[2] || "").trim() !== ac) continue;
        if (String(row[3] || "").trim() !== "UA" || String(row[11] || "").trim() === "NO HABILITADO") continue;
        const fStr = toYMD(row[1]);
        if (!fStr) continue;
        if (fStr >= dInicio && fStr <= dFin) r.nuevas++;
        if (fStr >= dInicioAnt && fStr <= dFinAnt) r.pNuevas++;
    }

    // =========================================
    // 5. Aux Leads (Soc Sin Gestión + Actividad)
    // =========================================
    const ssgAll = [];
    for (const row of dAux) {
        if (String(row[1] || "").trim() !== ac) continue;
        const isNuevo = String(row[4] || "").trim().toUpperCase() === "NUEVO";
        if (isNuevo) r.socSinGestNum++;

        const cDateStr = toYMD(row[2]);
        const obj = {
            kt: row[31] || "", kv: row[36] || "", soc: row[29] || "",
            fa: cDateStr ? new Date(row[2]).getUTCDate().toString().padStart(2, '0') + '/' + (new Date(row[2]).getUTCMonth() + 1).toString().padStart(2, '0') : "",
            fu: row[37] || "", ug: row[40] || "", ua: row[39] || "", sg: row[38] || "",
            w: Number(row[22]) || 0,
            cDateStr
        };

        if (isNuevo) ssgAll.push(obj);
        if (cDateStr >= dInicio && cDateStr <= dFin) {
            const ag = Number(row[32]) || 0;
            const aoStr = String(row[40] || "").trim().toLowerCase();
            if (ag <= 6 && aoStr !== "sin gestión") r.actSemanal.push(obj);
        }
    }
    ssgAll.sort((a, b) => b.w - a.w);
    r.ssgTop5 = ssgAll.slice(0, 5);

    // =========================================
    // 6. SACs
    // =========================================
    for (const row of dSac) {
        if (String(row[18] || "").trim() !== ac) continue;
        const fStr = toYMD(row[19]);
        if (!fStr) continue;
        if (fStr >= dInicio && fStr <= dFin) {
            r.sacs.push({ s: row[1] || "", f: new Date(row[19]).getTime(), e: row[3] || "" });
        }
        if (fStr >= dInicioAnt && fStr <= dFinAnt) r.pSac++;
    }

    // =========================================
    // 7. Remates
    // =========================================
    const remIds = {}, pRemIds = {};
    for (const row of dRem) {
        if (String(row[2] || "").trim() !== ac) continue;
        const fStr = toYMD(row[1]);
        if (!fStr) continue;
        if (fStr >= dInicio && fStr <= dFin) remIds[row[3] || row[0] || "x"] = 1;
        if (fStr >= dInicioAnt && fStr <= dFinAnt) pRemIds[row[3] || row[0] || "x"] = 1;
    }
    r.rem = Object.keys(remIds).length;
    r.pRem = Object.keys(pRemIds).length;

    return r;
}

// =========================================
// Google Sheets Auth Helper
// =========================================
function getSheetsApi() {
    let email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || "";
    let key = process.env.GOOGLE_PRIVATE_KEY || "";
    if (email.startsWith('{')) {
        try { const p = JSON.parse(email); email = p.client_email; key = p.private_key; } catch (e) { }
    }
    if (!email || !key) return null;
    key = key.trim().replace(/\\n/g, '\n');
    if (!key.includes("---")) key = "-----BEGIN PRIVATE KEY-----\n" + key + "\n-----END PRIVATE KEY-----";
    const auth = new google.auth.GoogleAuth({
        credentials: { client_email: email, private_key: key },
        scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
    });
    return google.sheets({ version: 'v4', auth });
}
