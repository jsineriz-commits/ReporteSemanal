/**
 * Vercel Serverless API - Reporte Semanal Híbrido
 * Fuentes: Metabase (BASE, OPS) + Google Sheets Web App (CRM, SAC, REMATES)
 */

const METABASE_URL = (process.env.METABASE_URL || "https://bi.decampoacampo.com").replace(/\/$/, "");
const METABASE_USER = process.env.METABASE_USER || "";
const METABASE_PASS = process.env.METABASE_PASS || "";
const SHEETS_API_URL = (process.env.SHEETS_API_URL || "");

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
    return res.json(); // Array of objects
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
    return day === 0 ? 6 : day - 1; // Lun=0 ... Dom=6
}

// ============ MAIN HANDLER ============
module.exports = async function handler(req, res) {
    // CORS headers
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
            return res.status(400).json({ error: "Missing op parameter (config|report)" });
        }
    } catch (e) {
        console.error(e);
        return res.status(500).json({ error: e.message || String(e) });
    }
};

// ============ CONFIG (from Sheets) ============
async function handleConfig(res) {
    if (!SHEETS_API_URL) {
        return res.status(500).json({ error: "SHEETS_API_URL no configurada en variables de entorno de Vercel" });
    }
    try {
        const r = await fetch(`${SHEETS_API_URL}?api=true&op=config`);
        if (!r.ok) {
            const txt = await r.text();
            return res.status(500).json({ error: `La Web App de Google Sheets respondió con error (${r.status}): ${txt.slice(0, 100)}` });
        }
        const data = await r.json();
        return res.status(200).json(data);
    } catch (e) {
        return res.status(500).json({ error: "Error de conexión con Google Sheets: " + e.message });
    }
}

// ============ REPORT (Hybrid) ============
async function handleReport(res, ac, startTs, endTs) {
    if (!ac || !startTs || !endTs) {
        return res.status(400).json({ error: "Faltan parámetros: ac, startTs, endTs" });
    }

    // Compute date boundaries
    const dIn = new Date(startTs); dIn.setHours(0, 0, 0, 0);
    const dFi = new Date(endTs); dFi.setHours(23, 59, 59, 999);
    const dInAnt = new Date(startTs - 604800000); dInAnt.setHours(0, 0, 0, 0);
    const dFiAnt = new Date(endTs - 604800000); dFiAnt.setHours(23, 59, 59, 999);

    const dInicio = toYMD(dIn);
    const dFin = toYMD(dFi);
    const dInicioAnt = toYMD(dInAnt);
    const dFinAnt = toYMD(dFiAnt);

    // Result object
    const r = {
        cab: 0, trop: 0, dT: [0, 0, 0, 0, 0, 0, 0], pCab: 0, pTrop: 0, cccNum: 0,
        cabV: 0, cabC: 0, trConc: 0, pConc: 0, socOps: 0, top5: [],
        tSG: 0, pTSG: 0, com: 0, age: 0, nuevas: 0, pNuevas: 0,
        socSinGestNum: 0, ssgTop5: [], actSemanal: [],
        sacs: [], pSac: 0, rem: 0, pRem: 0,
        carg: 0, cargProp: 0, cargAjen: 0
    };

    // ---- Parallel fetching ----
    let sessionId;
    try {
        sessionId = await getMetabaseSession();
    } catch (e) {
        return res.status(500).json({ error: "No se pudo conectar a Metabase: " + e.message });
    }

    // Fetch Metabase cards + Sheets CRM in parallel
    const [baseRows, opsRows, sheetsCRM] = await Promise.all([
        queryCard(sessionId, 3588),  // BASE
        queryCard(sessionId, 3584),  // OPS
        fetchSheetsCRM(ac, startTs, endTs)
    ]);

    // ============ PROCESS BASE (Card 3588) ============
    try {
        // Metabase returns array of objects with column names as keys
        // We need to identify the column names. Let's use the first row to discover them.
        const socS = {};
        const cols = baseRows.length > 0 ? Object.keys(baseRows[0]) : [];

        for (const row of baseRows) {
            // Get values by array index (matching the sheet column order)
            const vals = cols.map(c => row[c]);
            // row[5]=F(AC), row[1]=B(fecha), row[3]=D(estado), row[4]=E(cab), row[6]=G(gF), row[15]=P(motivo), row[2]=C(soc)
            const rowAC = String(vals[5] || "").trim();
            if (rowAC !== ac) continue;

            const fechaStr = toYMD(vals[1]);
            if (!fechaStr) continue;

            const est = String(vals[3] || "").trim().toUpperCase();
            const gF = Number(vals[6]) || 0;
            const mot = String(vals[15] || "").trim();
            const cab = Number(vals[4]) || 0;

            let ok = false, esCCC = false;
            if (est === "CONCRETADA") { ok = true; esCCC = true; }
            else if (est === "PUBLICADO") { ok = true; esCCC = true; }
            else if (est === "NO CONCRETADAS" && mot !== "No la comercializo" && gF === 1) { ok = true; }
            if (!ok) continue;

            if (fechaStr >= dInicio && fechaStr <= dFin) {
                r.cab += cab; r.trop++;
                if (esCCC) r.cccNum++;
                if (vals[2]) socS[vals[2]] = 1;
                r.dT[dayOfWeekIdx(vals[1])]++;
            }
            if (fechaStr >= dInicioAnt && fechaStr <= dFinAnt) { r.pCab += cab; r.pTrop++; }
        }
        r.socOf = Object.keys(socS).length;
        r.ccc = r.trop > 0 ? Math.round((r.cccNum / r.trop) * 100) + "%" : "0%";
    } catch (e) { r.errBase = e.message; }

    // ============ PROCESS OPS (Card 3584) ============
    try {
        const socOps = {};
        const allOps = [];
        const colsO = opsRows.length > 0 ? Object.keys(opsRows[0]) : [];

        for (const row of opsRows) {
            const vals = colsO.map(c => row[c]);
            // row[2]=C(fecha), row[6]=G(acV), row[8]=I(acC), row[9]=J(q)
            const fechaStr = toYMD(vals[2]);
            if (!fechaStr) continue;

            const aV = String(vals[6] || "").trim();
            const aC = String(vals[8] || "").trim();
            const q = Number(vals[9]) || 0;

            if (aV === ac || aC === ac) {
                if (fechaStr >= dInicio && fechaStr <= dFin) {
                    if (aV === ac) { r.cabV += q; if (vals[5]) socOps[vals[5]] = 1; }
                    if (aC === ac) { r.cabC += q; if (vals[7]) socOps[vals[7]] = 1; }
                    r.trConc++;
                    allOps.push({
                        q: q,
                        d: [vals[0] || "", vals[1] || "", vals[5] || "", vals[6] || "", vals[7] || "", vals[8] || "", fmtDate(vals[2]), q, vals[22] || "", vals[20] || "", vals[10] || ""]
                    });
                }
                if (fechaStr >= dInicioAnt && fechaStr <= dFinAnt) r.pConc += q;
            }

            // Cargas
            const fCarStr = toYMD(vals[18]);
            if (fCarStr && String(vals[22] || "").trim() === ac) {
                if (fCarStr >= dInicio && fCarStr <= dFin) {
                    r.carg++;
                    if (aV === ac) r.cargProp++; else r.cargAjen++;
                }
            }
        }
        allOps.sort((a, b) => b.q - a.q);
        r.top5 = allOps.slice(0, 5);
        r.socOps = Object.keys(socOps).length;
    } catch (e) { r.errOps = e.message; }

    // ============ MERGE CRM DATA FROM SHEETS ============
    if (sheetsCRM) {
        r.tSG = sheetsCRM.tSG || 0;
        r.pTSG = sheetsCRM.pTSG || 0;
        r.com = sheetsCRM.com || 0;
        r.age = sheetsCRM.age || 0;
        r.nuevas = sheetsCRM.nuevas || 0;
        r.pNuevas = sheetsCRM.pNuevas || 0;
        r.socSinGestNum = sheetsCRM.socSinGestNum || 0;
        r.ssgTop5 = sheetsCRM.ssgTop5 || [];
        r.actSemanal = sheetsCRM.actSemanal || [];
        r.sacs = sheetsCRM.sacs || [];
        r.pSac = sheetsCRM.pSac || 0;
        r.rem = sheetsCRM.rem || 0;
        r.pRem = sheetsCRM.pRem || 0;
    }

    return res.status(200).json(r);
}

// ============ SHEETS CRM FETCH ============
async function fetchSheetsCRM(ac, startTs, endTs) {
    if (!SHEETS_API_URL) return null;
    try {
        const url = `${SHEETS_API_URL}?api=true&op=report&ac=${encodeURIComponent(ac)}&startTs=${startTs}&endTs=${endTs}`;
        const r = await fetch(url);
        const data = await r.json();
        return data;
    } catch (e) {
        console.error("Sheets CRM fetch error:", e);
        return null;
    }
}
