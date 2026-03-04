/**
 * Vercel Serverless API - Reporte Semanal 12.1 (Diagnostic)
 * Agrega un endpoint de diagnóstico para ver qué hay realmente en las hojas.
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
        if (!api) return res.status(500).json({ error: "Google Auth falló." });

        // ==============================================
        // DIAGNÓSTICO: Ver qué hojas existen y qué datos tienen
        // ==============================================
        if (op === "diag") {
            // Primero: obtener la lista de hojas
            const meta = await api.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
            const sheetNames = meta.data.sheets.map(s => s.properties.title);

            // Intentar leer muestras de cada pestaña clave
            const samples = {};
            for (const name of ['BASE', 'OPS', 'Comentarios_CRM', 'Config 2.0']) {
                if (sheetNames.includes(name)) {
                    try {
                        const d = await api.spreadsheets.values.get({
                            spreadsheetId: SPREADSHEET_ID,
                            range: `'${name}'!A1:W3` // Headers + 2 filas de ejemplo
                        });
                        samples[name] = d.data.values || [];
                    } catch (e) { samples[name] = "Error: " + e.message; }
                } else {
                    samples[name] = "⛔ NO EXISTE ESTA PESTAÑA";
                }
            }

            return res.status(200).json({
                spreadsheetId: SPREADSHEET_ID,
                allSheets: sheetNames,
                samples
            });
        }

        if (op === "config") {
            // Leer ACs desde Config 2.0 (columna A tiene los nombres reales)
            // Los nombres de Config 2.0 coinciden con las columnas AC_Vend en BASE y asoc_com_vend en OPS
            const meta = await api.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
            const sheetNames = meta.data.sheets.map(s => s.properties.title);

            let acList = [];

            // Config 2.0 tiene los nombres reales (como el original Codigo.js)
            if (sheetNames.includes('Config 2.0')) {
                const d = await api.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'Config 2.0'!A2:A" });
                acList = (d.data.values || []).map(r => (r[0] || "").trim()).filter(Boolean);
            }

            // Fallback: Si no hay Config 2.0, usar Comentarios_CRM col F
            if (acList.length === 0 && sheetNames.includes('Comentarios_CRM')) {
                const d = await api.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'Comentarios_CRM!F2:F' });
                const set = new Set();
                (d.data.values || []).forEach(r => { if (r[0]) set.add(r[0].trim()); });
                acList = Array.from(set);
            }

            // Semanas
            let semanas = [];
            // Intentar leer de pestaña "aux" como el original
            if (sheetNames.includes('aux')) {
                try {
                    const auxData = await api.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: "'aux'!J2:M" });
                    const auxRows = auxData.data.values || [];
                    for (const row of auxRows) {
                        if (row[0] && row[1] && row[2]) {
                            semanas.push({
                                n: Number(row[0]),
                                s: new Date(row[1]).getTime(),
                                e: new Date(row[2]).getTime(),
                                y: Number(row[3]) || 2026
                            });
                        }
                    }
                } catch (e) { }
            }

            // Fallback: generar semanas fijas
            if (semanas.length === 0) {
                const s2026 = new Date("2026-01-01T00:00:00Z").getTime();
                for (let i = 1; i <= 52; i++) {
                    const s = s2026 + (i - 1) * 604800000;
                    semanas.push({ n: i, s, e: s + 518400000, y: 2026 });
                }
            }

            return res.status(200).json({ acs: acList.sort(), semanas, _source: 'Config 2.0' });
        }

        if (op === "report") {
            if (!ac || !startTs || !endTs) return res.status(400).json({ error: "Faltan parámetros" });

            // Verificar qué pestañas existen
            const meta = await api.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
            const sheetNames = meta.data.sheets.map(s => s.properties.title);

            // Construir rangos dinámicamente según las pestañas que existan
            const ranges = [];
            const rangeMap = {};
            const addRange = (name, range, key) => {
                if (sheetNames.includes(name)) {
                    rangeMap[key] = ranges.length;
                    ranges.push(`'${name}'!${range}`);
                }
            };

            addRange('BASE', 'A2:P', 'base');
            addRange('OPS', 'A2:W', 'ops');
            addRange('Comentarios_CRM', 'A2:H', 'com');
            addRange('Agenda_CRM', 'A2:E', 'age');
            addRange('Leads_CRM', 'A2:L', 'leads');
            addRange('aux leads', 'A2:AS', 'aux');
            addRange('SAC', 'A2:T', 'sac');
            addRange('REMATES', 'A2:D', 'rem');

            if (ranges.length === 0) {
                return res.status(200).json({ error: "No se encontró ninguna pestaña válida", sheets: sheetNames });
            }

            const batchRes = await api.spreadsheets.values.batchGet({ spreadsheetId: SPREADSHEET_ID, ranges });

            const getVals = (key) => {
                if (rangeMap[key] === undefined) return [];
                return batchRes.data.valueRanges[rangeMap[key]].values || [];
            };

            const dBase = getVals('base');
            const dOps = getVals('ops');
            const dCom = getVals('com');
            const dAge = getVals('age');
            const dLeads = getVals('leads');
            const dAux = getVals('aux');
            const dSac = getVals('sac');
            const dRem = getVals('rem');

            const r = processReport(ac, Number(startTs), Number(endTs), dBase, dOps, dCom, dAge, dLeads, dAux, dSac, dRem);

            // Debug info
            r._debug = {
                sheetsFound: sheetNames,
                rangesUsed: ranges,
                rowCounts: { base: dBase.length, ops: dOps.length, com: dCom.length, age: dAge.length, sac: dSac.length, rem: dRem.length },
                acSearched: ac,
                // Muestra de valores AC encontrados en BASE col F (idx 5)
                sampleACsInBase: [...new Set(dBase.slice(0, 50).map(r => r[5]).filter(Boolean))].slice(0, 10),
                // Muestra de valores AC encontrados en OPS col G (idx 6)
                sampleACsInOps: [...new Set(dOps.slice(0, 50).map(r => r[6]).filter(Boolean))].slice(0, 10),
                // Muestra de valores AC en Comentarios_CRM col F (idx 5)
                sampleACsInCom: [...new Set(dCom.slice(0, 50).map(r => r[5]).filter(Boolean))].slice(0, 10),
            };

            return res.status(200).json(r);
        }

        return res.status(400).json({ error: "Especifique op=config, op=report, o op=diag" });
    } catch (e) {
        return res.status(500).json({ error: e.message, stack: e.stack ? e.stack.substring(0, 500) : "" });
    }
};

function processReport(ac, startTs, endTs, dBase, dOps, dCom, dAge, dLeads, dAux, dSac, dRem) {
    const dIn = new Date(startTs); dIn.setUTCHours(0, 0, 0, 0);
    const dFi = new Date(endTs); dFi.setUTCHours(23, 59, 59, 999);
    const dInAnt = new Date(startTs - 604800000); dInAnt.setUTCHours(0, 0, 0, 0);
    const dFiAnt = new Date(endTs - 604800000); dFiAnt.setUTCHours(23, 59, 59, 999);

    const toYMD = (val) => {
        if (!val) return "";
        let d;
        // Si ya es un número (serial date de Excel/Sheets)
        if (!isNaN(val) && typeof val === 'number') {
            d = new Date((val - 25569) * 86400 * 1000);
        } else {
            // Si es un string tipo "25/2/2026"
            const parts = String(val).split('/');
            if (parts.length === 3) {
                // Asumimos D/M/YYYY (formato Latam/España común en Sheets)
                d = new Date(Date.UTC(parts[2], parts[1] - 1, parts[0]));
            } else {
                d = new Date(val);
            }
        }
        if (isNaN(d.getTime())) return "";
        return d.getUTCFullYear() + String(d.getUTCMonth() + 1).padStart(2, "0") + String(d.getUTCDate()).padStart(2, "0");
    };

    const dInicio = toYMD(startTs), dFin = toYMD(endTs);
    const dInicioAnt = toYMD(startTs - 604800000), dFinAnt = toYMD(endTs - 604800000);

    const r = {
        cab: 0, trop: 0, dT: [0, 0, 0, 0, 0, 0, 0], pCab: 0, pTrop: 0, cccNum: 0,
        cabV: 0, cabC: 0, trConc: 0, pConc: 0, socOps: 0, top5: [],
        tSG: 0, pTSG: 0, com: 0, age: 0, nuevas: 0, pNuevas: 0,
        socSinGestNum: 0, ssgTop5: [], actSemanal: [],
        sacs: [], pSac: 0, rem: 0, pRem: 0,
        carg: 0, cargProp: 0, cargAjen: 0
    };

    const parseDateIdx = (val) => {
        if (!val) return null;
        const parts = String(val).split('/');
        if (parts.length === 3) return new Date(Date.UTC(parts[2], parts[1] - 1, parts[0]));
        const d = new Date(val);
        return isNaN(d.getTime()) ? null : d;
    };

    // 1. BASE
    const socS = {};
    for (const row of dBase) {
        if (String(row[5] || "").trim().toLowerCase() !== ac.toLowerCase()) continue;
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
            const d = parseDateIdx(row[1]);
            if (d) {
                const dIdx = d.getUTCDay() === 0 ? 6 : d.getUTCDay() - 1;
                r.dT[dIdx]++;
            }
        }
        if (fStr >= dInicioAnt && fStr <= dFinAnt) { r.pCab += cab; r.pTrop++; }
    }
    r.socOf = Object.keys(socS).length;
    r.ccc = r.trop > 0 ? Math.round((r.cccNum / r.trop) * 100) + "%" : "0%";

    // 2. OPS
    const socOps = {}, allOps = [];
    for (const row of dOps) {
        const fStr = toYMD(row[2]);
        if (!fStr) continue;
        const aV = String(row[6] || "").trim();
        const aC = String(row[8] || "").trim();
        const q = Number(row[9]) || 0;
        const meV = aV.toLowerCase() === ac.toLowerCase();
        const meC = aC.toLowerCase() === ac.toLowerCase();

        if (meV || meC) {
            if (fStr >= dInicio && fStr <= dFin) {
                if (meV) { r.cabV += q; if (row[5]) socOps[row[5]] = 1; }
                if (meC) { r.cabC += q; if (row[7]) socOps[row[7]] = 1; }
                r.trConc++;
                const d = parseDateIdx(row[2]);
                const fmtD = d ? (d.getUTCDate().toString().padStart(2, '0') + '/' + (d.getUTCMonth() + 1).toString().padStart(2, '0')) : "...";
                allOps.push({ q, d: [row[0] || "", row[1] || "", row[5] || "", aV, row[7] || "", aC, fmtD, q, row[22] || "", row[20] || "", row[10] || ""] });
            }
            if (fStr >= dInicioAnt && fStr <= dFinAnt) r.pConc += q;
        }
        const fCarStr = toYMD(row[18]);
        if (fCarStr && String(row[22] || "").trim().toLowerCase() === ac.toLowerCase()) {
            if (fCarStr >= dInicio && fCarStr <= dFin) {
                r.carg++;
                if (meV) r.cargProp++; else r.cargAjen++;
            }
        }
    }
    allOps.sort((a, b) => b.q - a.q);
    r.top5 = allOps.slice(0, 5);
    r.socOps = Object.keys(socOps).length;

    // 3. CRM
    const socGest = {}, pSocGest = {};
    for (const row of dCom) {
        if (String(row[5] || "").trim().toLowerCase() !== ac.toLowerCase()) continue;
        const fStr = toYMD(row[3]);
        if (!fStr) continue;
        if (fStr >= dInicio && fStr <= dFin) {
            if (String(row[7] || "").trim() === "") r.com++;
            if (row[0]) socGest[row[0]] = 1;
        }
        if (fStr >= dInicioAnt && fStr <= dFinAnt) { if (row[0]) pSocGest[row[0]] = 1; }
    }
    for (const row of dAge) {
        if (String(row[3] || "").trim().toLowerCase() !== ac.toLowerCase()) continue;
        const fStr = toYMD(row[4]);
        if (!fStr) continue;
        if (fStr >= dInicio && fStr <= dFin) { r.age++; if (row[1]) socGest[row[1]] = 1; }
        if (fStr >= dInicioAnt && fStr <= dFinAnt) { if (row[1]) pSocGest[row[1]] = 1; }
    }
    r.tSG = Object.keys(socGest).length;
    r.pTSG = Object.keys(pSocGest).length;

    // 4. Leads
    for (const row of dLeads) {
        if (String(row[2] || "").trim().toLowerCase() !== ac.toLowerCase()) continue;
        if (String(row[3] || "").trim() !== "UA" || String(row[11] || "").trim() === "NO HABILITADO") continue;
        const fStr = toYMD(row[1]);
        if (!fStr) continue;
        if (fStr >= dInicio && fStr <= dFin) r.nuevas++;
        if (fStr >= dInicioAnt && fStr <= dFinAnt) r.pNuevas++;
    }

    // 5. Aux
    const ssgAll = [];
    for (const row of dAux) {
        if (String(row[1] || "").trim().toLowerCase() !== ac.toLowerCase()) continue;
        const isNuevo = String(row[4] || "").trim().toUpperCase() === "NUEVO";
        if (isNuevo) r.socSinGestNum++;
        const cDateStr = toYMD(row[2]);
        const d = parseDateIdx(row[2]);
        const obj = {
            soc: row[29] || "", w: Number(row[22]) || 0, cDateStr,
            fa: d ? (d.getUTCDate().toString().padStart(2, '0') + '/' + (d.getUTCMonth() + 1).toString().padStart(2, '0')) : ""
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

    // 6. SAC
    for (const row of dSac) {
        if (String(row[18] || "").trim() !== ac) continue;
        const fStr = toYMD(row[19]);
        if (!fStr) continue;
        if (fStr >= dInicio && fStr <= dFin) r.sacs.push({ s: row[1] || "", f: new Date(row[19]).getTime(), e: row[3] || "" });
        if (fStr >= dInicioAnt && fStr <= dFinAnt) r.pSac++;
    }

    // 7. Remates
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

function getSheetsApi() {
    let email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || "";
    let key = process.env.GOOGLE_PRIVATE_KEY || "";
    if (email.startsWith('{')) { try { const p = JSON.parse(email); email = p.client_email; key = p.private_key; } catch (e) { } }
    if (!email || !key) return null;
    key = key.trim().replace(/\\n/g, '\n');
    if (!key.includes("---")) key = "-----BEGIN PRIVATE KEY-----\n" + key + "\n-----END PRIVATE KEY-----";
    return google.sheets({
        version: 'v4', auth: new google.auth.GoogleAuth({
            credentials: { client_email: email, private_key: key },
            scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
        })
    });
}
