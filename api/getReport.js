// api/getReport.js
const { getReport } = require('./_lib/logic');
module.exports = async (req, res) => {
  if (req.method !== 'GET') return res.status(405).end();
  const { ac, startTs, endTs } = req.query;
  if (!ac || !startTs || !endTs) return res.status(400).json({ error: 'Faltan parámetros: ac, startTs, endTs' });
  try {
    const data = await getReport(ac, Number(startTs), Number(endTs));
    res.json(data);
  } catch (e) { console.error('[api/getReport]', e); res.status(500).json({ error: e.message }); }
};
