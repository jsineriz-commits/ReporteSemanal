// api/getConfigData.js
const { getConfigData } = require('./_lib/logic');
module.exports = async (req, res) => {
  if (req.method !== 'GET') return res.status(405).end();
  try { res.json(await getConfigData()); }
  catch (e) { console.error('[api/getConfigData]', e); res.status(500).json({ error: e.message }); }
};
