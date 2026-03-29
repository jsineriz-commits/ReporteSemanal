// api/getConfig.js
const { getConfig } = require('./_lib/logic');
module.exports = async (req, res) => {
  if (req.method !== 'GET') return res.status(405).end();
  try { res.json(await getConfig()); }
  catch (e) { console.error('[api/getConfig]', e); res.status(500).json({ error: e.message }); }
};
