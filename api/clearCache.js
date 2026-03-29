// api/clearCache.js
const { clearCache } = require('./_lib/logic');
module.exports = async (req, res) => {
  if (req.method !== 'POST') return res.status(405).end();
  try { res.json({ ok: true, message: clearCache() }); }
  catch (e) { res.status(500).json({ error: e.message }); }
};
