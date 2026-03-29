// api/refreshCacheAndWarmup.js
const { refreshCacheAndWarmup } = require('./_lib/logic');
module.exports = async (req, res) => {
  if (req.method !== 'POST') return res.status(405).end();
  const { ac, startTs, endTs } = req.body || {};
  try {
    const result = await refreshCacheAndWarmup(ac || '', Number(startTs) || 0, Number(endTs) || 0);
    res.json(result);
  } catch (e) { console.error('[api/refreshCacheAndWarmup]', e); res.status(500).json({ ok: false, error: e.message }); }
};
