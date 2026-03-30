const { fetchMetabaseToken } = require('./_lib/metabase');
module.exports = async (req, res) => {
  try {
    const t = await fetchMetabaseToken();
    const q101res = await fetch(t.baseUrl + 'api/card/101/query', {
      method: 'POST',
      headers: { 'Content-Type':'application/json', 'X-Metabase-Session': t.id },
      body: JSON.stringify({ ignore_cache: false, parameters: [] })
    });
    const text = await q101res.text();
    res.json({
      status: q101res.status,
      slice: text.substring(0, 500)
    });
  } catch(e) { res.status(500).json({ err: e.message }); }
};
