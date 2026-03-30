const { fetchMetabaseQuery } = require('./_lib/metabase');
module.exports = async (req, res) => {
  try {
    const q101 = await fetchMetabaseQuery(101);
    const q102 = await fetchMetabaseQuery(102);
    res.json({
      h101: q101.headers,
      h102: q102.headers,
      q101rows: q101.rows.length,
      q102rows: q102.rows.length
    });
  } catch(e) { res.status(500).json({ err: e.message }); }
};
