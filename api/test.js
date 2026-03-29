const { fetchMetabaseQuery } = require('./_lib/metabase');

module.exports = async (req, res) => {
  try {
    const data101 = await fetchMetabaseQuery(101).catch(e => ({ error: e.message, stack: e.stack }));
    const data102 = await fetchMetabaseQuery(102).catch(e => ({ error: e.message, stack: e.stack }));
    
    res.json({
      envCheck: {
        hasUrl: !!process.env.METABASE_URL,
        hasUser: !!process.env.METABASE_USER,
        hasPass: !!process.env.METABASE_PASS
      },
      q101: data101.error ? data101.error : `Success: ${data101.rows?.length} rows`,
      q101stack: data101.stack,
      q102: data102.error ? data102.error : `Success: ${data102.rows?.length} rows`,
      q102stack: data102.stack,
    });
  } catch (error) {
    res.status(500).json({ globalError: error.message });
  }
};
