// api/warmUp.js
// Invocado por Vercel Cron: "0 * * * *" (cada hora UTC)
// Equivale a scheduledWarmup() + setupAutoWarmup() del .gs original.
//
// Vercel envía Authorization: Bearer <CRON_SECRET> automáticamente.
// Para llamadas manuales usar ?secret=WARM_UP_SECRET

const { scheduledWarmup } = require('./_lib/logic');

module.exports = async (req, res) => {
  if (req.method !== 'GET' && req.method !== 'POST') return res.status(405).end();

  const cronSecret = process.env.CRON_SECRET    || '';
  const warmSecret = process.env.WARM_UP_SECRET || '';
  const authHeader = req.headers['authorization'] || '';

  const isVercelCron = cronSecret && authHeader === `Bearer ${cronSecret}`;
  const isManual     = warmSecret && req.query.secret === warmSecret;

  if (cronSecret && !isVercelCron && !isManual) {
    return res.status(401).json({ error: 'No autorizado.' });
  }

  try {
    const result = await scheduledWarmup();
    res.json(result);
  } catch (e) {
    console.error('[api/warmUp]', e);
    res.status(500).json({ ok: false, error: e.message });
  }
};
