// local-dev.js — Simulador local de Vercel (puerto 4000)
// Carga .env.local automáticamente usando el API nativo de Node 20 (--env-file)
// o dotenv si está disponible como fallback.

// ── Carga de variables de entorno ──────────────────────────────────────────────
const fs = require('fs');
const path = require('path');

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) return;
  const lines = fs.readFileSync(filePath, 'utf8').split('\n');
  for (const raw of lines) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eqIdx = line.indexOf('=');
    if (eqIdx < 0) continue;
    const key = line.slice(0, eqIdx).trim();
    let val = line.slice(eqIdx + 1).trim();
    // Quitar comillas envolventes " o '
    if ((val.startsWith('"') && val.endsWith('"')) ||
        (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1);
    }
    if (!(key in process.env)) process.env[key] = val;
  }
  console.log(`[env] Cargado: ${filePath}`);
}

loadEnvFile(path.join(__dirname, '.env.local'));
loadEnvFile(path.join(__dirname, '.env'));

// ── Servidor Express ────────────────────────────────────────────────────────────
const express = require('express');
const app = express();

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Servir archivos estáticos desde /public
app.use(express.static(path.join(__dirname, 'public')));

// Endpoints de la API (equivalente a las Serverless Functions de Vercel)
const endpoints = ['getConfig', 'getReport', 'refreshCacheAndWarmup', 'warmUp', 'sendEmailWithPDF'];

endpoints.forEach(ep => {
  app.all(`/api/${ep}`, async (req, res) => {
    try {
      // Re-require en cada petición para reflejar cambios sin reiniciar
      delete require.cache[require.resolve(`./api/${ep}.js`)];
      const handler = require(`./api/${ep}.js`);
      await handler(req, res);
    } catch (e) {
      console.error(`[Local Dev] Error en /api/${ep}:`, e.message);
      if (!res.headersSent) res.status(500).json({ error: e.message });
    }
  });
});

// Fallback → index.html (SPA / Express 5 compatible)
app.get(/.*/, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.LOCAL_PORT || 4000;
app.listen(PORT, () => {
  console.log(`\n======================================================`);
  console.log(`✅  Servidor local activo en: http://localhost:${PORT}`);
  console.log(`======================================================\n`);
});
