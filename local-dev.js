const express = require('express');
const path = require('path');
const app = express();

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Servir la carpeta public (donde está index.html)
app.use(express.static(path.join(__dirname, 'public')));

// Cargar dinámicamente cada función de la API de Vercel
const endpoints = ['getConfig', 'getReport', 'refreshCacheAndWarmup', 'warmUp', 'sendEmailWithPDF'];

endpoints.forEach(ep => {
  app.all(`/api/${ep}`, async (req, res) => {
    try {
      // Válido para Vercel Serverless Functions comunes
      const handler = require(`./api/${ep}.js`);
      await handler(req, res);
    } catch (e) {
      console.error(`[Local Dev] Error en /api/${ep}:`, e);
      if (!res.headersSent) {
        res.status(500).json({ error: e.message });
      }
    }
  });
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`\n======================================================`);
  console.log(`✅ Servidor local (Simulador de Vercel) en marcha`);
  console.log(`👉 http://localhost:${PORT}`);
  console.log(`======================================================\n`);
  console.log(`Podés abrir tu navegador en ese link y ver la página.`);
  console.log(`Para apagar el servidor luego, presioná Ctrl+C acá en la consola.`);
});
