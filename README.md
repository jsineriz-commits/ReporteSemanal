# Reportes Semanales — Migración Apps Script → Vercel

## Estructura del proyecto

```
/
├── api/
│   ├── _lib/
│   │   ├── sheets.js          ← Auth Google Sheets API (Service Account)
│   │   ├── cache.js           ← Cache en memoria (reemplaza CacheService)
│   │   ├── props.js           ← Propiedades persistentes (reemplaza PropertiesService)
│   │   ├── mailer.js          ← Email + Drive (reemplaza GmailApp + DriveApp)
│   │   └── logic.js           ← TODA la lógica de negocio (Code.gs)
│   ├── getConfig.js           ← GET  /api/getConfig
│   ├── getReport.js           ← GET  /api/getReport?ac=&startTs=&endTs=
│   ├── getConfigData.js       ← GET  /api/getConfigData
│   ├── refreshCacheAndWarmup.js ← POST /api/refreshCacheAndWarmup
│   ├── clearCache.js          ← POST /api/clearCache
│   ├── sendEmailWithPDF.js    ← POST /api/sendEmailWithPDF
│   └── warmUp.js              ← GET  /api/warmUp  ← cron cada 1 hora
├── public/
│   └── index.html             ← Frontend idéntico + shim google.script.run
├── package.json
├── vercel.json                ← Routes + Cron Jobs
└── .env.example
```

## Tabla de equivalencias

| Apps Script                     | Vercel / Node                                  |
|--------------------------------|------------------------------------------------|
| `SpreadsheetApp.getActiveSpreadsheet()` | `googleapis` con Service Account       |
| `CacheService`                  | Cache en memoria (`api/_lib/cache.js`)         |
| `PropertiesService`             | Map en memoria (`api/_lib/props.js`)           |
| `GmailApp.sendEmail()`          | `nodemailer` via Gmail SMTP                    |
| `DriveApp.getFolderById()`      | Drive API v3 (`googleapis`)                    |
| `UrlFetchApp.fetch()`           | `fetch()` nativo (Node 18)                     |
| `google.script.run`             | Shim JS → `fetch('/api/...')`                  |
| Trigger cada 1 hora             | Vercel Cron Job `"0 * * * *"`                  |
| `Logger.log()`                  | `console.log()` → Vercel Logs                  |

## Variables de entorno

| Variable                     | Descripción                                              |
|-----------------------------|----------------------------------------------------------|
| `GOOGLE_SERVICE_ACCOUNT_KEY` | JSON completo de la clave de la Service Account          |
| `SPREADSHEET_ID`             | ID del Google Spreadsheet                                |
| `SMTP_USER`                  | Gmail desde el que se envían los reportes                |
| `SMTP_PASS`                  | App Password de Gmail (no la contraseña real)            |
| `WARM_UP_SECRET`             | Secreto para disparar warm-up manualmente por URL        |

## Setup paso a paso

### 1. Google Service Account

1. Ir a [Google Cloud Console](https://console.cloud.google.com/)
2. Activar **Google Sheets API** y **Google Drive API**
3. Crear una **Service Account** → generar clave JSON
4. **Compartir el Spreadsheet** con el email de la SA (Lectura)
5. **Compartir las carpetas de Drive** de Config 2.0 con la SA (Editor), si usás el envío de mails con Drive

### 2. Gmail App Password

1. Activar verificación en 2 pasos en la cuenta Gmail
2. Ir a Google Account → Seguridad → Contraseñas de aplicaciones
3. Crear una contraseña para "Correo" → copiar los 16 dígitos
4. Usar ese valor en `SMTP_PASS`

### 3. Variables en Vercel

En Vercel Dashboard → tu proyecto → Settings → Environment Variables, agregar todas las variables del `.env.example`.

Para `GOOGLE_SERVICE_ACCOUNT_KEY`, pegar el JSON en una sola línea:
```bash
cat clave.json | tr -d '\n'
```

### 4. Deploy

```bash
npm i -g vercel
vercel
```
O conectar el repo en vercel.com y hacer push a `main`.

### 5. Desarrollo local

```bash
npm install
cp .env.example .env.local
# Editar .env.local con los valores reales
vercel dev
```

## Cron Job (warm-up cada hora)

`vercel.json` configura:
```json
{ "path": "/api/warmUp", "schedule": "0 * * * *" }
```
Equivale al trigger `setupAutoWarmup()` del Apps Script original.

Para dispararlo manualmente:
```
GET https://tu-app.vercel.app/api/warmUp?secret=TU_WARM_UP_SECRET
```

## Notas sobre el caché

- **Config** (lista de ACs, semanas): 1 hora
- **Data** (9 hojas leídas en paralelo): 1 hora
- **Reportes** por AC: 1 hora
- El warm-up horario invalida todo el caché y recarga las hojas
- El botón "Actualizar versión" en la UI también invalida y recarga
