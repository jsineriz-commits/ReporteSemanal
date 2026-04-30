# 🗺️ Mapa del Proyecto — Reporte Semanal DCAC

> Documento vivo. Actualizar cuando cambien lógicas, filtros o estructura de datos.
> Última actualización: 2026-04-29

---

## 1. Arquitectura General

```mermaid
graph TD
    MB[(Metabase\nQ101 · Q102 · Q221)]
    GS[(Google Sheets\nConfig 2.0)]
    API[api/_lib/logic.js\nNode.js / Vercel Serverless]
    CACHE_DISK[Disk Cache\n12h TTL]
    CACHE_MEM[Memory Cache\nIn-process]
    HTML[public/index.html\nSingle Page App]
    USER([Usuario])

    USER -- selecciona AC + Semana --> HTML
    HTML -- google.script.run / fetch /api/report --> API
    API -- getReport() --> CACHE_MEM
    CACHE_MEM -- miss --> CACHE_DISK
    CACHE_DISK -- miss --> MB
    MB -- rows JSON --> API
    GS -- Config ACs/Repres --> API
    API -- datos procesados --> HTML
    HTML -- renderiza --> USER
```

---

## 2. Fuentes de Datos (Metabase)

| Query | Nombre | Contenido | Uso |
|-------|--------|-----------|-----|
| **Q101** | Base Ofrecidas | Lotes publicados/ofrecidos. 1 fila por lote. Incluye estado, AC vendedor, cabezas, rend. | Ofrecidas, CCC, Cotizadas, Ranking Ofrecidas |
| **Q102** | Operaciones | Operaciones concretadas. 1 fila por op. Incluye AC vend/comp, repre vend/comp, Q, rend, estado. | Operadas, Compradas, Top Negocios, Ranking Operadas/Compradas |
| **Q221** | AuxLeads | Actividades CRM: comentarios, agenda. | Sección CRM, Socs. Gestionadas |

---

## 3. Flujo de Carga de Datos

```mermaid
flowchart TD
    A[warmup / scheduledWarmup] --> B[loadData]
    B --> C{Disk cache\nválido < 12h?}
    C -- Sí --> D[Retorna datos del disco]
    C -- No --> E[fetch Q101 desde Metabase]
    E --> F[fetch Q102 desde Metabase]
    F --> G[fetch Q221 desde Metabase]
    G --> H[_processLoadData]
    H --> I[Guarda en diskCache]
    I --> D

    H --> H1["Mapea columnas por nombre\n(oMap, bMap, etc.)"]
    H1 --> H2["Filtra filas inválidas de Q102\n⚠️ BAJA · NO CONCRETADA\nOFRECIMIENTOS · vacío → descarta"]
    H2 --> H3["Construye arrays:\nD.base · D.ops · D.auxLeads"]
```

---

## 4. Procesamiento del Reporte (getReport)

```mermaid
flowchart TD
    START([getReport ac, startTs, endTs]) --> RC{Report cache\nhit?}
    RC -- Sí --> RETURN[Retorna caché]
    RC -- No --> LD[loadData]
    LD --> RANGES["Define rangos de semana:\ninS · inA · inM\nbasados en startTs/endTs"]

    RANGES --> BASE["Loop D.base (Q101)\n→ Ofrecidas, CCC, Cotizadas\n→ rendPonderadoOf"]
    RANGES --> OPS["Loop D.ops (Q102)\n→ allOps del AC actual\n→ top5, allOps, myRendPond"]
    RANGES --> CRM["Loop D.auxLeads (Q221)\n→ comentarios, agenda"]

    OPS --> FILTER["isV = aV===acN OR rV===acN\nisC = aC===acN OR rC===acN\nsi !isV && !isC → skip"]
    FILTER --> STATEOPS["⚠️ Ya filtrado en _processLoadData\nsolo llegan estados CONCRETADOS"]
    STATEOPS --> ALLOPS["allOps.push(\nq, kt, kv, rend, d:[id,un,socV,...]\n)"]

    ALLOPS --> MYREND["myRendPond =\nΣ(rend×q) / Σq\nde allOps\n⚠️ |rend| >= 25% → excluido"]

    RANGES --> RANKING["Loop global D.ops (todos los ACs)\n→ rankingOfrecidas\n→ rankingCompradas\n→ rankingOperadas con rendPond"]

    BASE & OPS & CRM & RANKING --> CACHE2[Guarda en memory cache]
    CACHE2 --> RETURN
```

---

## 5. Cálculo del Ranking Semanal

```mermaid
flowchart TD
    GLOOP["Loop global D.ops\n(todas las ops de la semana)"] --> DEDUP{seenRnkOp\noKey ya visto?}
    DEDUP -- Sí --> SKIP[Skip]
    DEDUP -- No --> MARK[Marca como visto]
    MARK --> INS{inS fecha\nen semana actual?}
    INS -- No --> SKIP
    INS -- Sí --> PARTS["allPartic = {aV, aC, rV, rC}\nrComp = {aC, rC}"]
    PARTS --> QACUM["rOper[x] += q\nrComp[x] += q (solo compradores)"]
    PARTS --> RENDCHECK{"rend ≠ 0 AND\n|rend| < 25%?"}
    RENDCHECK -- No --> NEXT[siguiente op]
    RENDCHECK -- Sí --> RENDACUM["rOperRend[x].sumW += rend×q\nrOperRend[x].cabW += q"]

    QACUM & RENDACUM --> BUILD["rankingOperadas =\n{nombre, q, rendPond=sumW/cabW}\nsorted desc por q"]

    BUILD --> PATCH["En frontend:\nAC actual → rendPond = myRendPond\n(calculado desde allOps del panel)\n→ 100% consistente con lo visible"]
```

---

## 6. Filtros de Datos Críticos

```mermaid
flowchart LR
    subgraph "⚠️ Filtros activos"
        F1["Q102 en _processLoadData:\nEstado = BAJA → descarta\nEstado = NO CONCRETADA → descarta\nEstado = OFRECIMIENTOS → descarta\nEstado vacío → descarta"]
        F2["Rendimiento outliers:\n|rend| >= 25% → muestra '—'\nNo entra en promedios ponderados"]
        F3["Ranking: seenRnkOp\nDeduplication por opId\n→ cada op se cuenta 1 sola vez"]
        F4["allOps (panel ops):\nSolo inS (semana actual)\nisV OR isC del AC seleccionado"]
    end
```

---

## 7. Estructura del Panel Lateral (Side Panel)

```mermaid
stateDiagram-v2
    [*] --> Ranking: Primera carga
    Ranking --> Ranking: Click tab Ofrecidas/Compradas/Operadas
    Ranking --> Ranking: Click filtro Todos/AC/Representantes
    Ranking --> DetalleOF: Click KPI Cabezas Ofrecidas
    Ranking --> DetalleCC: Click KPI Cabezas Compradas
    Ranking --> DetalleOPS: Click Top Negocios (header)
    Ranking --> DetalleOPS: Click punto en gráfico Evolución
    DetalleOF --> Ranking: Click X
    DetalleCC --> Ranking: Click X
    DetalleOPS --> Ranking: Click X

    note right of Ranking
        Persiste al cambiar AC/Semana
        LAST_PANEL_STATE guarda modo+tipo
        renderAll lo restaura con nuevos datos
    end note
```

---

## 8. Fuentes de Datos para cada Vista del Panel

| Vista | Fuente de datos | Formato item |
|-------|----------------|--------------|
| Ranking Ofrecidas | `CURRENT_DATA.rankingOfrecidas` | `{nombre, q}` |
| Ranking Compradas | `CURRENT_DATA.rankingCompradas` | `{nombre, q}` |
| Ranking Operadas | `CURRENT_DATA.rankingOperadas` + patch `myRendPond` | `{nombre, q, rendPond}` |
| Detalle Ofrecidas | `CURRENT_DATA.detOf` | `{soc, est, cot, rend, q, kt, kv, un}` |
| Detalle Compradas | `CURRENT_DATA.detC` | `{soc, rend, q, kt, kv, un}` |
| Ops desde Top Negocios | `CURRENT_DATA.allOps` → `allOpsToDetailFormat()` | `{id, un, soc, fecha, q, kt, kv, lado, rend}` |
| Ops desde gráfico | `CURRENT_DATA.operSemMesDets[weekIdx]` | `{id, un, soc, fecha, q, kt, kv, lado, rend}` |
| Detalle Cargas | `CURRENT_DATA.detCarg` | `{soc, fecha, q, ...}` |

---

## 9. Persistencia del Estado del Panel (LAST_PANEL_STATE)

```mermaid
sequenceDiagram
    participant U as Usuario
    participant FE as index.html
    participant PS as LAST_PANEL_STATE

    U->>FE: Click Cabezas Ofrecidas
    FE->>PS: {mode:'detail', type:'of'}
    U->>FE: Cambia AC (navAC / dropdown)
    FE->>FE: autoGen() → panel.classList.remove('is-visible')\n(NO llama closeSidePanel → NO resetea PS)
    FE->>FE: renderAll(newData)
    FE->>PS: lee LAST_PANEL_STATE
    PS->>FE: {mode:'detail', type:'of'}
    FE->>FE: showDetailPanel('of', newData.detOf)
    FE->>U: Panel restaurado con datos nuevos
```

---

## 10. Flujo Completo: Cambio de AC

```mermaid
flowchart TD
    A([Usuario cambia AC]) --> B["autoGen()\npanel.classList.remove is-visible\n⚠️ NO resetea LAST_PANEL_STATE"]
    B --> C{L_CACHE hit?}
    C -- Sí --> D[renderAll con datos cacheados]
    C -- No --> E[showSkeletons\nfetch /api/report]
    E --> F[renderAll con nuevos datos]
    D & F --> G["CURRENT_DATA = newData"]
    G --> H{LAST_PANEL_STATE?}
    H -- null --> I[showDefaultRanking Ofrecidas]
    H -- ranking --> J["showDefaultRanking(type, roleFlt)"]
    H -- "detail 'of'" --> K["showDetailPanel('of', newData.detOf)"]
    H -- "detail 'comp'" --> L["showDetailPanel('comp', newData.detC)"]
    H -- "detail 'ops' source='all'" --> M["allOpsToDetailFormat(newData.allOps)\nshowDetailPanel('ops', ...)"]
    H -- "detail 'ops' source='week'" --> N["newData.operSemMesDets[weekIdx]\nshowDetailPanel('ops', ...)"]
    I & J & K & L & M & N --> O[renderCore · renderCRM · renderExtras · renderSACs]
```

---

## 11. Campos Clave en arrays internos

### D.ops (por elemento, índice del array)
| Índice | Campo | Fuente Q102 |
|--------|-------|-------------|
| 0 | aV (AC vendedor normalizado) | asoc_com_vend |
| 1 | aC (AC comprador normalizado) | asoc_com_compra |
| 2 | f (fecha operación) | fecha_operacion |
| 4 | Q total | q |
| 5 | socV | rs_vendedora |
| 6 | socC | rs_compradora |
| 8 | ID operación | id |
| 9 | UN | un |
| 10 | Cat / Estado | estado |
| 18 | rV (repre vendedor) | repre_vendedor |
| 19 | rC (repre comprador) | repre_comprador |
| 20 | rend (decimal, ej: 0.039 = 3.9%) | rend |

### allOps (por item)
```
{ q, kt, kv, ktC, kvC, rend,
  d: [id, un, socV, acV, socC, acC, fecha, q, tieneCargar, lado] }
```

### operSemMesDets (formato para panel desde gráfico)
```
{ id, un, soc, fecha, q, kt, kv, lado, rend }
```

---

## 12. Consideraciones y Reglas de Negocio

> [!IMPORTANT]
> **Filtro de estados (Q102)**: Solo pasan operaciones con estado CONCRETADA (o equivalente). Estados descartados: BAJA, NO CONCRETADA, OFRECIMIENTOS, vacío. Este filtro se aplica en `_processLoadData` al construir `D.ops`.

> [!WARNING]
> **Rendimiento outliers**: Valores `|rend| >= 25%` se descartan de todos los promedios y se muestran como `—` en badges. Se considera dato erróneo en la fuente.

> [!NOTE]
> **Consistencia ranking vs panel**: El `rendPond` del AC actualmente seleccionado en el Ranking Operadas se reemplaza por `myRendPond` (calculado desde `allOps` del panel). Los demás ACs usan el cálculo global del ranking.

> [!NOTE]
> **Repre en operaciones**: Las ops donde el AC es `repre_vendedor` o `repre_comprador` (no AC directo) SÍ aparecen en su panel de operaciones. La condición es `isV = (aV===acN) OR (rV===acN)`.

> [!TIP]
> **KT/KV tags**: `kt` = categoría tipo (FAE VEND, FAE COMP, INV VEND, INV COMP, etc.), `kv` = valor numérico de la categoría. Estos se calculan con `getKtKv()` y determinan el color del badge.

---

## 13. Archivos Principales

```
reporte semanal vercel/
├── api/
│   ├── _lib/
│   │   ├── logic.js          ← NÚCLEO: toda la lógica de datos y cálculos
│   │   ├── cache.js          ← Memory cache con TTL
│   │   ├── diskCache.js      ← Disk cache (12h) para datos Metabase
│   │   └── blobCache.js      ← Vercel Blob storage (prod)
│   └── report.js             ← Endpoint serverless /api/report
├── public/
│   └── index.html            ← SPA completa: UI + JS + CSS inline
├── local-dev.js              ← Servidor Express local (puerto 4000)
├── vercel.json               ← Config Vercel: rutas + env
└── MAPA_PROYECTO.md          ← Este archivo ← MANTENER ACTUALIZADO
```
