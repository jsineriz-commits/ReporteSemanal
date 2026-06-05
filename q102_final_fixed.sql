WITH 
-- =================================================================
-- 1. CTES GLOBALES COMPARTIDOS Y REPRESENTANTES
-- =================================================================
aso_comercial AS (
    SELECT st.id AS ID, st.razon_social AS razon_social, concat(u.nombre, ' ', u.apellido) AS asoc_com
    FROM dcac.sociedades_tags AS st FINAL
    LEFT JOIN dcac.usuarios AS u ON st.asociado_comercial = u.usuario 
),
repre_vinc AS (
    SELECT ST.id AS id, ST.razon_social AS razon_social, arrayStringConcat(groupUniqArray(concat(R.nombre, ' ', R.apellido)), ', ') AS representante
    FROM dcac.rel_usuarios_sociedades AS RUS FINAL
    INNER JOIN dcac.clientes_x_representantes AS CXR ON CXR.cliente = RUS.usuario 
    INNER JOIN dcac.representantes AS R ON R.usuario = CXR.representante
    INNER JOIN dcac.sociedades_tags AS ST ON ST.id = RUS.sociedad
    WHERE RUS.usuario != 0 AND RUS.estado = 0 AND ST.estado = 0
    GROUP BY ST.id, ST.razon_social
),
RepreCompraInvernada AS (
    SELECT lxi.revisacion, arrayStringConcat(groupUniqArray(concat(u.nombre, ' ', u.apellido)), ', ') AS RepresentanteCompra
    FROM dcac.lotes_x_interesados AS lxi FINAL LEFT JOIN dcac.usuarios AS u ON u.usuario = lxi.representante_de_compra GROUP BY lxi.revisacion
),
RepreVentaInv AS (
    SELECT r.revisacion AS id, arrayStringConcat(groupUniqArray(concat(u.nombre, ' ', u.apellido)), ', ') AS RepresentanteVenta
    FROM dcac.revisaciones AS r FINAL LEFT JOIN dcac.usuarios AS u ON u.usuario = r.representante GROUP BY r.revisacion
),

-- =================================================================
-- 2. BLOQUES DE DEDUPLICACIÓN PREVIA (EL SECRETO PARA NO QUEDARSE SIN RAM)
-- =================================================================
-- Al forzar 1 fila por ID antes de cruzar, evitamos la Explosión Cartesiana
liq_uniq AS (SELECT * FROM negocios.liquidaciones FINAL ORDER BY toFloat64OrZero(toString(kg_salida_neto)) DESC LIMIT 1 BY negocio),
dc_uniq AS (SELECT * FROM dcac.detalles_carga FINAL ORDER BY toFloat64OrZero(toString(peso_neto)) DESC LIMIT 1 BY revisacion),
lxi_uniq AS (SELECT * FROM dcac.lotes_x_interesados FINAL LIMIT 1 BY revisacion),
ar_neg_uniq AS (SELECT * FROM dcac.analisis_resultados FINAL LIMIT 1 BY negocio),
ar_rev_uniq AS (SELECT * FROM dcac.analisis_resultados FINAL LIMIT 1 BY revisacion),
arp_neg_uniq AS (SELECT * FROM dcac.analisis_resultados_proyectado FINAL LIMIT 1 BY negocio),
arp_rev_uniq AS (SELECT * FROM dcac.analisis_resultados_proyectado FINAL LIMIT 1 BY revisacion),
ib_neg_uniq AS (SELECT * FROM dcac.informes_baja FINAL LIMIT 1 BY negocio),
ib_rev_uniq AS (SELECT * FROM dcac.informes_baja FINAL LIMIT 1 BY revisacion),
cotiz_uniq AS (SELECT * FROM negocios.cd_cotizaciones FINAL LIMIT 1 BY lote_nro),
ir_uniq AS (SELECT * FROM dcac.informes_revisaciones FINAL LIMIT 1 BY revisacion),
lo_uniq AS (SELECT lo_negocio, lo_tipo_liquid, lo_fecha_faena_real, lo_fecha_pago_real FROM negocios.liquidacion_oficial FINAL WHERE lo_tipo_liquid = 'interna' LIMIT 1 BY lo_negocio),

-- =================================================================
-- 3. ESTADOS Y CONCRECIONES
-- =================================================================
c5_datitos AS (
    SELECT r.revisacion AS lote_id, sv.id AS id_soc, toDateOrNull(if(toString(r.fecha_publicacion) IN ('', '0000-00-00'), NULL, toString(r.fecha_publicacion))) AS fecha, 'Invernada' AS Tipo,
        CASE WHEN toString(r.estado) = '4' AND toString(r.no_concretado) = '0' THEN 'Concretadas'
             WHEN toString(r.estado) = '5' THEN 'Dadas de baja'
             WHEN toString(r.estado) = '7' THEN 'No Concretadas' END AS Estado
    FROM dcac.revisaciones AS r FINAL INNER JOIN dcac.sociedades_tags AS sv ON r.sociedad_vendedora = sv.id
    WHERE toString(sv.razon_social) != ''
    UNION ALL 
    SELECT n.id AS lote_id, sv.id AS id_soc, toDateOrNull(if(toString(n.fecha_publicacion) IN ('', '0000-00-00'), NULL, toString(n.fecha_publicacion))) AS fecha, 'Faena' AS Tipo,
        CASE WHEN toInt32OrZero(toString(n.tipo)) IN (3,4,5,7,8,9) AND toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' THEN 'Concretadas'
             WHEN toString(n.borrado) = '1' THEN 'Dadas de baja'
             WHEN toString(n.no_concretado) = '1' THEN 'No Concretadas' END AS Estado
    FROM dcac.negocios AS n FINAL INNER JOIN dcac.sociedades_tags AS sv ON n.sociedad_vendedora = sv.id
    WHERE toString(sv.razon_social) != ''
),
c5_ranked AS (
    SELECT *, row_number() OVER (PARTITION BY id_soc, Tipo ORDER BY fecha DESC) AS rn_tipo, row_number() OVER (PARTITION BY id_soc ORDER BY fecha DESC) AS rn_total
    FROM c5_datitos WHERE Estado IN ('Concretadas', 'No Concretadas')
),
concrecion_ult_5 AS (
    SELECT t_tipo.id_soc, coalesce(t_tipo.Conc_Fae / nullIf(t_tipo.Conc_Fae + t_tipo.NoConc_Fae, 0), 0) AS porc_conc_5_Fae, coalesce(t_tipo.Conc_Inv / nullIf(t_tipo.Conc_Inv + t_tipo.NoConc_Inv, 0), 0) AS porc_conc_5_Inv, coalesce(t_tot.Conc_Tot / nullIf(t_tot.Conc_Tot + t_tot.NoConc_Tot, 0), 0) AS porc_conc_5_tot
    FROM (SELECT id_soc, sum(Estado = 'Concretadas' AND Tipo = 'Faena') AS Conc_Fae, sum(Estado = 'No Concretadas' AND Tipo = 'Faena') AS NoConc_Fae, sum(Estado = 'Concretadas' AND Tipo = 'Invernada') AS Conc_Inv, sum(Estado = 'No Concretadas' AND Tipo = 'Invernada') AS NoConc_Inv FROM c5_ranked WHERE rn_tipo <= 5 GROUP BY id_soc) AS t_tipo
    INNER JOIN (SELECT id_soc, sum(Estado = 'Concretadas') AS Conc_Tot, sum(Estado = 'No Concretadas') AS NoConc_Tot FROM c5_ranked WHERE rn_total <= 5 GROUP BY id_soc) AS t_tot ON t_tipo.id_soc = t_tot.id_soc
),
cg_datitos AS (
    SELECT r.revisacion AS lote_id, sv.id AS id_soc, 'Invernada' AS Tipo, CASE WHEN toString(r.estado) = '4' AND toString(r.no_concretado) = '0' THEN 'Concretadas' WHEN toString(r.estado) = '5' THEN 'Dadas de baja' WHEN toString(r.estado) = '7' THEN 'No Concretadas' WHEN toString(r.estado) IN ('1','3','4','11','12') THEN 'Publicadas' END AS Estado
    FROM dcac.revisaciones AS r FINAL INNER JOIN dcac.sociedades_tags AS sv ON r.sociedad_vendedora = sv.id WHERE toString(sv.razon_social) != ''
    UNION ALL 
    SELECT n.id AS lote_id, sv.id AS id_soc, 'Faena' AS Tipo, CASE WHEN toInt32OrZero(toString(n.tipo)) IN (3,4,5,7,8,9,13) AND toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' THEN 'Concretadas' WHEN toString(n.borrado) = '1' THEN 'Dadas de baja' WHEN toString(n.no_concretado) = '1' THEN 'No Concretadas' WHEN toInt32OrZero(toString(n.tipo)) IN (0,10,11,12) AND toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' THEN 'Publicadas' END AS Estado
    FROM dcac.negocios AS n FINAL INNER JOIN dcac.sociedades_tags AS sv ON n.sociedad_vendedora = sv.id WHERE toString(sv.razon_social) != ''
),
concrecion_gral AS (
    SELECT id_soc, sum(Estado = 'Concretadas' AND Tipo = 'Faena') / nullIf(sum(Estado = 'Concretadas' AND Tipo = 'Faena') + sum(Estado = 'No Concretadas' AND Tipo = 'Faena'), 0) AS conc_gral_fae, sum(Estado = 'Concretadas' AND Tipo = 'Invernada') / nullIf(sum(Estado = 'Concretadas' AND Tipo = 'Invernada') + sum(Estado = 'No Concretadas' AND Tipo = 'Invernada'), 0) AS conc_gral_inv, sum(Estado = 'Concretadas') / nullIf(sum(Estado = 'Concretadas') + sum(Estado = 'No Concretadas'), 0) AS conc_gral, sum(Estado = 'Concretadas') + sum(Estado = 'No Concretadas') + sum(Estado = 'Publicadas') AS Ofrecimientos
    FROM cg_datitos WHERE Estado IS NOT NULL GROUP BY id_soc
),
FechaFaenaReal AS (
    SELECT lo_negocio, max(toDateOrNull(if(toString(lo_fecha_faena_real) IN ('','0000-00-00'), NULL, toString(lo_fecha_faena_real)))) AS lo_fecha_faena_real
    FROM negocios.liquidacion_oficial FINAL WHERE lo_tipo_liquid = 'interna' GROUP BY lo_negocio
),
Estados_FAE AS (
    SELECT n.id AS ID_Negocio,
        CASE 
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='0' AND toString(n.estado)='0' THEN 'OFRECIMIENTOS'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toInt32OrZero(toString(n.tipo)) IN (0,10,11,12) AND toString(n.estado) = '1' THEN 'PUBLICADAS'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='3' AND toString(n.estado) = '1' THEN 'Vendidas'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='9' AND toString(n.estado) = '1' THEN 'A Cargar'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='7' AND toString(n.estado) = '1' THEN 'Cargadas'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='13' AND toString(n.estado) = '1' THEN 'Faenadas'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='4' AND toString(n.estado) = '1' THEN 'A Liquidar'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='5' AND toString(n.estado) = '1' THEN 'Liquidadas'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo) = '8' AND toString(n.estado) = '1' AND (addDays(toDateOrNull(toString(lo.lo_fecha_faena_real)), toInt32OrZero(toString(l.plazo2)) + toInt32OrZero(toString(l.plazo_promedio))) >= today()) AND (toString(lo.lo_fecha_faena_real) NOT IN ('','0000-00-00')) AND (toString(lo.lo_fecha_pago_real) IN ('','0000-00-00') OR lo.lo_fecha_pago_real IS NULL) THEN 'Cerrados'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.estado) = '1' AND toString(n.tipo) = '8' AND (addDays(toDateOrNull(toString(lo.lo_fecha_faena_real)), toInt32OrZero(toString(l.plazo2)) + toInt32OrZero(toString(l.plazo_promedio))) < today()) AND (toString(lo.lo_fecha_faena_real) NOT IN ('','0000-00-00')) AND (toString(lo.lo_fecha_pago_real) IN ('','0000-00-00') OR lo.lo_fecha_pago_real IS NULL) AND toInt32OrZero(toString(l.negocio)) > 7300 THEN 'Pagos Vencidos'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo) = '8' AND toString(n.estado) = '1' AND toString(lo.lo_fecha_pago_real) NOT IN ('','0000-00-00') THEN 'Negocios Terminados'
            ELSE '0'
        END AS ESTADO
    FROM dcac.negocios AS n FINAL
    LEFT JOIN liq_uniq AS l ON n.id = l.negocio
    LEFT JOIN lo_uniq AS lo ON n.id = lo.lo_negocio
    WHERE toString(n.borrado) != '1' AND toString(n.no_concretado) != '1'
    GROUP BY n.id, ESTADO
),
cte_estados_inv AS (
    SELECT RS.revisacion AS ID_Revisacion, 
        CASE
            WHEN toString(RS.estado) = '0' THEN 'OFRECIMIENTOS'
            WHEN toString(RS.estado) = '4' AND toString(RS.estado_b) = '6' AND toString(DTC.fecha_carga_final) NOT IN ('','0000-00-00') AND toString(RS.no_concretado) = '0' AND ( (toString(AR.fecha_pago_real_1) IN ('','0000-00-00') AND today() > addDays(toDateOrNull(toString(DTC.fecha_carga_final)), toInt32OrZero(toString(DTC.plazo_c1)))) OR (toString(AR.fecha_pago_real_1) NOT IN ('','0000-00-00') AND toInt32OrZero(toString(DTC.plazo_c2)) > 0 AND toString(AR.fecha_pago_real_2) IN ('','0000-00-00') AND today() > addDays(toDateOrNull(toString(DTC.fecha_carga_final)), toInt32OrZero(toString(DTC.plazo_c2)))) OR (toString(AR.fecha_pago_real_2) NOT IN ('','0000-00-00') AND toInt32OrZero(toString(DTC.plazo_c3)) > 0 AND toString(AR.fecha_pago_real_3) IN ('','0000-00-00') AND today() > addDays(toDateOrNull(toString(DTC.fecha_carga_final)), toInt32OrZero(toString(DTC.plazo_c3)))) OR (toString(AR.fecha_pago_real_3) NOT IN ('','0000-00-00') AND toInt32OrZero(toString(DTC.plazo_c4)) > 0 AND toString(AR.fecha_pago_real_4) IN ('','0000-00-00') AND today() > addDays(toDateOrNull(toString(DTC.fecha_carga_final)), toInt32OrZero(toString(DTC.plazo_c4)))) ) THEN 'Pagos Vencidos'
            WHEN toString(RS.estado) IN ('3','6','11','12') AND toString(RS.estado_b) = '0' AND toString(RS.no_concretado) = '0' THEN 'Publicadas'   
            WHEN toString(RS.estado) = '4' AND toString(RS.estado_b) = '0' AND toString(RS.no_concretado) = '0' THEN 'Vendidas'
            WHEN toString(RS.estado) = '4' AND toString(RS.estado_b) = '2' AND toString(RS.no_concretado) = '0' THEN 'A Cargar'
            WHEN toString(RS.estado) = '4' AND toString(RS.estado_b) = '3' AND toString(RS.no_concretado) = '0' THEN 'Cargadas'
            WHEN toString(RS.estado) = '4' AND toString(RS.estado_b) = '4' AND toString(RS.no_concretado) = '0' THEN 'A Liquidar'
            WHEN toString(RS.estado) = '4' AND toString(RS.estado_b) = '5' AND toString(RS.no_concretado) = '0' THEN 'Liquidadas'
            WHEN toString(RS.estado) = '4' AND toString(RS.estado_b) = '6' AND toString(RS.no_concretado) = '0' AND ( (toString(AR.fecha_pago_real_1) IN ('','0000-00-00') AND today() <= addDays(toDateOrNull(toString(DTC.fecha_carga_final)), toInt32OrZero(toString(DTC.plazo_c1)))) OR (toString(AR.fecha_pago_real_1) NOT IN ('','0000-00-00') AND toInt32OrZero(toString(DTC.plazo_c2)) > 0 AND toString(AR.fecha_pago_real_2) IN ('','0000-00-00') AND today() <= addDays(toDateOrNull(toString(DTC.fecha_carga_final)), toInt32OrZero(toString(DTC.plazo_c2)))) OR (toString(AR.fecha_pago_real_2) NOT IN ('','0000-00-00') AND toInt32OrZero(toString(DTC.plazo_c3)) > 0 AND toString(AR.fecha_pago_real_3) IN ('','0000-00-00') AND today() <= addDays(toDateOrNull(toString(DTC.fecha_carga_final)), toInt32OrZero(toString(DTC.plazo_c3)))) OR (toString(AR.fecha_pago_real_3) NOT IN ('','0000-00-00') AND toInt32OrZero(toString(DTC.plazo_c4)) > 0 AND toString(AR.fecha_pago_real_4) IN ('','0000-00-00') AND today() <= addDays(toDateOrNull(toString(DTC.fecha_carga_final)), toInt32OrZero(toString(DTC.plazo_c4)))) ) THEN 'Cerradas'
            WHEN toString(RS.estado) = '4' AND toString(RS.estado_b) = '6' AND toString(RS.no_concretado) = '0' AND ( (((toInt32OrZero(toString(DTC.plazo_c1)) != 0 AND toInt32OrZero(toString(DTC.plazo_c2)) != 0 AND toInt32OrZero(toString(DTC.plazo_c3)) != 0 AND toInt32OrZero(toString(DTC.plazo_c4)) != 0) OR (toInt32OrZero(toString(DTC.plazo_c1)) = 0 AND toInt32OrZero(toString(DTC.plazo_c2)) != 0 AND toInt32OrZero(toString(DTC.plazo_c3)) != 0 AND toInt32OrZero(toString(DTC.plazo_c4)) != 0  AND toInt32OrZero(toString(DTC.plazo_c1_p)) > 0) AND toString(AR.fecha_pago_real_1) NOT IN ('','0000-00-00') AND toString(AR.fecha_pago_real_2) NOT IN ('','0000-00-00') AND toString(AR.fecha_pago_real_3) NOT IN ('','0000-00-00') AND toString(AR.fecha_pago_real_4) NOT IN ('','0000-00-00'))) OR ((toInt32OrZero(toString(DTC.plazo_c1)) != 0 AND toInt32OrZero(toString(DTC.plazo_c2)) != 0 AND toInt32OrZero(toString(DTC.plazo_c3)) != 0) OR (toInt32OrZero(toString(DTC.plazo_c1)) = 0 AND toInt32OrZero(toString(DTC.plazo_c2)) != 0 AND toInt32OrZero(toString(DTC.plazo_c3)) != 0 AND toInt32OrZero(toString(DTC.plazo_c1_p)) > 0) AND toString(AR.fecha_pago_real_1) NOT IN ('','0000-00-00') AND toString(AR.fecha_pago_real_2) NOT IN ('','0000-00-00') AND toString(AR.fecha_pago_real_3) NOT IN ('','0000-00-00')) OR ((toInt32OrZero(toString(DTC.plazo_c1)) != 0 AND toInt32OrZero(toString(DTC.plazo_c2)) != 0) OR (toInt32OrZero(toString(DTC.plazo_c1)) = 0 AND toInt32OrZero(toString(DTC.plazo_c2)) != 0 AND toInt32OrZero(toString(DTC.plazo_c1_p)) > 0) AND toString(AR.fecha_pago_real_1) NOT IN ('','0000-00-00') AND toString(AR.fecha_pago_real_2) NOT IN ('','0000-00-00')) OR ((toInt32OrZero(toString(DTC.plazo_c1)) = 0 AND toInt32OrZero(toString(DTC.plazo_c1_p)) > 0) AND toString(AR.fecha_pago_real_1) NOT IN ('','0000-00-00')) OR ((toInt32OrZero(toString(DTC.plazo_c1)) != 0) AND toString(AR.fecha_pago_real_1) NOT IN ('','0000-00-00')) ) THEN 'Negocios Terminados'
        END AS ESTADOS_Invernada 
    FROM dcac.revisaciones AS RS FINAL
    LEFT JOIN dc_uniq AS DTC ON RS.revisacion = DTC.revisacion
    LEFT JOIN ar_rev_uniq AS AR ON RS.revisacion = AR.revisacion
    WHERE toString(RS.estado) IN ('0','2','3','4','6','11','12') AND toInt64OrZero(toString(RS.revisacion)) > 1500
    GROUP BY ID_Revisacion, ESTADOS_Invernada
),
Estados_MAG AS (
    SELECT n.id AS ID_Negocio,
        CASE 
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='3' AND toString(n.mag) = '1' THEN 'Vendidas'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='9' AND toString(n.mag) = '1' THEN 'A Cargar'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='7' AND toString(n.mag) = '1' AND toFloat64OrZero(toString(l.kg_carne)) = 0 THEN 'Cargadas'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='13' AND toString(n.mag) = '1' THEN 'Faenadas'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='4' AND toString(n.mag) = '1' THEN 'A Liquidar'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='5' AND toString(n.mag) = '1' THEN 'Liquidadas'
            WHEN toString(n.borrado) != '1' AND toString(n.no_concretado) != '1' AND toString(n.tipo)='8' AND toString(n.mag) = '1' THEN 'Cerrados'
            ELSE '0'
        END AS ESTADO
    FROM negocios.liquidacion_oficial AS lo FINAL
    LEFT JOIN liq_uniq AS l ON lo.lo_negocio = l.negocio
    LEFT JOIN dcac.negocios AS n ON n.id = lo.lo_negocio
    WHERE toFloat64OrZero(toString(lo.lo_total_cabezas)) != 0 AND toString(n.estado) = '1' AND toString(n.motivo_baja_id) = '0' AND toString(n.no_concretado) = '0' AND toString(n.fecha_vendida) != '0' AND toString(n.mag) = '1' AND toInt64OrZero(toString(n.id)) > 0
    GROUP BY ID_Negocio, ESTADO
),

-- =================================================================
-- 4. PRE-FILTROS DE TABLAS GRANDES (PARA NO QUEMAR MEMORIA RAM)
-- =================================================================
faena_base AS (
    SELECT * FROM dcac.negocios FINAL WHERE toDateOrNull(toString(fecha)) >= '2026-01-01' AND toInt64OrZero(toString(id)) > 0
),
invernada_base AS (
    SELECT * FROM dcac.revisaciones FINAL WHERE toDateOrNull(toString(fecha_hora)) >= '2026-01-01' AND toInt64OrZero(toString(revisacion)) > 0
),

-- =================================================================
-- 5. FAENA FINAL
-- =================================================================
faena_final AS (
    SELECT 
        toInt64OrZero(toString(n.id)) AS ID,
        toString(CASE WHEN toString(n.mag) = '1' THEN 'MAG' ELSE 'FAE' END) AS UN,
        CASE WHEN toString(n.mag) = '1' THEN toDateOrNull(toString(n.fecha_vendida)) WHEN ffr.lo_fecha_faena_real IS NULL THEN toDateOrNull(toString(nl.fecha_faena)) ELSE ffr.lo_fecha_faena_real END AS fecha_operacion,
        toInt32OrZero(toString(toMonth(CASE WHEN toString(n.mag) = '1' THEN toDateOrNull(toString(n.fecha_vendida)) WHEN ffr.lo_fecha_faena_real IS NULL THEN toDateOrNull(toString(nl.fecha_faena)) ELSE ffr.lo_fecha_faena_real END))) AS mes_operacion,
        toInt32OrZero(toString(toYear(CASE WHEN toString(n.mag) = '1' THEN toDateOrNull(toString(n.fecha_vendida)) WHEN ffr.lo_fecha_faena_real IS NULL THEN toDateOrNull(toString(nl.fecha_faena)) ELSE ffr.lo_fecha_faena_real END))) AS anio_operacion,
        toString(stvend.razon_social) AS RS_Vendedora,
        toString(if(concat(uacvend.nombre, ' ', uacvend.apellido) = '' OR uacvend.nombre IS NULL, ac_vend.asoc_com, concat(uacvend.nombre, ' ', uacvend.apellido))) AS asoc_com_vend,
        toString(stcomp.razon_social) AS RS_Compradora, 
        toString(if(concat(uaccomp.nombre, ' ', uaccomp.apellido) = '' OR uaccomp.nombre IS NULL, ac_comp.asoc_com, concat(uaccomp.nombre, ' ', uaccomp.apellido))) AS asoc_com_compra,
        toFloat64OrZero(toString(CASE WHEN toFloat64OrZero(toString(nl.cantidad_liquidada)) = 0 THEN toString(n.cantidad) ELSE toString(nl.cantidad_liquidada) END)) AS Q,
        
        toString(CASE WHEN ef.ESTADO IS NULL THEN em.ESTADO WHEN ef.ESTADO IS NULL AND em.ESTADO IS NULL THEN '' ELSE ef.ESTADO END) AS Estado,
        toString(CASE WHEN concat(uop.nombre, ' ', uop.apellido) = 'Tomas Ignacio Lasarte' THEN 'TL' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Segundo Guevara' THEN 'SG' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Pedro De Hagen' THEN 'PD' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Maximiliano Oliveri' THEN 'MO' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Benjamin Guiraldes' THEN 'BG' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Jorge Torriglia' THEN 'JT' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Celestino Rodriguez' THEN 'CR' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Alberto Pedro Bernaudo' THEN 'AB' WHEN concat(uop.nombre, ' ', uop.apellido) LIKE 'Ignacio Diez Pe_a' THEN 'IDP' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Gonzalo Haedo' THEN 'GH' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Andres Moronell' THEN 'AM' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Segundo Balestra' THEN 'SB' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Maximiliano Salgado' THEN 'MS' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Kevin Ceravolo' THEN 'KC' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Santiago Busquet' THEN 'SB' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Juan Manuel Rial' THEN 'JMR' ELSE concat(uop.nombre, ' ', uop.apellido) END) AS Operador,
        
        toString(fri.nombre_frigorifico) AS Frigorif_Establ_Comprador,
        toDateOrNull(toString(nl.fecha_carga)) AS fecha_carga,
        toString(if(toDateOrNull(toString(nl.fecha_faena)) IS NULL, '', concat(leftPad(toString(toDayOfMonth(toDateOrNull(toString(nl.fecha_faena)))), 2, '0'), '/', leftPad(toString(toMonth(toDateOrNull(toString(nl.fecha_faena)))), 2, '0'), '/', toString(toYear(toDateOrNull(toString(nl.fecha_faena))))))) AS Fecha_Faena,
        toInt64OrNull(toString(stvend.cuit)) AS cuit_vend,
        toInt64OrNull(toString(stcomp.cuit)) AS cuit_comp,
        toString(concat(ucarga.nombre, ' ', ucarga.apellido)) AS op_carga,
        toFloat64OrZero(toString(CASE
            WHEN upper(trim(ef.ESTADO)) IN ('TROPAS A CARGAR', 'TROPAS CARGADAS', 'A CARGAR', 'CARGADAS') AND toFloat64OrZero(toString(arp.rendimiento_proyectado)) != 0 THEN toFloat64OrZero(toString(arp.rendimiento_proyectado))/100
            WHEN upper(trim(ef.ESTADO)) IN ('FAENADAS', 'LIQUIDADAS', 'A LIQUIDAR', 'CERRADOS', 'PAGOS VENCIDOS') AND toFloat64OrZero(toString(ar.rendimiento_final)) != 0 THEN toFloat64OrZero(toString(ar.rendimiento_final))/100
            WHEN toFloat64OrZero(toString(ar.rendimiento_real_total)) != 0 THEN toFloat64OrZero(toString(ar.rendimiento_real_total))/100
            WHEN toFloat64OrZero(toString(ar.rendimiento_final)) != 0 THEN toFloat64OrZero(toString(ar.rendimiento_final))/100
            WHEN toFloat64OrZero(toString(arp.rendimiento_proyectado)) != 0 THEN toFloat64OrZero(toString(arp.rendimiento_proyectado))/100
            WHEN toFloat64OrZero(toString(arp.rendimiento_final)) != 0 THEN toFloat64OrZero(toString(arp.rendimiento_final))/100
            ELSE 0
        END)) AS rend,
        toString(coalesce(nullIf(concat(rv.nombre, ' ', rv.apellido), ''), rv_vend.representante)) AS repre_vendedor,
        toString(rv_comp.representante) AS repre_comprador

    FROM faena_base AS n
    LEFT JOIN liq_uniq AS nl ON n.id = nl.negocio
    LEFT JOIN dcac.sociedades_tags AS stvend ON n.sociedad_vendedora = stvend.id
    LEFT JOIN dcac.sociedades_tags AS stcomp ON nl.sociedad_compradora = stcomp.id
    LEFT JOIN ar_neg_uniq AS ar ON n.id = ar.negocio
    LEFT JOIN arp_neg_uniq AS arp ON n.id = arp.negocio
    LEFT JOIN dcac.usuarios AS acsv ON stvend.asociado_comercial = acsv.usuario
    LEFT JOIN dcac.usuarios AS uacvend ON n.asociado_comercial = uacvend.usuario
    LEFT JOIN dcac.usuarios AS rv ON n.creado_rep = rv.usuario
    LEFT JOIN negocios.liquidacion_oficial AS lo ON n.id = lo.lo_negocio
    LEFT JOIN dcac.usuarios AS acsc ON stcomp.asociado_comercial = acsc.usuario
    LEFT JOIN dcac.usuarios AS uaccomp ON nl.asociado_comercial_comprador = uaccomp.usuario
    LEFT JOIN cotiz_uniq AS cd_cotizaciones ON n.id = cd_cotizaciones.lote_nro
    LEFT JOIN dcac.usuarios AS uc ON n.comprado_por = uc.usuario
    LEFT JOIN ib_neg_uniq AS nc ON n.id = nc.negocio 
    LEFT JOIN (SELECT * FROM negocios.compra_inmediata_faena FINAL LIMIT 1 BY negocio) AS ci ON n.id = ci.negocio AND toString(ci.comprada) = '1'
    LEFT JOIN dcac.usuarios AS us_o ON n.generado_por = us_o.usuario AND toString(us_o.perfil) = '3' 
    LEFT JOIN (SELECT * FROM negocios.cd_logs_estado FINAL LIMIT 1 BY cd_cotizacion_id) AS cdl ON cd_cotizaciones.id = cdl.cd_cotizacion_id AND toString(cdl.estado) = '1'
    LEFT JOIN dcac.usuarios AS us_c ON cdl.usuario = us_c.usuario
    LEFT JOIN negocios.frigorificos AS fri ON nl.frigorifico = fri.frigorifico
    LEFT JOIN dcac.usuarios AS uop ON n.operador = uop.usuario
    LEFT JOIN dcac.usuarios AS ucarga ON nl.operario_carga = ucarga.usuario
    LEFT JOIN repre_vinc AS rv_vend ON n.sociedad_vendedora = rv_vend.id
    LEFT JOIN repre_vinc AS rv_comp ON nl.sociedad_compradora = rv_comp.id
    LEFT JOIN FechaFaenaReal AS ffr ON n.id = ffr.lo_negocio
    LEFT JOIN Estados_FAE AS ef ON n.id = ef.ID_Negocio
    LEFT JOIN Estados_MAG AS em ON n.id = em.ID_Negocio
    LEFT JOIN aso_comercial AS ac_vend ON n.sociedad_vendedora = ac_vend.ID
    LEFT JOIN aso_comercial AS ac_comp ON nl.sociedad_compradora = ac_comp.ID
    
    WHERE CASE 
            WHEN toString(n.mag) = '1' THEN toDateOrNull(toString(n.fecha_vendida))
            WHEN ffr.lo_fecha_faena_real IS NULL THEN toDateOrNull(toString(nl.fecha_faena))
            ELSE ffr.lo_fecha_faena_real
          END >= '2026-01-01'
    ORDER BY toFloat64OrZero(toString(nl.kg_salida_neto)) DESC
    LIMIT 1 BY toInt64OrZero(toString(n.id))
),

-- =================================================================
-- 6. INVERNADA FINAL
-- =================================================================
invernada_final AS (
    SELECT 
        toInt64OrZero(toString(r.revisacion)) AS ID,
        toString(CASE WHEN ir.tipo_precio = 'BULTO' THEN 'CRIA' WHEN ir.tipo_precio = 'KILO' THEN 'INV' END) AS UN,
        CASE WHEN toString(dc.fecha_carga_final) IN ('', '0000-00-00') OR dc.fecha_carga_final IS NULL THEN toDateOrNull(toString(dc.fecha_carga)) ELSE toDateOrNull(toString(dc.fecha_carga_final)) END AS fecha_operacion,
        toInt32OrZero(toString(toMonth(CASE WHEN toString(dc.fecha_carga_final) IN ('', '0000-00-00') OR dc.fecha_carga_final IS NULL THEN toDateOrNull(toString(dc.fecha_carga)) ELSE toDateOrNull(toString(dc.fecha_carga_final)) END))) AS mes_operacion,
        toInt32OrZero(toString(toYear(CASE WHEN toString(dc.fecha_carga_final) IN ('', '0000-00-00') OR dc.fecha_carga_final IS NULL THEN toDateOrNull(toString(dc.fecha_carga)) ELSE toDateOrNull(toString(dc.fecha_carga_final)) END))) AS anio_operacion,
        toString(stvend.razon_social) AS RS_Vendedora,
        toString(if(concat(uacvend.nombre, ' ', uacvend.apellido) = '' OR uacvend.nombre IS NULL, ac_vend.asoc_com, concat(uacvend.nombre, ' ', uacvend.apellido))) AS asoc_com_vend,
        toString(stcomp.razon_social) AS RS_Compradora, 
        toString(if(concat(uaccomp.nombre, ' ', uaccomp.apellido) = '' OR uaccomp.nombre IS NULL, ac_comp.asoc_com, concat(uaccomp.nombre, ' ', uaccomp.apellido))) AS asoc_com_compra,
        toFloat64OrZero(toString(CASE WHEN toFloat64OrZero(toString(dc.cantidad_animales)) = 0 THEN toString(r.cantidad) ELSE toString(dc.cantidad_animales) END)) AS Q,
        
        
        toString(ei.ESTADOS_Invernada) AS Estado,
        
        toString(CASE WHEN concat(uop.nombre, ' ', uop.apellido) = 'Andres Moronell' THEN 'AM' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Benjamin Guiraldes' THEN 'BG' WHEN concat(uop.nombre, ' ', uop.apellido) LIKE 'Ignacio Diez Pe_a' THEN 'IDP' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Pedro Hita' THEN 'PH' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Pedro De Hagen' THEN 'PdH' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Maximiliano Oliveri' THEN 'MO' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Santiago Busquet' THEN 'SB' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Segundo Guevara' THEN 'SG' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Alberto Pedro Bernaudo' THEN 'AB' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Facundo Martin' THEN 'FM' WHEN concat(uop.nombre, ' ', uop.apellido) = 'Gonzalo Haedo' THEN 'GH' ELSE concat(uop.nombre, ' ', uop.apellido) END) AS Operador,
        
        toString(estabcomp.nombre) AS Frigorif_Establ_Comprador,
        toDateOrNull(CASE WHEN toString(dc.fecha_carga_final) IN ('', '0000-00-00') OR dc.fecha_carga_final IS NULL THEN toString(dc.fecha_carga) ELSE toString(dc.fecha_carga_final) END) AS fecha_carga,
        '' AS Fecha_Faena,
        toInt64OrNull(toString(stvend.cuit)) AS cuit_vend,
        toInt64OrNull(toString(stcomp.cuit)) AS cuit_comp,
        toString(concat(ucarga.nombre, ' ', ucarga.apellido)) AS op_carga,
        toFloat64OrZero(toString(CASE
            WHEN upper(trim(ei.ESTADOS_Invernada)) IN ('TROPAS CARGADAS', 'TROPAS A CARGAR', 'CARGADAS', 'A CARGAR') AND toFloat64OrZero(toString(arp.rendimiento_proyectado)) != 0 THEN toFloat64OrZero(toString(arp.rendimiento_proyectado))/100
            WHEN upper(trim(ei.ESTADOS_Invernada)) IN ('FAENADAS', 'LIQUIDADAS', 'TROPAS A LIQUIDAR', 'CERRADAS', 'PAGOS VENCIDOS') AND toFloat64OrZero(toString(ar.rendimiento_final)) != 0 THEN toFloat64OrZero(toString(ar.rendimiento_final))/100
            WHEN toFloat64OrZero(toString(ar.rendimiento_real_total)) != 0 THEN toFloat64OrZero(toString(ar.rendimiento_real_total))/100
            WHEN toFloat64OrZero(toString(ar.rendimiento_final)) != 0 THEN toFloat64OrZero(toString(ar.rendimiento_final))/100
            WHEN toFloat64OrZero(toString(arp.rendimiento_proyectado)) != 0 THEN toFloat64OrZero(toString(arp.rendimiento_proyectado))/100
            WHEN toFloat64OrZero(toString(arp.rendimiento_final)) != 0 THEN toFloat64OrZero(toString(arp.rendimiento_final))/100
            ELSE 0
        END)) AS rend,
        toString(coalesce(nullIf(concat(u_rep_vend.nombre, ' ', u_rep_vend.apellido), ''), rv_vend.representante)) AS repre_vendedor,
        toString(coalesce(nullIf(concat(u_rep_comp.nombre, ' ', u_rep_comp.apellido), ''), rv_comp.representante)) AS repre_comprador

    FROM invernada_base AS r
    LEFT JOIN dc_uniq AS dc ON r.revisacion = dc.revisacion
    LEFT JOIN dcac.sociedades_tags AS stvend ON r.sociedad_vendedora = stvend.id
    LEFT JOIN lxi_uniq AS lxi ON r.revisacion = lxi.revisacion
    LEFT JOIN dcac.sociedades_tags AS stcomp ON lxi.sociedad_compradora = stcomp.id
    LEFT JOIN ar_rev_uniq AS ar ON r.revisacion = ar.revisacion
    LEFT JOIN arp_rev_uniq AS arp ON r.revisacion = arp.revisacion
    LEFT JOIN dcac.usuarios AS acsv ON stvend.asociado_comercial = acsv.usuario
    LEFT JOIN dcac.usuarios AS uacvend ON r.asociado_comercial = uacvend.usuario
    LEFT JOIN dcac.usuarios AS acsc ON stcomp.asociado_comercial = acsc.usuario
    LEFT JOIN dcac.usuarios AS uaccomp ON lxi.asociado_comercial_comprador = uaccomp.usuario
    LEFT JOIN cotiz_uniq AS cd_cotizaciones ON r.revisacion = cd_cotizaciones.lote_nro
    LEFT JOIN ir_uniq AS ir ON r.revisacion = ir.revisacion
    LEFT JOIN dcac.usuarios AS uc ON r.comprado_por = uc.usuario
    LEFT JOIN ib_rev_uniq AS nc ON r.revisacion = nc.revisacion 
    LEFT JOIN dcac.establecimientos AS estabcomp ON lxi.establecimiento_comprador = estabcomp.establecimiento
    LEFT JOIN dcac.usuarios AS uop ON r.adm_solicitud = uop.usuario
    LEFT JOIN aso_comercial AS ac_vend ON r.sociedad_vendedora = ac_vend.ID
    LEFT JOIN aso_comercial AS ac_comp ON lxi.sociedad_compradora = ac_comp.ID
    LEFT JOIN repre_vinc AS rv_vend ON r.sociedad_vendedora = rv_vend.id
    LEFT JOIN repre_vinc AS rv_comp ON lxi.sociedad_compradora = rv_comp.id
    LEFT JOIN dcac.usuarios AS u_rep_vend ON r.representante = u_rep_vend.usuario
    LEFT JOIN dcac.usuarios AS u_rep_comp ON lxi.representante_de_compra = u_rep_comp.usuario
    LEFT JOIN cte_estados_inv AS ei ON r.revisacion = ei.ID_Revisacion
    LEFT JOIN dcac.usuarios AS ucarga ON dc.operario_carga = ucarga.usuario
    
    WHERE CASE 
            WHEN toString(dc.fecha_carga_final) IN ('', '0000-00-00') OR dc.fecha_carga_final IS NULL THEN toDateOrNull(toString(dc.fecha_carga))
            ELSE toDateOrNull(toString(dc.fecha_carga_final))
          END >= '2026-01-01'
    ORDER BY toFloat64OrZero(toString(dc.peso_neto)) DESC
    LIMIT 1 BY toInt64OrZero(toString(r.revisacion))
)

-- =================================================================
-- 7. UNION FINAL 
-- =================================================================
SELECT * FROM (
    SELECT * FROM faena_final
    UNION ALL
    SELECT * FROM invernada_final
)
ORDER BY ID DESC;
