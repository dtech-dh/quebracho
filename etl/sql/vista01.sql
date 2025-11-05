DROP MATERIALIZED VIEW IF EXISTS mv_ventas_mensuales;

CREATE MATERIALIZED VIEW mv_ventas_mensuales AS
SELECT
    mes,
    cliente,
    vendedor,
    SUM(COALESCE(monto::numeric, 0)) AS total_ventas,
    SUM(
        COALESCE(cantidad::numeric, 0)
    ) AS total_unidades
FROM ventas
GROUP BY
    mes,
    cliente,
    vendedor;

-- Índices para lectura rápida
CREATE INDEX IF NOT EXISTS idx_mv_mes ON mv_ventas_mensuales (mes);

CREATE INDEX IF NOT EXISTS idx_mv_cliente ON mv_ventas_mensuales (cliente);

CREATE INDEX IF NOT EXISTS idx_mv_vendedor ON mv_ventas_mensuales (vendedor);