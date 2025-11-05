-- === Índices para acelerar las consultas IA ===
CREATE INDEX IF NOT EXISTS idx_ventas_fecha        ON ventas (fecha);
CREATE INDEX IF NOT EXISTS idx_ventas_mes          ON ventas (mes);
CREATE INDEX IF NOT EXISTS idx_ventas_cliente      ON ventas (cliente);
CREATE INDEX IF NOT EXISTS idx_ventas_vendedor     ON ventas (vendedor);
CREATE INDEX IF NOT EXISTS idx_ventas_mes_cliente  ON ventas (mes, cliente);
CREATE INDEX IF NOT EXISTS idx_ventas_mes_vendedor ON ventas (mes, vendedor);
