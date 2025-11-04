CREATE TABLE IF NOT EXISTS public.ventas (
    row_hash TEXT PRIMARY KEY, -- Hash único de la fila (deduplicación)
    fecha TIMESTAMP NULL, -- Fecha de la venta (antes: Date)
    cliente TEXT NULL, -- Nombre o ID del cliente (antes: Customer)
    tipo_documento TEXT NULL, -- Tipo de documento (Factura, Nota, etc.)
    numero TEXT NULL, -- Número del comprobante (antes: Num)
    producto TEXT NULL, -- Código o descripción del producto
    descripcion TEXT NULL, -- Descripción del ítem
    cantidad NUMERIC(15, 3) NULL, -- Cantidad (antes: Qty)
    precio_unitario NUMERIC(15, 2) NULL, -- Precio unitario (antes: SalesPrice)
    monto NUMERIC(18, 2) NULL, -- Importe total (antes: Amount)
    saldo NUMERIC(18, 2) NULL, -- Saldo pendiente (antes: Balance)
    clase TEXT NULL, -- Clase de operación o categoría (antes: Class)
    vendedor TEXT NULL, -- Vendedor o representante (antes: SalesRep)
    tipo_cliente TEXT NULL, -- Tipo de cliente (mayorista, minorista, etc.)
    id BIGINT NULL, -- ID interno (antes: ID)
    mes TEXT NULL -- Mes derivado de fecha (YYYY-MM)
);