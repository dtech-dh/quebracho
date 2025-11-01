CREATE TABLE IF NOT EXISTS ventas (
    "Date" TIMESTAMP,
    "Customer" TEXT,
    "TipoDocumento" TEXT,
    "Num" TEXT,
    "Producto" TEXT,
    "Descripcion" TEXT,
    "Qty" DOUBLE PRECISION,
    "SalesPrice" TEXT,
    "Amount" DOUBLE PRECISION,
    "Balance" DOUBLE PRECISION,
    "Class" TEXT,
    "SalesRep" TEXT,
    "TipoCliente" TEXT,
    "ID" INTEGER,
    "Month" TEXT,
    "row_hash" TEXT PRIMARY KEY
);