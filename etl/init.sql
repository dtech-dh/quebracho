CREATE TABLE IF NOT EXISTS ventas (
    "date" TIMESTAMP,
    "customer" TEXT,
    "tipodocumento" TEXT,
    "num" TEXT,
    "producto" TEXT,
    "descripcion" TEXT,
    "qty" DOUBLE PRECISION,
    "salesprice" TEXT,
    "amount" DOUBLE PRECISION,
    "balance" DOUBLE PRECISION,
    "class" TEXT,
    "salesrep" TEXT,
    "tipocliente" TEXT,
    "id" INTEGER,
    "month" TEXT,
    "row_hash" TEXT PRIMARY KEY
);