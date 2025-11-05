import os
import json
import logging
import datetime
import pandas as pd
import psycopg2
import pyodbc  # type: ignore
import hashlib
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler  # type: ignore

# ===========================================================
# CONFIGURACIÓN GENERAL
# ===========================================================
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SQL_CONFIG = {
    "host": os.getenv("SQL_HOST"),
    "port": os.getenv("SQL_PORT"),
    "database": os.getenv("SQL_DATABASE"),
    "user": os.getenv("SQL_USER"),
    "password": os.getenv("SQL_PASSWORD"),
}

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

TARGET_TABLE = os.getenv("POSTGRES_TARGET_TABLE", "ventas")

# ===========================================================
# MAPEO DE CAMPOS SQLSERVER → POSTGRES
# ===========================================================
COLUMN_MAP = {
    "Date": "fecha",
    "Customer": "cliente",
    "TipoDocumento": "tipo_documento",
    "Num": "numero",
    "Producto": "producto",
    "Descripcion": "descripcion",
    "Qty": "cantidad",
    "SalesPrice": "precio_unitario",
    "Amount": "monto",
    "Balance": "saldo",
    "Class": "clase",
    "SalesRep": "vendedor",
    "TipoCliente": "tipo_cliente",
    "ID": "id",
    "Month": "mes",
}

# ===========================================================
# FUNCIONES AUXILIARES
# ===========================================================
def hash_row(row):
    concat = "|".join([str(row[col]) for col in row.index])
    return hashlib.sha256(concat.encode("utf-8")).hexdigest()


def ensure_pg_table(conn, df: pd.DataFrame):
    """Crea la tabla si no existe, usando los nombres ya mapeados."""
    df.columns = [c.lower() for c in df.columns]
    cols = df.columns.tolist()
    col_defs = []
    for c in cols:
        if c == "row_hash":
            col_defs.append('"row_hash" TEXT PRIMARY KEY')
        else:
            col_defs.append(f'"{c}" TEXT')
    ddl = f'''
    CREATE TABLE IF NOT EXISTS "{TARGET_TABLE}" (
        {", ".join(col_defs)}
    );
    '''
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    logging.info(f"Tabla {TARGET_TABLE} verificada/creada con columnas: {cols}")


# ===========================================================
# CREACIÓN DE ÍNDICES Y VISTAS OPTIMIZADAS
# ===========================================================
def ensure_indexes_and_views():
    """Crea índices y vista materializada si no existen."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()

        logging.info("🧱 Verificando índices y vista materializada...")

        # Índices clave
        index_sql = [
            f'CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_fecha        ON "{TARGET_TABLE}" (fecha);',
            f'CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_mes          ON "{TARGET_TABLE}" (mes);',
            f'CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_cliente      ON "{TARGET_TABLE}" (cliente);',
            f'CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_vendedor     ON "{TARGET_TABLE}" (vendedor);',
            f'CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_mes_cliente  ON "{TARGET_TABLE}" (mes, cliente);',
            f'CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_mes_vendedor ON "{TARGET_TABLE}" (mes, vendedor);'
        ]
        for sql in index_sql:
            cur.execute(sql)

        # Vista materializada
        cur.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ventas_mensuales AS
            SELECT
                mes,
                cliente,
                vendedor,
                SUM(COALESCE(monto::numeric,0)) AS total_ventas,
                SUM(COALESCE(cantidad::numeric,0)) AS total_unidades
            FROM ventas
            GROUP BY mes, cliente, vendedor;
        """)

        # Índices para la vista
        cur.execute('CREATE INDEX IF NOT EXISTS idx_mv_mes ON mv_ventas_mensuales(mes);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_mv_cliente ON mv_ventas_mensuales(cliente);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_mv_vendedor ON mv_ventas_mensuales(vendedor);')

        conn.commit()
        conn.close()
        logging.info("✅ Índices y vista materializada verificados correctamente.")
    except Exception as e:
        logging.error(f"Error al crear índices/vistas: {e}")


def refresh_materialized_view():
    """Actualiza la vista materializada si existe."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ventas_mensuales;")
        conn.commit()
        conn.close()
        logging.info("♻️ Vista materializada mv_ventas_mensuales actualizada correctamente.")
    except Exception as e:
        logging.warning(f"No se pudo refrescar la vista materializada: {e}")


# ===========================================================
# EXTRACCIÓN
# ===========================================================
def fetch_data():
    start = datetime.datetime.now() - datetime.timedelta(hours=4)
    end = datetime.datetime.now()
    logging.info(f"Extrayendo datos desde {start.date()} hasta {end.date()}")

    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={SQL_CONFIG["host"]},{SQL_CONFIG["port"]};'
        f'DATABASE={SQL_CONFIG["database"]};'
        f'UID={SQL_CONFIG["user"]};'
        f'PWD={SQL_CONFIG["password"]}'
    )

    with pyodbc.connect(conn_str, timeout=30) as conn:
        query = "SELECT * FROM [Sheet1$];"
        df = pd.read_sql(query, conn)

    if df.empty:
        logging.info("Sin nuevas filas.")
        return None

    df.rename(columns=COLUMN_MAP, inplace=True)
    df.columns = [c.lower() for c in df.columns]

    if "fecha" in df.columns:
        df["mes"] = pd.to_datetime(df["fecha"], errors="coerce").dt.strftime("%Y-%m")

    df["row_hash"] = df.apply(hash_row, axis=1)

    logging.info(f"{len(df)} filas leídas y normalizadas.")
    return df


# ===========================================================
# CARGA EN POSTGRES
# ===========================================================
def load_to_pg(df: pd.DataFrame):
    if df is None or df.empty:
        logging.warning("⚠️ DataFrame vacío, no se insertará nada.")
        return

    df = df.fillna("").replace("NaT", "")
    df = df.astype(str)
    cols = df.columns.tolist()

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    ensure_pg_table(conn, df)

    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = f'''
    INSERT INTO "{TARGET_TABLE}" ({", ".join([f'"{c}"' for c in cols])})
    VALUES ({placeholders})
    ON CONFLICT ("row_hash") DO NOTHING;
    '''

    inserted, failed = 0, 0
    with conn:
        with conn.cursor() as cur:
            for i, (_, row) in enumerate(df.iterrows(), start=1):
                try:
                    cur.execute(insert_sql, tuple(row))
                    if cur.rowcount > 0:
                        inserted += 1
                    if i % 1000 == 0:
                        logging.info(f"📦 Procesadas {i} filas...")
                except Exception as e:
                    failed += 1
                    conn.rollback()
                    if failed <= 3:
                        logging.warning(f"⚠️ Error en fila {i}: {e}")
                    continue
    conn.close()
    logging.info(f"✅ {inserted} filas insertadas. ❌ {failed} errores.")


# ===========================================================
# VALIDACIÓN CRUZADA
# ===========================================================
def cross_validate():
    try:
        conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={SQL_CONFIG["host"]},{SQL_CONFIG["port"]};'
            f'DATABASE={SQL_CONFIG["database"]};'
            f'UID={SQL_CONFIG["user"]};'
            f'PWD={SQL_CONFIG["password"]}'
        )
        with pyodbc.connect(conn_str, timeout=30) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM [Sheet1$]")
            src_count = cursor.fetchone()[0]

        conn_pg = psycopg2.connect(**POSTGRES_CONFIG)
        cur_pg = conn_pg.cursor()
        cur_pg.execute(f'SELECT COUNT(*) FROM "{TARGET_TABLE}"')
        dest_count = cur_pg.fetchone()[0]
        conn_pg.close()

        diff = abs(src_count - dest_count)
        logging.info(f"🔎 Validación: SQLServer={src_count} | Postgres={dest_count} | Δ={diff}")
    except Exception as e:
        logging.error(f"Error en validación cruzada: {e}")


# ===========================================================
# JOB PRINCIPAL
# ===========================================================
def job():
    try:
        ensure_indexes_and_views()
        df = fetch_data()
        if df is not None:
            load_to_pg(df)
            refresh_materialized_view()
            cross_validate()
        else:
            logging.info("No hay datos nuevos para insertar.")
    except Exception:
        logging.exception("Error durante el ETL.")


# ===========================================================
# MAIN
# ===========================================================
if __name__ == "__main__":
    logging.info("🚀 Iniciando ETL automatizado con índices + vistas + validación...")
    scheduler = BlockingScheduler()
    scheduler.add_job(job, 'interval', hours=4, next_run_time=datetime.datetime.now())
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Detenido.")
