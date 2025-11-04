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

    # 🔁 Renombrar columnas según el mapa
    df.rename(columns=COLUMN_MAP, inplace=True)
    df.columns = [c.lower() for c in df.columns]

    # 📅 Agregar mes (YYYY-MM)
    if "fecha" in df.columns:
        df["mes"] = pd.to_datetime(df["fecha"], errors='coerce').dt.strftime("%Y-%m")

    # 🧮 Hash deduplicador
    df["row_hash"] = df.apply(hash_row, axis=1)

    logging.info(f"{len(df)} filas leídas y normalizadas.")
    return df


# ===========================================================
# CARGA EN POSTGRES (robusta y con saneo de datos)
# ===========================================================
def load_to_pg(df: pd.DataFrame):
    """Carga incremental deduplicada en Postgres, con validación detallada."""
    if df is None or df.empty:
        logging.warning("⚠️ DataFrame vacío, no se insertará nada.")
        return

    # 🔹 Limpieza básica
    df = df.fillna("").replace("NaT", "")
    df = df.astype(str)
    cols = df.columns.tolist()

    # 🔹 Saneado de columnas numéricas
    for col in ["cantidad", "precio_unitario", "monto", "saldo"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .replace(r"[^0-9\.\-]", "", regex=True)  # solo números y puntos
                .replace("", "0")
            )

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    ensure_pg_table(conn, df)

    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = f'''
    INSERT INTO "{TARGET_TABLE}" ({", ".join([f'"{c}"' for c in cols])})
    VALUES ({placeholders})
    ON CONFLICT ("row_hash") DO NOTHING;
    '''

    inserted = 0
    failed = 0
    error_samples = []

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
                    if len(error_samples) < 5:
                        error_samples.append(str(e))
                    continue

    conn.close()

    logging.info("=== RESULTADO DE CARGA ===")
    logging.info(f"✅ Filas insertadas correctamente: {inserted}")
    if failed > 0:
        logging.warning(f"⚠️ Filas con error: {failed}")
        for err in error_samples:
            logging.warning(f"  → {err}")
    else:
        logging.info("🎯 Todas las filas insertadas correctamente.")


# ===========================================================
# VALIDACIÓN CRUZADA SQLSERVER ↔ POSTGRES
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
        cursor_pg = conn_pg.cursor()
        cursor_pg.execute(f'SELECT COUNT(*) FROM "{TARGET_TABLE}"')
        dest_count = cursor_pg.fetchone()[0]
        conn_pg.close()

        diff = abs(src_count - dest_count)
        logging.info(f"✅ Validación cruzada: SQLServer={src_count} vs Postgres={dest_count} (Δ={diff})")
        if diff > 0:
            logging.warning(f"⚠️ Diferencia detectada de {diff} registros entre origen y destino.")
        else:
            logging.info("🎯 Las cantidades coinciden perfectamente.")
    except Exception as e:
        logging.error(f"Error durante validación cruzada: {e}")


# ===========================================================
# DIAGNÓSTICO INICIAL
# ===========================================================
def initial_diagnostics():
    logging.info("===== Diagnóstico inicial =====")
    logging.info(f"📘 Mapeo activo SQLServer → Postgres:\n{json.dumps(COLUMN_MAP, indent=2, ensure_ascii=False)}")

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
            cursor.execute("SELECT TOP 1 * FROM [Sheet1$]")
            columns = [c[0] for c in cursor.description]
            cursor.execute("SELECT COUNT(*) FROM [Sheet1$]")
            count = cursor.fetchone()[0]
            logging.info(f"Origen columnas: {columns}")
            logging.info(f"Origen cantidad: {count}")
    except Exception as e:
        logging.error(f"Error al conectar con SQL Server: {e}")

    try:
        conn_pg = psycopg2.connect(**POSTGRES_CONFIG)
        cursor_pg = conn_pg.cursor()
        cursor_pg.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{TARGET_TABLE}'")
        columns = [r[0] for r in cursor_pg.fetchall()]
        cursor_pg.execute(f"SELECT COUNT(*) FROM \"{TARGET_TABLE}\"")
        count = cursor_pg.fetchone()[0]
        logging.info(f"Destino columnas: {columns}")
        logging.info(f"Destino cantidad: {count}")
        conn_pg.close()
    except Exception as e:
        logging.error(f"Error al conectar con PostgreSQL: {e}")

    logging.info("===== Fin diagnóstico =====")


# ===========================================================
# JOB PRINCIPAL
# ===========================================================
def job():
    try:
        df = fetch_data()
        if df is not None and not df.empty:
            load_to_pg(df)
        else:
            logging.info("No hay datos nuevos para insertar.")
        cross_validate()
    except Exception:
        logging.exception("Error durante el ETL.")


# ===========================================================
# MAIN
# ===========================================================
if __name__ == "__main__":
    initial_diagnostics()
    scheduler = BlockingScheduler()
    scheduler.add_job(job, 'interval', hours=4, next_run_time=datetime.datetime.now())
    logging.info("Worker ETL con deduplicación + validación cruzada iniciado.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Detenido.")
