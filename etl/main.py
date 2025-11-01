
# Cambios realizados en tu script:
# - Agregado bloque de chequeo inicial de estructura y conteos antes del scheduler.
# - Mostrando nombres de tablas y columnas desde ambas bases de datos.

import os
import json
import logging
import datetime
import pandas as pd
import psycopg2
import pyodbc #type: ignore
import hashlib
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler #type: ignore

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

def hash_row(row):
    concat = "|".join([str(row[col]) for col in row.index])
    return hashlib.sha256(concat.encode("utf-8")).hexdigest()

def ensure_pg_table(conn, df: pd.DataFrame):
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
    logging.info(f"Tabla {TARGET_TABLE} verificada/creada.")

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
        query = f'''
        SELECT *
        FROM [Sheet1$]
        WHERE 1=1
--          AND TRY_CONVERT(DATE, [Date], 103) >= '{start.date()}'
--          AND TRY_CONVERT(DATE, [Date], 103) < '{end.date()}'
        '''
        df = pd.read_sql(query, conn)

    if df.empty:
        logging.info("Sin nuevas filas.")
        return None

    df["Month"] = pd.to_datetime(df["Date"], errors='coerce').dt.strftime("%Y-%m")
    df["row_hash"] = df.apply(hash_row, axis=1)
    logging.info(f"{len(df)} filas leídas.")
    return df

def initial_diagnostics():
    logging.info("===== Diagnóstico inicial =====")

    try:
        # SQL Server
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
            result = cursor.fetchone()
            count = result[0] if result else 0
            cursor.execute("SELECT TOP 1 * FROM [Sheet1$]")
            columns = [column[0] for column in cursor.description]
            logging.info(f"Origen [Sheet1$] columnas: {columns}")
            logging.info(f"Origen [Sheet1$] cantidad de registros: {count}")
    except Exception as e:
        logging.error(f"Error al conectar con SQL Server: {e}")

    try:
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG["host"],
            port=POSTGRES_CONFIG["port"],
            database=POSTGRES_CONFIG["database"],
            user=POSTGRES_CONFIG["user"],
            password=POSTGRES_CONFIG["password"]
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM \"{TARGET_TABLE}\"")
        result = cursor.fetchone()
        count = result[0] if result else 0
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{TARGET_TABLE}'")
        columns = [row[0] for row in cursor.fetchall()]
        logging.info(f"Destino [{TARGET_TABLE}] columnas: {columns}")
        logging.info(f"Destino [{TARGET_TABLE}] cantidad de registros: {count}")
        logging.info(f"Destino [{TARGET_TABLE}] cantidad de registros: {count}")
        conn.close()
    except Exception as e:
        logging.error(f"Error al conectar con PostgreSQL: {e}")

    logging.info("===== Fin diagnóstico =====")

def load_to_pg(df: pd.DataFrame):
    conn = psycopg2.connect(
        host=POSTGRES_CONFIG["host"],
        port=POSTGRES_CONFIG["port"],
        database=POSTGRES_CONFIG["database"],
        user=POSTGRES_CONFIG["user"],
        password=POSTGRES_CONFIG["password"]
    )
    ensure_pg_table(conn, df)

    cols = df.columns.tolist()
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = f'''
    INSERT INTO "{TARGET_TABLE}" ({", ".join([f'"{c}"' for c in cols])})
    VALUES ({placeholders})
    ON CONFLICT ("row_hash") DO NOTHING;
    '''
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(insert_sql, tuple(row))
    conn.commit()
    conn.close()
    logging.info(f"{len(df)} filas procesadas (con deduplicación por hash).")

def job():
    try:
        df = fetch_data()
        if df is not None:
            load_to_pg(df)
        else:
            logging.info("No hay datos nuevos para insertar.")
    except Exception as e:
        logging.exception("Error durante el ETL.")

if __name__ == "__main__":
    initial_diagnostics()
    scheduler = BlockingScheduler()
    scheduler.add_job(job, 'interval', hours=4, next_run_time=datetime.datetime.now())
    logging.info("Worker ETL con deduplicación por hash iniciado.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Detenido.")
