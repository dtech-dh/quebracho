import os, logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

class PostgresMCP:
    """
    MCP para ejecutar consultas directas sobre PostgreSQL.
    Usa exactamente los nombres de columnas y tipos reales de producción,
    sin alterar case ni agregar comillas automáticas.
    """

    def __init__(self):
        DB = os.getenv("POSTGRES_DB")
        USER = os.getenv("POSTGRES_USER")
        PWD = os.getenv("POSTGRES_PASSWORD")
        HOST = os.getenv("POSTGRES_HOST", "db")
        PORT = os.getenv("POSTGRES_PORT", "5432")
        self.table = os.getenv("TABLE_NAME", "ventas")

        self.engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")

        # Cache de columnas (solo para info)
        with self.engine.begin() as conn:
            cols = conn.execute(text(f"SELECT * FROM {self.table} LIMIT 0")).keys()
        self.columns = list(cols)
        logging.info(f"🗃️ Tabla detectada: {self.table} con columnas {self.columns}")

    # -------------------------------------------------------------------------
    def validate_sql(self, sql: str) -> dict:
        """Valida sintaxis con EXPLAIN antes de ejecutar."""
        try:
            with self.engine.begin() as conn:
                plan = conn.execute(text(f"EXPLAIN {sql}")).fetchall()
            return {"valid": True, "plan": [r[0] for r in plan]}
        except Exception as e:
            return {"valid": False, "error": str(e)}

    # -------------------------------------------------------------------------
    def run_sql(self, sql: str) -> pd.DataFrame:
        """Ejecuta SQL directo contra la base sin modificarlo."""
        try:
            logging.info(f"🚀 Ejecutando SQL:\n{sql}")

            validation = self.validate_sql(sql)
            if not validation["valid"]:
                logging.error(f"❌ SQL inválido: {validation['error']}")
                return pd.DataFrame({"error": [validation["error"]], "sql": [sql]})

            with self.engine.begin() as conn:
                df = pd.read_sql(text(sql), con=conn)
            return df

        except Exception as e:
            logging.error(f"❌ Error al ejecutar SQL: {e}")
            return pd.DataFrame({"error": [str(e)], "sql": [sql]})


# -------------------------------------------------------------------------
def get_table_schema(table_name: str):
    """Devuelve el esquema de una tabla."""
    DB = os.getenv("POSTGRES_DB")
    USER = os.getenv("POSTGRES_USER")
    PWD = os.getenv("POSTGRES_PASSWORD")
    HOST = os.getenv("POSTGRES_HOST", "db")
    PORT = os.getenv("POSTGRES_PORT", "5432")

    engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = :t
            ORDER BY ordinal_position;
        """), {"t": table_name}).fetchall()

    return [{"column": r[0], "type": r[1]} for r in rows]
