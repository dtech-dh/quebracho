import os, re
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text #type: ignore
from dotenv import load_dotenv

load_dotenv()

class PostgresMCP:
    """
    Mini-SQL soportado:
      SELECT [DISTINCT] col | SUM(col)|AVG|MAX|MIN|COUNT(*)|COUNT(col)|COUNT(DISTINCT col)
      WHERE Year=YYYY [AND Month=M] [AND Date='YYYY-MM-DD'] [AND Date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD']
      GROUP BY Year|Month|Day|col
      ORDER BY <col|SUM(col)|COUNT(...)|...> [ASC|DESC]
      LIMIT N
    Ejecuta contra la tabla TABLE_NAME en Postgres.
    """

    def __init__(self):
        DB = os.getenv("POSTGRES_DB")
        USER = os.getenv("POSTGRES_USER")
        PWD = os.getenv("POSTGRES_PASSWORD")
        HOST = os.getenv("POSTGRES_HOST", "db")
        PORT = os.getenv("POSTGRES_PORT", "5432")
        self.table = os.getenv("TABLE_NAME", "ventas")

        self.engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")

        with self.engine.begin() as conn:
            cols = conn.execute(text(f'SELECT * FROM "{self.table}" LIMIT 0')).keys()
        self.columns = set(cols)

    def _resolve_col(self, col: str) -> str:
        for c in self.columns:
            if c.lower() == col.strip().lower():
                return c
        return col.strip()

    def _resolve_agg(self, expr: str) -> str:
        inner = re.search(r"\(\s*([^)]+)\s*\)", expr)
        func = expr.split("(")[0].upper()
        col = inner.group(1).strip() if inner else ""
        if col == "*":
            return f"{func}(*)"
        if col.lower().startswith("distinct "):
            inner_col = col[9:].strip()
            match = self._resolve_col(inner_col)
            return f'{func}(DISTINCT "{match}")'
        match = self._resolve_col(col)
        return f'{func}("{match}")'

    def build_sql(self, mini: str) -> str:
        # (idéntico al tuyo, sin cambios importantes)
        # sql = f'SELECT * FROM "{self.table}"'
        return sql + ";"

    def run_sql(self, mini: str):
        sql = self.build_sql(mini)
        try:
            with self.engine.begin() as conn:
                return pd.read_sql(text(sql), con=conn)
        except Exception as e:
            return {"error": str(e), "sql": sql}

def get_table_schema(table_name: str):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=os.getenv("POSTGRES_PORT", 5432),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB")
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
    """, (table_name,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"column": r[0], "type": r[1]} for r in rows]
