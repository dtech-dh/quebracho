import os, re
import pandas as pd
from sqlalchemy import create_engine, text # type: ignore
from dotenv import load_dotenv

load_dotenv()

class PostgresMCP:
    """
    MCP (Mini Command Processor) robusto:
      - Soporta columnas con o sin comillas (case-sensitive aware)
      - Auto-corrige alias y ORDER BY
      - Usa comillas solo cuando son necesarias
    """

    def __init__(self):
        DB = os.getenv("POSTGRES_DB")
        USER = os.getenv("POSTGRES_USER")
        PWD = os.getenv("POSTGRES_PASSWORD")
        HOST = os.getenv("POSTGRES_HOST", "db")
        PORT = os.getenv("POSTGRES_PORT", "5432")
        self.table = os.getenv("TABLE_NAME", "ventas")

        self.engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")

        # Leer columnas reales
        with self.engine.begin() as conn:
            cols = conn.execute(text(f'SELECT * FROM {self.table} LIMIT 0')).keys()
        self.columns = set(cols)

        # Detectar si la DB es case-insensitive (todas minúsculas)
        self.lowercase_mode = all(c == c.lower() for c in self.columns)

    # -------------------------------------------------------------------------
    def _resolve_col(self, col: str) -> str:
        col_clean = col.strip().replace('"', '')
        for c in self.columns:
            if c.lower() == col_clean.lower():
                return c
        return col_clean

    def _quote(self, col: str) -> str:
        """Decide si agregar comillas según el modo."""
        return f'"{col}"' if not self.lowercase_mode else col.lower()

    def _resolve_agg(self, expr: str) -> str:
        inner = re.search(r"\(\s*([^)]+)\s*\)", expr)
        func = expr.split("(")[0].upper()
        col = inner.group(1).strip() if inner else ""
        if col == "*":
            return f"{func}(*)"
        if col.lower().startswith("distinct "):
            inner_col = col[9:].strip()
            match = self._resolve_col(inner_col)
            return f'{func}(DISTINCT {self._quote(match)})'
        match = self._resolve_col(col)
        return f'{func}({self._quote(match)})'

    # -------------------------------------------------------------------------
    def build_sql(self, mini: str) -> str:
        raw = mini.strip()
        low = raw.lower()

        m_sel = re.search(r"select\s+(.+?)(\s+where|\s+group by|\s+order by|\s+limit|$)", low, flags=re.S)
        select_txt = m_sel.group(1).strip() if m_sel else "*"

        m_where = re.search(r"where\s+(.+?)(\s+group by|\s+order by|\s+limit|$)", low, flags=re.S)
        where_section = m_where.group(1).strip() if m_where else ""

        m_gb = re.search(r"group\s+by\s+(.+?)(\s+order by|\s+limit|$)", low, flags=re.S)
        group_by = self._resolve_col(m_gb.group(1).strip()) if m_gb else None

        # SELECT
        select_cols = []
        aggs = re.findall(r"(sum|avg|max|min|count)\s*\(\s*([^)]+)\s*\)", select_txt, flags=re.I)
        if aggs:
            for fn, col in aggs:
                select_cols.append(self._resolve_agg(f"{fn}({col})"))
        elif select_txt != "*":
            cols = [c.strip() for c in re.split(r"\s*,\s*", select_txt)]
            select_cols = [self._quote(self._resolve_col(c)) for c in cols]
        else:
            select_cols = ["*"]

        if group_by and self._quote(group_by) not in select_cols:
            select_cols.insert(0, self._quote(group_by))

        sql = f"SELECT {', '.join(select_cols)} FROM {self._quote(self.table)}"

        # WHERE
        where_clauses = []
        if where_section:
            if "between" in where_section:
                a, b = re.findall(r"'([\d\-]+)'", where_section)
                where_clauses.append(f"{self._quote('date')} BETWEEN DATE '{a}' AND DATE '{b}'")
            else:
                if "date" in where_section:
                    m_date = re.search(r"date\s*>=\s*'([\d\-]+)'", where_section)
                    if m_date:
                        where_clauses.append(f"{self._quote('date')} >= DATE '{m_date.group(1)}'")
                    m_date2 = re.search(r"date\s*<\s*'([\d\-]+)'", where_section)
                    if m_date2:
                        where_clauses.append(f"{self._quote('date')} < DATE '{m_date2.group(1)}'")
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        # GROUP BY
        if group_by:
            sql += f" GROUP BY {self._quote(group_by)}"

        # ORDER BY
        m_ob = re.search(r"order\s+by\s+([a-zA-Z0-9_\(\)]+)(\s+asc|\s+desc)?", low)
        if m_ob:
            ob_key = m_ob.group(1).strip()
            ob_dir = " DESC" if m_ob.group(2) and "desc" in m_ob.group(2).lower() else " ASC"
            sql += f" ORDER BY {self._quote(ob_key)}{ob_dir}"

        m_lim = re.search(r"limit\s+(\d+)", low)
        if m_lim:
            sql += f" LIMIT {int(m_lim.group(1))}"

        return sql + ";"

    # -------------------------------------------------------------------------
    def run_sql(self, mini: str) -> pd.DataFrame:
        """Ejecuta SQL traducida desde mini-sintaxis y devuelve DataFrame."""
        try:
            sql = self.build_sql(mini)
            with self.engine.begin() as conn:
                df = pd.read_sql(text(sql), con=conn)
            return df
        except Exception as e:
            return pd.DataFrame({"error": [str(e)], "sql": [mini]})

    # -------------------------------------------------------------------------
    @staticmethod
    def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [c.lower().strip() for c in df.columns]
        return df


def get_table_schema(table_name: str):
    DB = os.getenv("POSTGRES_DB")
    USER = os.getenv("POSTGRES_USER")
    PWD = os.getenv("POSTGRES_PASSWORD")
    HOST = os.getenv("POSTGRES_HOST", "db")
    PORT = os.getenv("POSTGRES_PORT", "5432")
    engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
    query = text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = :t
        ORDER BY ordinal_position;
    """)
    with engine.begin() as conn:
        rows = conn.execute(query, {"t": table_name}).fetchall()
    return [{"column": r[0], "type": r[1]} for r in rows]
