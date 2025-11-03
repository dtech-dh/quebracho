import os, re
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

class PostgresMCP:
    """
    MCP (Mini Command Processor) para ejecutar consultas simplificadas sobre una tabla de Postgres.
    ✅ Resuelve diferencias de mayúsculas/minúsculas automáticamente.
    ✅ Comilla columnas y alias de forma segura.
    ✅ Compatible con IA, ETL y entornos dev/prod.
    ✅ Permite validar SQL sin ejecutarla (validate_sql).
    """

    def __init__(self):
        DB = os.getenv("POSTGRES_DB")
        USER = os.getenv("POSTGRES_USER")
        PWD = os.getenv("POSTGRES_PASSWORD")
        HOST = os.getenv("POSTGRES_HOST", "db")
        PORT = os.getenv("POSTGRES_PORT", "5432")
        self.table = os.getenv("TABLE_NAME", "ventas")

        self.engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")

        # Cache de columnas con case real
        with self.engine.begin() as conn:
            cols = conn.execute(text(f'SELECT * FROM "{self.table}" LIMIT 0')).keys()
        self.columns = set(cols)

    # -------------------------------------------------------------------------
    def _resolve_col(self, col: str) -> str:
        """Devuelve el nombre exacto de la columna, respetando el case real."""
        col_clean = col.strip().replace('"', '')
        for c in self.columns:
            if c.lower() == col_clean.lower():
                return c
        return col_clean

    def _resolve_agg(self, expr: str) -> str:
        """Convierte expresiones tipo SUM(col) en SUM("Col")."""
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

    # -------------------------------------------------------------------------
    def build_sql(self, mini: str) -> str:
        """Construye SQL a partir de una mini-consulta."""
        raw = mini.strip()
        low = raw.lower()

        # SELECT
        m_sel = re.search(r"select\s+(.+?)(\s+where|\s+group by|\s+order by|\s+limit|$)", low, flags=re.S)
        select_txt = m_sel.group(1).strip() if m_sel else "*"

        # WHERE
        where_section = ""
        m_where = re.search(r"where\s+(.+?)(\s+group by|\s+order by|\s+limit|$)", low, flags=re.S)
        if m_where:
            where_section = m_where.group(1).strip()

        # GROUP BY
        group_by = None
        m_gb = re.search(r"group\s+by\s+(.+?)(\s+order by|\s+limit|$)", low, flags=re.S)
        if m_gb:
            group_expr = m_gb.group(1).strip()
            group_by = self._resolve_col(group_expr)

        # SELECT columns
        select_cols = []
        aggs = re.findall(r"(sum|avg|max|min|count)\s*\(\s*([^)]+)\s*\)", select_txt, flags=re.I)
        if aggs:
            for fn, col in aggs:
                fn_up = fn.upper()
                col_txt = col.strip()
                if col_txt == "*":
                    select_cols.append(f"{fn_up}(*)")
                elif col_txt.lower().startswith("distinct "):
                    col_inner = col_txt[9:].strip()
                    col_real = self._resolve_col(col_inner)
                    select_cols.append(f'{fn_up}(DISTINCT "{col_real}")')
                else:
                    col_real = self._resolve_col(col_txt)
                    select_cols.append(f'{fn_up}("{col_real}")')
        elif select_txt != "*":
            cols = [c.strip() for c in re.split(r"\s*,\s*", select_txt)]
            select_cols = [f'"{self._resolve_col(c)}"' for c in cols]
        else:
            select_cols = ["*"]

        # Si hay GROUP BY, incluir la columna si no está en el SELECT
        if group_by and f'"{group_by}"' not in select_cols:
            select_cols.insert(0, f'"{group_by}"')

        sql = "SELECT " + ", ".join(select_cols) + f' FROM "{self.table}"'

        # WHERE
        where_clauses = []
        if where_section:
            m_year = re.search(r"year\s*=\s*(\d{4})", where_section)
            if m_year:
                where_clauses.append(f'"Year"={int(m_year.group(1))}')
            m_month = re.search(r"month\s*=\s*([0-9]{1,2})", where_section)
            if m_month:
                where_clauses.append(f'"Month"={int(m_month.group(1))}')
            m_date = re.search(r"date\s*=\s*'([\d\-]+)'", where_section)
            if m_date:
                where_clauses.append(f'"Date"::date = DATE \'{m_date.group(1)}\'')
            m_between = re.search(r"between\s*'([\d\-]+)'\s*and\s*'([\d\-]+)'", where_section)
            if m_between:
                a, b = m_between.groups()
                where_clauses.append(f'"Date" BETWEEN DATE \'{a}\' AND DATE \'{b}\'')

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        # GROUP BY
        if group_by:
            sql += f' GROUP BY "{group_by}"'

        # ORDER BY
        m_ob = re.search(r"order\s+by\s+([a-zA-Z0-9_\(\) ]+)(\s+asc|\s+desc)?", low)
        if m_ob:
            ob_key = m_ob.group(1).strip()
            ob_dir = " DESC" if m_ob.group(2) and "desc" in m_ob.group(2).lower() else " ASC"
            if ob_key.lower().startswith(("sum(", "avg(", "max(", "min(", "count(")):
                ob_sql = self._resolve_agg(ob_key)
            else:
                ob_sql = f'"{self._resolve_col(ob_key)}"'
            if group_by in ("Month", "Year", "Day", "Date"):
                ob_sql = f'"{group_by}"'
                ob_dir = " ASC"
            sql += f" ORDER BY {ob_sql}{ob_dir}"
        elif group_by in ("Month", "Year", "Day", "Date"):
            sql += f' ORDER BY "{group_by}" ASC'

        # LIMIT
        m_lim = re.search(r"limit\s+(\d+)", low)
        if m_lim:
            sql += f" LIMIT {int(m_lim.group(1))}"

        return sql + ";"

    # -------------------------------------------------------------------------
    def validate_sql(self, sql: str) -> dict:
        """
        Valida la consulta SQL usando EXPLAIN (no ejecuta realmente).
        Devuelve un dict con 'valid' y 'plan' o 'error'.
        """
        try:
            with self.engine.begin() as conn:
                plan = conn.execute(text(f"EXPLAIN {sql}")).fetchall()
            return {"valid": True, "plan": [r[0] for r in plan]}
        except Exception as e:
            return {"valid": False, "error": str(e)}

    # -------------------------------------------------------------------------
    def run_sql(self, mini: str) -> pd.DataFrame:
        """
        Ejecuta SQL traducida y corrige automáticamente alias y columnas no comilladas.
        """
        try:
            sql = self.build_sql(mini)

            # 🔧 Auto-corrección de nombres de columnas (case-insensitive)
            for col in self.columns:
                col_lower = col.lower()
                pattern = fr'\b{col_lower}\b'
                if re.search(pattern, sql, flags=re.I) and f'"{col}"' not in sql:
                    sql = re.sub(pattern, f'"{col}"', sql, flags=re.I)

            # 🔧 Corrige alias en ORDER BY (ej: total_sales)
            sql = re.sub(r'order by\s+([a-zA-Z_][a-zA-Z0-9_]*)',
                         r'ORDER BY "\1"', sql, flags=re.I)

            # 🧠 Validación previa (no bloquea si falla)
            validation = self.validate_sql(sql)
            if not validation["valid"]:
                return pd.DataFrame({"error": [validation["error"]], "sql": [sql]})

            with self.engine.begin() as conn:
                df = pd.read_sql(text(sql), con=conn)

            return df

        except Exception as e:
            return pd.DataFrame({"error": [str(e)], "sql": [mini]})

    # -------------------------------------------------------------------------
    @staticmethod
    def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convierte nombres de columnas a minúsculas antes de guardar en la DB.
        Ideal para ETL: mantiene consistencia entre entornos.
        """
        df.columns = [c.lower().strip() for c in df.columns]
        return df


# -------------------------------------------------------------------------
def get_table_schema(table_name: str):
    """Obtiene el esquema de una tabla en formato JSON serializable."""
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
