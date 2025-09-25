import os, re
import pandas as pd
from sqlalchemy import create_engine, text
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

        # Cache columnas (respeta mayúsculas/minúsculas reales en Postgres)
        with self.engine.begin() as conn:
            cols = conn.execute(text(f'SELECT * FROM "{self.table}" LIMIT 0')).keys()
        self.columns = set(cols)

    def _resolve_col(self, col: str) -> str:
        """Resolver nombre de columna respetando case real de Postgres."""
        for c in self.columns:
            if c.lower() == col.strip().lower():
                return c
        return col.strip()

    def _resolve_agg(self, expr: str) -> str:
        """SUM(Amount) -> SUM("Amount") con case correcto."""
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
        raw = mini.strip()
        low = raw.lower()

        # --- SELECT (texto entre SELECT y la próxima cláusula) ---
        m_sel = re.search(r"select\s+(.+?)(\s+where|\s+group by|\s+order by|\s+limit|$)", low, flags=re.S)
        select_txt = m_sel.group(1).strip() if m_sel else "*"

        # --- WHERE (sección segura, cortada antes de group/order/limit) ---
        where_section = ""
        m_where = re.search(r"where\s+(.+?)(\s+group by|\s+order by|\s+limit|$)", low, flags=re.S)
        if m_where:
            where_section = m_where.group(1).strip()

        # --- GROUP BY (texto cortado antes de order/limit) ---
        group_by = None
        m_gb = re.search(r"group\s+by\s+(.+?)(\s+order by|\s+limit|$)", low, flags=re.S)
        if m_gb:
            group_expr = m_gb.group(1).strip()
            group_by = self._resolve_col(group_expr)

        # --- Construir lista de columnas del SELECT ---
        select_cols = []
        if select_txt.lower().startswith("distinct "):
            col = select_txt[9:].strip()
            col_real = self._resolve_col(col)
            select_cols = [f'DISTINCT "{col_real}"']
        else:
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

        # Si hay GROUP BY, incluir la columna de agrupación en el SELECT si no está
        if group_by and f'"{group_by}"' not in select_cols and not select_txt.lower().startswith("distinct "):
            select_cols.insert(0, f'"{group_by}"')

        # --- Armar FROM base ---
        sql = "SELECT " + ", ".join(select_cols) + f' FROM "{self.table}"'

        # --- WHERE (construcción de condiciones) ---
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
                where_clauses.append(f'"Date"=DATE \'{m_date.group(1)}\'')

            m_between = re.search(r"between\s*'([\d\-]+)'\s*and\s*'([\d\-]+)'", where_section)
            if m_between:
                a, b = m_between.groups()
                where_clauses.append(f'"Date" BETWEEN DATE \'{a}\' AND DATE \'{b}\'')

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        # --- GROUP BY (después del WHERE, como corresponde) ---
        if group_by:
            sql += f' GROUP BY "{group_by}"'

        # --- ORDER BY ---
        m_ob = re.search(r"order\s+by\s+([a-zA-Z0-9_\(\) ]+)(\s+asc|\s+desc)?", low)
        if m_ob:
            ob_key = m_ob.group(1).strip()
            ob_dir = " DESC" if m_ob.group(2) and "desc" in m_ob.group(2).lower() else " ASC"

            if ob_key.lower().startswith(("sum(", "avg(", "max(", "min(", "count(")):
                ob_sql = self._resolve_agg(ob_key)
            else:
                ob_sql = f'"{self._resolve_col(ob_key)}"'

            # Si hay agrupación temporal → imponemos orden cronológico
            if group_by in ("Month", "Year", "Day"):
                ob_sql = f'"{group_by}"'
                ob_dir = " ASC"

            sql += f" ORDER BY {ob_sql}{ob_dir}"
        else:
            # Si no hay ORDER BY pero hay GROUP BY temporal → orden cronológico por defecto
            if group_by in ("Month", "Year", "Day"):
                sql += f' ORDER BY "{group_by}" ASC'

        # --- LIMIT ---
        m_lim = re.search(r"limit\s+(\d+)", low)
        if m_lim:
            sql += f" LIMIT {int(m_lim.group(1))}"

        return sql + ";"

    def run_sql(self, mini: str) -> pd.DataFrame:
        sql = self.build_sql(mini)
        with self.engine.begin() as conn:
            return pd.read_sql(text(sql), con=conn)
