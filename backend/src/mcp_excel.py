import re
import pandas as pd
from datetime import datetime, timedelta

class ExcelMCP:
    """
    Ejecuta un "mini-SQL" sobre un DataFrame (derivado de un Excel).
    Soporta:
      - SELECT SUM(col) | AVG(col) | MAX(col) | MIN(col) | col1[, col2]
      - WHERE Year=YYYY [AND Month=M] [AND Date='YYYY-MM-DD']
      - WHERE Date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
      - GROUP BY Year | Month | col
      - ORDER BY <col|SUM(col)|AVG(col)|MAX(col)|MIN(col)> [ASC|DESC]
      - LIMIT N
    """

    def __init__(self, file_path: str):
        self.df = pd.read_excel(file_path, sheet_name=0)

        # Detectar columna de fecha
        self.date_col = None
        for col in self.df.columns:
            if "date" in col.lower() or "fecha" in col.lower():
                self.date_col = col
                break
        if self.date_col:
            self.df[self.date_col] = pd.to_datetime(self.df[self.date_col], errors="coerce")
            self.df["Year"] = self.df[self.date_col].dt.year
            self.df["Month"] = self.df[self.date_col].dt.month
            self.df["Day"] = self.df[self.date_col].dt.date

        # Mapa case-insensitive de columnas
        self._col_ci = {c.lower(): c for c in self.df.columns}

    def _resolve_col(self, name: str) -> str:
        """Resolver nombre de columna sin importar mayúsculas/minúsculas."""
        return self._col_ci.get(name.strip().lower(), name.strip())

    def run_sql(self, sql: str) -> pd.DataFrame:
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("Consulta vacía")

        df = self.df.copy()
        raw = sql.strip().lower()

        # --- WHERE ---
        year_m = re.search(r"year\s*=\s*(\d{4})", raw)
        month_m = re.search(r"month\s*=\s*([0-9]{1,2})", raw)
        date_eq_m = re.search(r"date\s*=\s*'([\d\-]+)'", raw)
        between_m = re.search(r"between\s*'([\d\-]+)'\s*and\s*'([\d\-]+)'", raw)

        if year_m and "Year" in df:
            df = df[df["Year"] == int(year_m.group(1))]
        if month_m and "Month" in df:
            df = df[df["Month"] == int(month_m.group(1))]
        if date_eq_m and self.date_col:
            d = pd.to_datetime(date_eq_m.group(1), errors="coerce")
            df = df[df[self.date_col].dt.date == d.date()]
        if between_m and self.date_col:
            start, end = pd.to_datetime(between_m.group(1)), pd.to_datetime(between_m.group(2))
            df = df[(df[self.date_col] >= start) & (df[self.date_col] <= end)]

        # --- SELECT ---
        sum_m = re.search(r"sum\(\s*([^)]+)\s*\)", raw)
        avg_m = re.search(r"avg\(\s*([^)]+)\s*\)", raw)
        max_m = re.search(r"max\(\s*([^)]+)\s*\)", raw)
        min_m = re.search(r"min\(\s*([^)]+)\s*\)", raw)

        select_cols, agg, agg_col = None, None, None
        if sum_m:
            agg, agg_col = "sum", self._resolve_col(sum_m.group(1))
        elif avg_m:
            agg, agg_col = "avg", self._resolve_col(avg_m.group(1))
        elif max_m:
            agg, agg_col = "max", self._resolve_col(max_m.group(1))
        elif min_m:
            agg, agg_col = "min", self._resolve_col(min_m.group(1))
        else:
            m = re.search(r"select\s+(.+?)\s+(where|group by|order by|limit|$)", raw, flags=re.S)
            if m:
                cols_part = m.group(1).strip()
                if cols_part.lower() != "*":
                    select_cols = [self._resolve_col(c) for c in cols_part.split(",")]

        # --- GROUP BY ---
        group_m = re.search(r"group\s+by\s+([a-zA-Z0-9_\- ]+)", raw)
        group_col = None
        if group_m:
            group_col = self._resolve_col(group_m.group(1))

        # --- Build result ---
        if agg and group_col:
            func = getattr(df.groupby(group_col, dropna=False)[agg_col], agg)
            result = func().reset_index()
            result.rename(columns={agg_col: f"{agg.upper()}({agg_col})"}, inplace=True)
        elif agg and not group_col:
            if agg in ("sum", "avg"):
                func = getattr(pd.to_numeric(df[agg_col], errors="coerce"), agg)
                result = pd.DataFrame({f"{agg.upper()}({agg_col})": [func()]})
            elif agg == "max":
                result = pd.DataFrame({f"MAX({agg_col})": [df[agg_col].max()]})
            elif agg == "min":
                result = pd.DataFrame({f"MIN({agg_col})": [df[agg_col].min()]})
        elif select_cols:
            keep = [c for c in select_cols if c in df.columns]
            if not keep:
                raise ValueError("Columnas de SELECT no válidas")
            result = df[keep]
        else:
            raise ValueError("No pude interpretar la consulta")

        # --- ORDER BY ---
        order_m = re.search(r"order\s+by\s+([a-zA-Z0-9_\(\) ]+)(\s+asc|\s+desc)?", raw)
        if order_m:
            order_key = self._resolve_col(order_m.group(1))
            ascending = not (order_m.group(2) and "desc" in order_m.group(2))

            # Si la query agrupa por Month o Year → forzar orden cronológico
            if group_col in ("Month", "Year"):
                order_key = group_col

            # Si piden ORDER BY SUM/AVG/MAX/MIN → usamos la columna calculada
            if order_key.lower().startswith(("sum(", "avg(", "max(", "min(")):
                inner = re.search(r"\(\s*([^)]+)\s*\)", order_key).group(1)
                func = order_key.split("(")[0].upper()
                order_key = f"{func}({self._resolve_col(inner)})"

            if order_key in result.columns:
                result = result.sort_values(order_key, ascending=ascending)
        else:
            # Si no hay ORDER BY pero hay GROUP BY Month o Year → ordenar cronológico
            if group_col in ("Month", "Year") and group_col in result.columns:
                result = result.sort_values(group_col, ascending=True)

        # --- LIMIT ---
        limit_m = re.search(r"limit\s+(\d+)", raw)
        if limit_m:
            result = result.head(int(limit_m.group(1)))

        return result
