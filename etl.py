import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB=os.getenv("POSTGRES_DB")
USER=os.getenv("POSTGRES_USER")
PWD=os.getenv("POSTGRES_PASSWORD")
HOST=os.getenv("POSTGRES_HOST","db")
PORT=os.getenv("POSTGRES_PORT","5432")
EXCEL=os.getenv("EXCEL_PATH","/app/data.xlsx")
TABLE=os.getenv("TABLE_NAME","ventas")

DATE_COL=os.getenv("DATE_COL") or ""
AMOUNT_COL=os.getenv("AMOUNT_COL") or ""
CLIENTE_COL=os.getenv("CLIENTE_COL") or ""
SUCURSAL_COL=os.getenv("SUCURSAL_COL") or ""
CIUDAD_COL=os.getenv("CIUDAD_COL") or ""

url = f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}"
engine = create_engine(url)

print(f"[ETL] Leyendo Excel: {EXCEL}")
df = pd.read_excel(EXCEL, sheet_name=0)

# Autodetectar columnas si no vienen por env
def find_col(candidates):
    for c in df.columns:
        lc = str(c).lower()
        for cand in candidates:
            if cand in lc:
                return c
    return None

date_col = DATE_COL or find_col(["date","fecha","fech"])
amount_col = AMOUNT_COL or find_col(["amount","monto","importe","total"])

if not date_col or not amount_col:
    raise RuntimeError(f"No se detectó columna de fecha o de monto. date_col={date_col}, amount_col={amount_col}")

# Normalizar tipos
df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
df = df.dropna(subset=[date_col])
df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)

# Enriquecimiento temporal
df["Year"]  = df[date_col].dt.year
df["Month"] = df[date_col].dt.month
df["Day"]   = df[date_col].dt.date

# Renombrados amigables (opcional)
std_cols = {
    date_col: "Date",
    amount_col: "Amount",
}
df = df.rename(columns=std_cols)

# Asegurar columnas opcionales (si existen en XLS)
for opt_env, std_name in [(CLIENTE_COL,"Cliente"),(SUCURSAL_COL,"Sucursal"),(CIUDAD_COL,"Ciudad")]:
    if opt_env and opt_env in df.columns and std_name not in df.columns:
        df.rename(columns={opt_env: std_name}, inplace=True)

print(f"[ETL] Columnas finales: {list(df.columns)}")

with engine.begin() as conn:
    # Crear tabla con esquema simple (drop & create para prototipo)
    conn.execute(text(f'DROP TABLE IF EXISTS "{TABLE}"'))
    # dtype básico por inferencia
    df.head(0).to_sql(TABLE, con=conn, index=False)
    # Insert masivo
    df.to_sql(TABLE, con=conn, if_exists="append", index=False)
    # Índices útiles
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{TABLE}_date ON "{TABLE}" ("Date")'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{TABLE}_year_month ON "{TABLE}" ("Year","Month")'))

print("[ETL] Carga completada ✅")
