import os
import pytest   #type: ignore
import pandas as pd
from sqlalchemy import create_engine, text #type: ignore
from dotenv import load_dotenv
from mcp_postgres import PostgresMCP

# === Cargar variables de entorno ===
load_dotenv()

# === Configuración ===
DB = os.getenv("POSTGRES_DB")
USER = os.getenv("POSTGRES_USER")
PWD = os.getenv("POSTGRES_PASSWORD")
HOST = os.getenv("POSTGRES_HOST", "db")
PORT = os.getenv("POSTGRES_PORT", "5432")
TABLE = os.getenv("TABLE_NAME", "ventas")

# -----------------------------------------------------------------------------
# FIXTURES
# -----------------------------------------------------------------------------
@pytest.fixture(scope="module")
def engine():
    """Conexión SQLAlchemy a Postgres"""
    engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
    yield engine
    engine.dispose()


@pytest.fixture(scope="module")
def mcp():
    """Instancia del Mini Command Processor"""
    return PostgresMCP()


# -----------------------------------------------------------------------------
# TESTS DIRECTOS DE BASE DE DATOS
# -----------------------------------------------------------------------------
def test_conexion_postgres(engine):
    """Verifica conexión básica a la base"""
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
        ok = True
    except Exception as e:
        print("❌ Error al conectar:", e)
        ok = False
    assert ok, "No se pudo conectar a PostgreSQL"


def test_tabla_ventas_existe(engine):
    """Confirma que la tabla de ventas exista"""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_name=:t;
    """
    with engine.begin() as conn:
        result = conn.execute(text(query), {"t": TABLE}).fetchone()
    assert result is not None, f"La tabla '{TABLE}' no existe en la base"


def test_tabla_ventas_tiene_datos(engine):
    """Asegura que la tabla tiene registros"""
    with engine.begin() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{TABLE}";')).scalar()
    print(f"📊 Cantidad de registros en {TABLE}: {result}")
    assert result > 0, f"La tabla '{TABLE}' está vacía"


def test_agregado_basico(engine):
    """Verifica que se pueda hacer un agregado temporal"""
    query = text(f'SELECT DATE_TRUNC(\'month\', "Date") AS mes, SUM("Amount") AS total '
                 f'FROM "{TABLE}" GROUP BY mes ORDER BY mes;')
    with engine.begin() as conn:
        df = pd.read_sql(query, con=conn)
    print(df.head())
    assert not df.empty, "No se devolvieron resultados del agregado mensual"


# -----------------------------------------------------------------------------
# TESTS DEL MCP
# -----------------------------------------------------------------------------
def test_mcp_suma_anual(mcp):
    """Valida que el MCP pueda generar un agregado por mes"""
    df = mcp.run_sql("SELECT SUM(Amount) WHERE Year=2025 GROUP BY Month ORDER BY Month ASC")
    print(df.head())
    assert not df.empty, "El MCP no devolvió datos para el agregado mensual"


def test_mcp_filtro_fecha(mcp):
    """Chequea que el MCP pueda filtrar por rango de fechas"""
    df = mcp.run_sql("SELECT SUM(Amount) WHERE Date BETWEEN '2025-01-01' AND '2025-03-31'")
    print(df.head())
    assert not df.empty, "El MCP no devolvió datos en el rango de fechas esperado"


def test_mcp_agrupado_salesrep(mcp):
    """Verifica agrupación por vendedor"""
    df = mcp.run_sql("SELECT SalesRep, SUM(Amount) GROUP BY SalesRep ORDER BY SUM(Amount) DESC LIMIT 5")
    print(df.head())
    assert not df.empty, "El MCP no devolvió resultados agrupados por SalesRep"


def test_mcp_falla_segura(mcp):
    """Valida que MCP maneje errores SQL correctamente"""
    df = mcp.run_sql("SELECT SUM(Amounnt) WHERE YeaR=2025")  # mal escrito a propósito
    if "error" in df.columns:
        print("⚠️ Error controlado:", df.loc[0, "error"])
        assert True
    else:
        pytest.fail("El MCP no detectó el error de columna inválida")
