import os
import logging
import psycopg2
import pandas as pd

CONTEXT = {}

def load_business_context():
    """Carga datos clave del negocio desde PostgreSQL al iniciar el backend."""
    global CONTEXT
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "db"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

        # === Top 10 clientes por monto total ===
        df_clientes = pd.read_sql("""
            SELECT cliente, SUM(monto) AS total
            FROM ventas
            WHERE fecha >= date_trunc('year', CURRENT_DATE) - interval '1 year'
            GROUP BY cliente
            ORDER BY total DESC
            LIMIT 10;
        """, conn)

        # === Top 10 vendedores ===
        df_vendedores = pd.read_sql("""
            SELECT vendedor, COUNT(*) AS operaciones
            FROM ventas
            WHERE fecha >= date_trunc('year', CURRENT_DATE) - interval '1 year'
            GROUP BY vendedor
            ORDER BY operaciones DESC
            LIMIT 10;
        """, conn)

        # === Última fecha disponible ===
        df_fecha = pd.read_sql("SELECT MAX(fecha) AS ultima_fecha FROM ventas;", conn)
        conn.close()

        CONTEXT = {
            "top_clientes": df_clientes.to_dict(orient="records"),
            "top_vendedores": df_vendedores.to_dict(orient="records"),
            "ultima_fecha": df_fecha["ultima_fecha"][0].strftime("%Y-%m-%d") if not df_fecha.empty else None,
        }

        logging.info("📚 Contexto empresarial cargado correctamente.")
        return CONTEXT

    except Exception as e:
        logging.error(f"❌ Error cargando contexto: {e}")
        CONTEXT = {}
        return CONTEXT


def get_context_text():
    """Devuelve el contexto como texto legible para inyectar al modelo."""
    if not CONTEXT:
        return "No hay contexto empresarial cargado."
    lines = [
        f"📅 Última fecha de datos: {CONTEXT.get('ultima_fecha','N/D')}",
        "",
        "🏆 Top clientes por monto:",
    ]
    for c in CONTEXT.get("top_clientes", []):
        lines.append(f"- {c['cliente']}: {c['total']:.2f}")
    lines.append("")
    lines.append("💼 Mejores vendedores:")
    for v in CONTEXT.get("top_vendedores", []):
        lines.append(f"- {v['vendedor']}: {v['operaciones']} operaciones")
    return "\n".join(lines)
