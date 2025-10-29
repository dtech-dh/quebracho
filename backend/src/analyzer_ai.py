import json, os, logging
from openai import OpenAI #type: ignore
from dotenv import load_dotenv
from mcp_postgres import get_table_schema, PostgresMCP

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

pg = PostgresMCP()

# ============================================
# 🧠 ANALIZADOR PRINCIPAL
# ============================================
async def analyze_query(prompt: str):
    """
    Interpreta la pregunta natural, genera un plan (JSON) y ejecuta SQL real si corresponde.
    """

    # --- Obtener esquema real desde Postgres
    schema = get_table_schema(pg.table)
    schema_text = json.dumps(schema, ensure_ascii=False)

    # --- Construcción del prompt mejorado
    plan_prompt = f"""
Sos un asistente comercial con acceso a una base PostgreSQL llamada "{pg.table}".
El esquema de la tabla es:
{schema_text}

Tu tarea es analizar la siguiente pregunta y devolver SOLO un JSON válido con este formato:
{{
  "action": "query_postgres" | "summary",
  "query": "<consulta SQL en formato MCP si aplica>",
  "need_data": true | false
}}

Reglas:
- Si la pregunta requiere datos o cálculos, usá "query_postgres" y "need_data": true.
- Si es conceptual o general, usá "summary" y "need_data": false.
- La consulta SQL debe ser simple y válida, por ejemplo:
  "SELECT SUM(Amount) WHERE Year=2025 AND Month=9;"
- Si no se menciona un año, asumí el actual.
- Siempre devolvé un JSON perfectamente formateado y válido.

Pregunta del usuario:
{prompt}
"""

    logging.info(f"🧩 Prompt enviado al modelo:\n{plan_prompt}")

    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": plan_prompt}],
            temperature=0
        )
        content = resp.choices[0].message.content.strip()
        logging.info(f"🧠 Respuesta cruda del modelo: {content}")
    except Exception as e:
        logging.error(f"❌ Error al invocar OpenAI: {e}")
        return {"error": str(e), "sql": None, "response": "Error en análisis del plan."}

    # --- Parsear JSON devuelto
    try:
        plan = json.loads(content)
    except Exception:
        logging.warning("⚠️ No se pudo parsear JSON, creando plan por defecto.")
        plan = {"action": "query_postgres", "need_data": True, "query": None}

    sql = plan.get("query")

    # --- 🔧 Si no hay SQL o need_data=False, forzar generación de SQL
    if not sql or not plan.get("need_data", True):
        logging.info("⚙️ Forzando generación de SQL por falta de plan válido...")
        cols = ", ".join([c["column"] for c in schema])
        sql_prompt = f"""
Convertí la siguiente pregunta en una mini consulta SQL para PostgreSQL (tabla "{pg.table}") usando las columnas [{cols}].
Ejemplo de formato: SELECT SUM(Amount) WHERE Year=2025 AND Month=9;
Pregunta: {prompt}
"""
        try:
            sql_resp = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "system", "content": "Traductor de lenguaje natural a SQL simplificada"},
                          {"role": "user", "content": sql_prompt}],
                temperature=0
            )
            sql = sql_resp.choices[0].message.content.strip().splitlines()[0]
            plan["query"] = sql
            plan["action"] = "query_postgres"
            plan["need_data"] = True
        except Exception as e:
            logging.error(f"❌ Error al intentar generar SQL forzada: {e}")
            sql = None

    # --- Ejecutar SQL si existe
    data = None
    if sql:
        try:
            logging.info(f"🚀 Ejecutando SQL: {sql}")
            data = pg.run_sql(sql)
        except Exception as e:
            logging.error(f"❌ Error al ejecutar SQL: {e}")
            data = None

    # --- Generar resumen comercial con IA
    summary_prompt = f"""
Usuario: {prompt}
Acción planificada: {json.dumps(plan, indent=2, ensure_ascii=False)}
Datos disponibles: {data.head(10).to_dict(orient='records') if data is not None and hasattr(data, 'head') else 'Sin datos o error.'}

Resumí en lenguaje comercial claro, destacando hallazgos relevantes y contexto de negocio.
"""

    try:
        summary = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": summary_prompt}],
            temperature=0.2
        )
        response_text = summary.choices[0].message.content.strip()
    except Exception as e:
        response_text = f"Error al generar resumen: {e}"

    # --- Respuesta final
    return {
        "plan": plan,
        "sql": sql,
        "response": response_text,
        "data_preview": data.head(10).to_dict(orient="records") if data is not None and hasattr(data, "head") else []
    }
