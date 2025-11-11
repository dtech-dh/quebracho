import json, os, logging
from openai import OpenAI  # type: ignore
from dotenv import load_dotenv
import pandas as pd
from mcp_postgres import PostgresMCP, get_table_schema
from context_loader import load_business_context, get_context_text  # 🧠 Contexto empresarial

load_dotenv()
logging.basicConfig(level=logging.INFO)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

pg = PostgresMCP()

# =====================================================
# 🧠 CONTEXTO EMPRESARIAL GLOBAL
# =====================================================
BUSINESS_CONTEXT = load_business_context()
if BUSINESS_CONTEXT:
    logging.info("📚 Contexto empresarial cargado correctamente en analyzer_ai.")
else:
    logging.warning("⚠️ No se pudo cargar contexto empresarial.")


# =====================================================
# 🔍 FUNCIÓN PRINCIPAL
# =====================================================
async def analyze_query(prompt: str, context: list = None):
    """Analiza la pregunta del usuario combinando memoria conversacional + contexto empresarial."""
    schema = get_table_schema(pg.table)
    schema_text = json.dumps(schema, ensure_ascii=False, indent=2)
    business_context_text = get_context_text()

    # =====================================================
    # 🧩 Normalización del contexto (previene error 400)
    # =====================================================
    normalized_context = []
    if context:
        for c in context[-10:]:  # solo las últimas 10 para mantener coherencia
            if not isinstance(c, dict):
                continue
            if "prompt" in c:
                normalized_context.append({"role": "user", "content": c["prompt"]})
            if "response" in c and c["response"]:
                normalized_context.append({"role": "assistant", "content": c["response"]})

    # =====================================================
    # 🎯 Construcción del prompt inicial
    # =====================================================
    plan_prompt = f"""
Tenés acceso a una base de datos PostgreSQL con la tabla "{pg.table}" y este esquema:
{schema_text}

También conocés el siguiente contexto empresarial actualizado:
{business_context_text}

Analizá la siguiente pregunta y devolveme SOLO un JSON válido con este formato:
{{
  "action": "query_postgres" | "summary",
  "query": "<consulta SQL si aplica>",
  "need_data": true | false
}}
Reglas:
- Si la pregunta requiere datos, usá "query_postgres".
- Si es conceptual, usá "summary".
- La SQL debe ser válida para Postgres (minúsculas, sin comillas).
- Si no se menciona año, asumí el actual.
- Devuelve siempre un JSON válido y limpio.

Pregunta del usuario:
{prompt}
"""

    # =====================================================
    # 🧠 Construcción del contexto completo (memoria extendida)
    # =====================================================
    messages = [
        {"role": "system", "content": (
            "Sos un analista comercial con memoria extendida y conocimiento del negocio. "
            "Recordá las últimas conversaciones del usuario y respondé de forma coherente, profesional y orientada a ventas."
        )}
    ]
    messages.extend(normalized_context)
    messages.append({"role": "user", "content": plan_prompt})

    # =====================================================
    # 🧠 Primera llamada: generación del plan (JSON)
    # =====================================================
    resp = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        temperature=0
    )

    content = resp.choices[0].message.content.strip()
    try:
        plan = json.loads(content)
    except Exception as e:
        logging.warning(f"⚠️ Error parseando plan JSON: {e}")
        plan = {"action": "summary", "need_data": False}

    # =====================================================
    # 🚀 Ejecución de SQL si aplica
    # =====================================================
    data = None
    if plan.get("need_data") and plan.get("action") == "query_postgres":
        sql = plan.get("query")
        if sql:
            logging.info(f"🚀 Ejecutando SQL validada: {sql}")
            data = pg.run_sql(sql)

    # =====================================================
    # 💬 Segunda llamada: resumen con contexto empresarial
    # =====================================================
    summary_prompt = f"""
Usuario: {prompt}
Acción planificada: {json.dumps(plan, indent=2, ensure_ascii=False)}
Datos disponibles: {data.head(10).to_dict(orient='records') if isinstance(data, pd.DataFrame) else 'Sin datos'}
Recordá el contexto empresarial actual:
{business_context_text}
Resumí la información en lenguaje claro, conciso y con enfoque comercial.
"""

    messages.append({"role": "assistant", "content": content})
    messages.append({"role": "user", "content": summary_prompt})

    summary = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        temperature=0.2
    )

    response_text = summary.choices[0].message.content

    # =====================================================
    # 🧾 Resultado final
    # =====================================================
    return {
        "plan": plan,
        "sql": plan.get("query"),
        "response": response_text,
        "data_preview": (
            data.head(10).to_dict(orient="records")
            if isinstance(data, pd.DataFrame)
            else []
        ),
    }
