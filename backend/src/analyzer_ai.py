import json, os, logging
from openai import OpenAI  # type: ignore
from dotenv import load_dotenv
import pandas as pd
from mcp_postgres import PostgresMCP, get_table_schema

load_dotenv()
logging.basicConfig(level=logging.INFO)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

pg = PostgresMCP()

async def analyze_query(prompt: str, context: list = None):
    """
    Analiza la pregunta del usuario y genera un plan (JSON con SQL o resumen),
    utilizando contexto conversacional del usuario.
    """
    schema = get_table_schema(pg.table)
    schema_text = json.dumps(schema, ensure_ascii=False, indent=2)

    # =====================================================
    # 🧩 Normalización del contexto (evita error 400)
    # =====================================================
    normalized_context = []
    if context:
        for c in context:
            if not isinstance(c, dict):
                continue
            if "prompt" in c:
                normalized_context.append({"role": "user", "content": c["prompt"]})
            if "response" in c and c["response"]:
                normalized_context.append({"role": "assistant", "content": c["response"]})

    # =====================================================
    # 🎯 Construcción del prompt para planificación
    # =====================================================
    plan_prompt = f"""
Tenés acceso a una tabla de PostgreSQL llamada "{pg.table}" con el siguiente esquema:
{schema_text}

Analizá la siguiente pregunta y devolveme SOLO un JSON válido con este formato:
{{
  "action": "query_postgres" | "summary",
  "query": "<consulta SQL si aplica>",
  "need_data": true | false
}}

Reglas:
- Si la pregunta requiere datos o cálculos, usá "query_postgres" y "need_data": true.
- Si es conceptual o general, usá "summary" y "need_data": false.
- La consulta SQL debe ser válida para Postgres (minúsculas, sin comillas).
- Si no se menciona el año, asumí el actual.
- Siempre devolvé un JSON válido y limpio.

Pregunta del usuario:
{prompt}
"""

    messages = [{"role": "system", "content": "Sos un analista comercial con memoria de contexto por usuario."}]
    messages.extend(normalized_context)
    messages.append({"role": "user", "content": plan_prompt})

    # =====================================================
    # 🧠 Primera llamada: generar plan de acción
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
    # 🚀 Ejecutar SQL si aplica
    # =====================================================
    data = None
    if plan.get("need_data") and plan.get("action") == "query_postgres":
        sql = plan.get("query")
        if sql:
            logging.info(f"🚀 Ejecutando SQL validada: {sql}")
            data = pg.run_sql(sql)

    # =====================================================
    # 🧩 Generar resumen final en lenguaje comercial
    # =====================================================
    summary_prompt = f"""
Usuario: {prompt}
Acción planificada: {json.dumps(plan, indent=2, ensure_ascii=False)}
Datos disponibles: {data.head(10).to_dict(orient='records') if isinstance(data, pd.DataFrame) else 'Sin datos'}

Resumí la información en lenguaje claro, breve y con énfasis comercial.
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
    # 🧾 Resultado final estructurado
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
