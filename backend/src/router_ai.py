import json, os
from openai import OpenAI
from dotenv import load_dotenv
from mcp_postgres import get_table_schema, PostgresMCP

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

pg = PostgresMCP()

async def analyze_query(prompt: str):
    schema = get_table_schema(pg.table)
    schema_text = json.dumps(schema, ensure_ascii=False)

    plan_prompt = f"""
    Tenés acceso a una tabla con el siguiente esquema:
    {schema_text}

    Analizá la siguiente pregunta y devolveme SOLO un JSON válido con:
    {{
      "action": "query_postgres" | "summary",
      "query": "<consulta SQL si aplica>",
      "need_data": true | false
    }}

    Pregunta del usuario:
    {prompt}
    """

    resp = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": plan_prompt}],
        temperature=0
    )
    content = resp.choices[0].message.content.strip()

    try:
        plan = json.loads(content)
    except:
        plan = {"action": "summary", "need_data": False}

    data = None
    if plan.get("need_data") and plan.get("action") == "query_postgres":
        data = pg.run_sql(plan["query"])

    summary_prompt = f"""
    Usuario: {prompt}
    Acción planificada: {json.dumps(plan, indent=2, ensure_ascii=False)}
    Datos disponibles: {data.head(10).to_dict(orient='records') if hasattr(data, 'head') else 'Sin datos o error.'}

    Resumí y explicá en lenguaje claro, con énfasis comercial.
    """

    summary = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": summary_prompt}],
        temperature=0
    )

    return {
        "plan": plan,
        "sql": plan.get("query"),
        "response": summary.choices[0].message.content,
        "data_preview": data.head(10).to_dict(orient="records") if hasattr(data, "head") else []
    }
