import os, re
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from mcp_postgres import PostgresMCP

load_dotenv()

API_KEY=os.getenv("OPENAI_API_KEY")
MODEL=os.getenv("OPENAI_MODEL","gpt-4o-mini")
if not API_KEY: raise RuntimeError("Falta OPENAI_API_KEY en .env")

client = OpenAI(api_key=API_KEY)
mcp = PostgresMCP()

st.set_page_config(page_title="MCP + Postgres (demo)", page_icon="🗄️", layout="centered")
st.title("💬 Chat con Postgres vía MCP (demo)")

with st.expander("Info"):
    st.write(f"Tabla: **{os.getenv('TABLE_NAME','ventas')}**")
    st.write("Columnas detectadas:", sorted(list(mcp.columns)))

if "messages" not in st.session_state:
    st.session_state["messages"] = []

for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

def to_mini_sql(nl: str) -> str:
    sys = "Sos un traductor de lenguaje natural a una consulta SQL simplificada. Devolvé SOLO la consulta (una línea)."
    cols = ", ".join(sorted(list(mcp.columns)))
    usr = f"""
Pregunta: "{nl}"

Generá una consulta en el dialecto:
- SELECT SUM(col)|AVG(col)|MAX(col)|MIN(col) | col1[, col2]
- WHERE Year=YYYY [AND Month=M] [AND Date='YYYY-MM-DD'] [AND Date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD']
- GROUP BY Year|Month|col
- ORDER BY <col|SUM(col)|...> [ASC|DESC]
- LIMIT N

Columnas disponibles: {cols}

Ejemplos:
- "evolución este año" -> SELECT SUM(Amount) WHERE Year=YYYY GROUP BY Month ORDER BY Month ASC
- "comparativa anual" -> SELECT Year, SUM(Amount) GROUP BY Year ORDER BY Year ASC
- "ayer" -> SELECT SUM(Amount) WHERE Date='YYYY-MM-DD'
Reemplazá YYYY/MM por el año/mes actual del sistema cuando digan "este año" / "este mes" / "ayer".
"""
    from datetime import datetime, timedelta
    now = datetime.now()
    usr = usr.replace("YYYY-MM-DD", (now - timedelta(days=1)).strftime("%Y-%m-%d")) \
             .replace("YYYY", str(now.year)) \
             .replace("MM", str(now.month))

    resp = client.chat.completions.create(model=MODEL, messages=[
        {"role":"system","content":sys},
        {"role":"user","content":usr}
    ], temperature=0)
    txt = resp.choices[0].message.content.strip()
    code = re.search(r"```sql(.*?)```", txt, flags=re.S|re.I)
    return code.group(1).strip() if code else txt.splitlines()[0].strip()

if prompt := st.chat_input("Preguntá algo de tus ventas..."):
    st.session_state["messages"].append({"role":"user","content":prompt})
    with st.chat_message("user"): st.markdown(prompt)

    try:
        mini = to_mini_sql(prompt)
        df = mcp.run_sql(mini)
        body = f"**Consulta generada:**\n```sql\n{mini}\n```\n"
        body += f"**Resultado:**\n```\n{df.to_string(index=False)}\n```" if not df.empty else "Sin resultados."
    except Exception as e:
        body = f"**Error:** {e}"

    st.session_state["messages"].append({"role":"assistant","content":body})
    with st.chat_message("assistant"): st.markdown(body)
