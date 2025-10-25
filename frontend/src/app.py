import streamlit as st #type: ignore
import requests
import os

API_URL = os.getenv("API_URL", "http://api:8000")

st.set_page_config(page_title="MCP Chat", page_icon="💬", layout="centered")
st.title("💬 Chat de Análisis Comercial (MCP + IA)")

if "messages" not in st.session_state:
    st.session_state["messages"] = []

for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("Escribí tu pregunta..."):
    st.chat_message("user").markdown(prompt)
    st.session_state["messages"].append({"role": "user", "content": prompt})

    with st.spinner("Analizando..."):
        try:
            resp = requests.post(f"{API_URL}/chat", json={"prompt": prompt}).json()
            if "response" in resp:
                body = f"**SQL generada:**\n```sql\n{resp.get('sql','')}\n```\n"
                body += f"**Respuesta:**\n{resp['response']}\n"
                st.chat_message("assistant").markdown(body)
                st.session_state["messages"].append({"role": "assistant", "content": body})
            else:
                st.error(resp)
        except Exception as e:
            st.error(f"Error de conexión: {e}")
