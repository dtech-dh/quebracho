import streamlit as st  # type: ignore
import requests
import os
from datetime import datetime

# =========================================================
# CONFIGURACIÓN
# =========================================================
API_URL = os.getenv("API_URL", "http://api:8000")

USERS = {
    "admin": os.getenv("APP_ADMIN_PASS", "1234minda2025"),
    "daniel": os.getenv("APP_DANI_PASS", "dani2025"),
    "marcelo": os.getenv("APP_MARCE_PASS", "marce2025"),
}

# =========================================================
# FUNCIONES AUXILIARES
# =========================================================
@st.cache_data(ttl=600)
def obtener_ultima_actualizacion():
    try:
        r = requests.get(f"{API_URL}/last", timeout=20)
        if r.status_code == 200:
            return r.json().get("ultima_actualizacion")
    except Exception:
        pass
    return None


def obtener_historial(user_id: str, limit: int = 50):
    try:
        r = requests.get(f"{API_URL}/history/{user_id}?limit={limit}", timeout=20)
        if r.status_code == 200:
            return r.json().get("history", [])
    except Exception as e:
        st.warning(f"No se pudo obtener historial: {e}")
    return []


def limpiar_historial(user_id: str):
    try:
        r = requests.delete(f"{API_URL}/context/{user_id}", timeout=20)
        if r.status_code == 200:
            st.success("🧹 Historial borrado correctamente.")
            st.session_state["messages"] = []
            st.rerun()
    except Exception as e:
        st.error(f"Error al borrar historial: {e}")


# =========================================================
# LOGIN
# =========================================================
def login():
    st.title("🔐 Acceso al Chat Comercial")
    u = st.text_input("Usuario")
    p = st.text_input("Contraseña", type="password")
    if st.button("Iniciar sesión"):
        if u in USERS and p == USERS[u]:
            st.session_state["auth"] = True
            st.session_state["user"] = u
            st.session_state["messages"] = []
            st.success(f"Bienvenido, {u} 👋")
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos.")


# =========================================================
# APLICACIÓN PRINCIPAL
# =========================================================
def main():
    st.set_page_config(page_title="MCP Chat", page_icon="💬", layout="wide")
    st.title("💬 Chat de Análisis Comercial (MCP + IA)")

    user_id = st.session_state.get("user", "anon")

    # Inicializamos variable temporal para reutilización
    if "reuse_prompt" not in st.session_state:
        st.session_state["reuse_prompt"] = None

    # === SIDEBAR ===
    with st.sidebar:
        st.subheader("📋 Panel lateral")
        st.markdown(f"👤 **Usuario:** `{user_id}`")

        ultima = obtener_ultima_actualizacion()
        if ultima:
            st.markdown(f"📅 **Datos actualizados al:** `{ultima}`")
        else:
            st.warning("⚠️ No se pudo determinar la fecha de datos.")

        # === Historial ===
        st.markdown("### 🕓 Historial de conversaciones")
        historial = obtener_historial(user_id)

        if not historial:
            st.info("Sin conversaciones previas.")
        else:
            for h in reversed(historial):
                fecha = h.get("timestamp", "")
                prompt = h.get("prompt", "")
                resp = h.get("response", "")
                with st.expander(f"📅 {fecha[:16]} — {prompt[:50]}"):
                    st.markdown(f"**🧍 Pregunta:** {prompt}")
                    st.markdown(f"**💬 Respuesta:** {resp[:400]}…")
                    if h.get("sql"):
                        st.code(h["sql"], language="sql")

                    # 🔁 Guardamos en session_state y forzamos rerun
                    if st.button("🔁 Reutilizar", key=f"reuse_{fecha}_{prompt[:10]}"):
                        st.session_state["reuse_prompt"] = prompt
                        st.rerun()

        st.markdown("### 📈 Estadísticas de uso")

        try:
            resp = requests.get(f"{API_URL}/metrics/usage", timeout=30)
            if resp.status_code == 200:
                data = resp.json()

                # Totales por usuario
                st.subheader("👥 Consultas por usuario")
                for item in data["usuarios"]:
                    st.markdown(f"- **{item['user']}** → {item['total']} preguntas")

                # Top 5 preguntas
                st.subheader("💬 Preguntas más repetidas")
                for t in data["top5"]:
                    st.markdown(f"• {t['prompt']} ({t['repeticiones']} veces)")

                # Gráfico de actividad diaria
                df = None
                try:
                    import pandas as pd
                    df = pd.DataFrame(data["por_dia"])
                    df["fecha"] = pd.to_datetime(df["fecha"])
                    st.line_chart(df.set_index("fecha")["total"])
                except Exception:
                    st.info("No hay suficientes datos para graficar.")

            else:
                st.warning("No se pudieron cargar métricas.")
        except Exception as e:
            st.error(f"Error cargando métricas: {e}")


        st.markdown("---")
        if st.button("🧹 Borrar historial"):
            limpiar_historial(user_id)
        if st.button("🚪 Cerrar sesión"):
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()

    # === CHAT CENTRAL ===
    if "messages" not in st.session_state:
        st.session_state["messages"] = []

    # Si se presionó reutilizar, inyectamos la pregunta
    if st.session_state.get("reuse_prompt"):
        prompt = st.session_state["reuse_prompt"]
        st.session_state["reuse_prompt"] = None
        st.chat_message("user").markdown(prompt)
        st.session_state["messages"].append({"role": "user", "content": prompt})
        with st.spinner("Analizando..."):
            try:
                payload = {"prompt": prompt, "user_id": user_id}
                r = requests.post(f"{API_URL}/chat", json=payload, timeout=60)
                if r.status_code == 200:
                    data = r.json()
                    body = f"**SQL generada:**\n```sql\n{data.get('sql','')}\n```\n"
                    body += f"**Respuesta:**\n{data.get('response','')}\n"
                    st.chat_message("assistant").markdown(body)
                    st.session_state["messages"].append(
                        {"role": "assistant", "content": body}
                    )
                else:
                    st.error(r.text)
            except Exception as e:
                st.error(f"Error de conexión: {e}")

    # Renderiza historial actual
    for m in st.session_state["messages"]:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

    # Entrada de texto normal
    if prompt := st.chat_input("Escribí tu pregunta..."):
        st.chat_message("user").markdown(prompt)
        st.session_state["messages"].append({"role": "user", "content": prompt})
        with st.spinner("Analizando..."):
            try:
                payload = {"prompt": prompt, "user_id": user_id}
                r = requests.post(f"{API_URL}/chat", json=payload, timeout=60)
                if r.status_code == 200:
                    data = r.json()
                    body = f"**SQL generada:**\n```sql\n{data.get('sql','')}\n```\n"
                    body += f"**Respuesta:**\n{data.get('response','')}\n"
                    st.chat_message("assistant").markdown(body)
                    st.session_state["messages"].append(
                        {"role": "assistant", "content": body}
                    )
                else:
                    st.error(r.text)
            except Exception as e:
                st.error(f"Error de conexión: {e}")


# =========================================================
# CONTROL DE AUTENTICACIÓN
# =========================================================
if "auth" not in st.session_state or not st.session_state["auth"]:
    login()
else:
    main()
