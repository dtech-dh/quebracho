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
    """Consulta al backend la última fecha disponible en la base."""
    try:
        resp = requests.get(f"{API_URL}/last", timeout=30)
        if resp.status_code == 200:
            return resp.json().get("ultima_actualizacion")
        return None
    except Exception as e:
        st.warning(f"No se pudo obtener la fecha de actualización: {e}")
        return None


def obtener_historial(user_id: str):
    """Obtiene el historial de chat del usuario desde el backend."""
    try:
        resp = requests.get(f"{API_URL}/context/{user_id}", timeout=30)
        if resp.status_code == 200:
            return resp.json().get("context", [])
        return []
    except Exception as e:
        st.warning(f"No se pudo obtener el historial: {e}")
        return []


def limpiar_historial(user_id: str):
    """Borra el contexto del usuario."""
    try:
        resp = requests.delete(f"{API_URL}/context/{user_id}", timeout=30)
        if resp.status_code == 200:
            st.success("🧹 Historial borrado correctamente.")
            st.session_state["messages"] = []
            st.rerun()
        else:
            st.error("No se pudo borrar el historial.")
    except Exception as e:
        st.error(f"Error al borrar historial: {e}")


# =========================================================
# LOGIN
# =========================================================
def login():
    """Renderiza formulario de login."""
    st.title("🔐 Acceso al Chat Comercial")
    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")
    if st.button("Iniciar sesión"):
        if username in USERS and password == USERS[username]:
            st.session_state["auth"] = True
            st.session_state["user"] = username
            st.session_state["messages"] = []
            st.success(f"Bienvenido, {username} 👋")
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos.")


# =========================================================
# APLICACIÓN PRINCIPAL
# =========================================================
def main():
    st.set_page_config(page_title="MCP Chat", page_icon="💬", layout="centered")
    st.title("💬 Chat de Análisis Comercial (MCP + IA)")

    user_id = st.session_state.get("user", "anon")

    # --- SIDEBAR ---
    with st.sidebar:
        st.subheader("📋 Panel lateral")

        # Info de usuario
        st.markdown(f"👤 **Usuario:** `{user_id}`")

        # Última actualización
        ultima_fecha = obtener_ultima_actualizacion()
        if ultima_fecha:
            st.markdown(f"📅 **Datos actualizados al:** `{ultima_fecha}`")
        else:
            st.warning("⚠️ No se pudo determinar la fecha de datos.")

        # Historial de chat
        st.markdown("### 🧠 Historial reciente")
        historial = obtener_historial(user_id)
        if historial:
            for item in historial:
                fecha = item.get("timestamp", "")[:16]
                prompt = item.get("prompt", "-")
                resp = item.get("response", "-")
                with st.expander(f"{prompt[:40]}..."):
                    st.markdown(f"**🗨️ Pregunta:** {prompt}")
                    st.markdown(f"**🤖 Respuesta:** {resp}")
                    st.badge(f"Fecha: {fecha}", icon=":material/schedule:", color="blue")
#                    if item.get("sql"):
#                        st.code(item["sql"], language="sql")
        else:
            st.info("Sin historial disponible.")

        # Botones
        if st.button("🧹 Borrar historial"):
            limpiar_historial(user_id)

        if st.button("🚪 Cerrar sesión"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

    # --- CHAT CENTRAL ---
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
                payload = {"prompt": prompt, "user_id": user_id}
                resp = requests.post(f"{API_URL}/chat", json=payload, timeout=60).json()
                if "response" in resp:
                    body = f"**SQL generada:**\n```sql\n{resp.get('sql','')}\n```\n"
                    body += f"**Respuesta:**\n{resp['response']}\n"
                    st.chat_message("assistant").markdown(body)
                    st.session_state["messages"].append(
                        {"role": "assistant", "content": body}
                    )
                else:
                    st.error(resp)
            except Exception as e:
                st.error(f"Error de conexión: {e}")


# =========================================================
# CONTROL DE AUTENTICACIÓN
# =========================================================
if "auth" not in st.session_state or not st.session_state["auth"]:
    login()
else:
    main()
