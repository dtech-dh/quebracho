import streamlit as st  # type: ignore
import requests
import os

# ==============================
# CONFIGURACIÓN
# ==============================
API_URL = os.getenv("API_URL", "http://api:8000")

# Usuarios válidos (podrías leerlos desde .env)
USERS = {
    "admin": os.getenv("APP_ADMIN_PASS", "1234minda2025"),
    "daniel": os.getenv("APP_DANI_PASS", "dani2025"),
    "marcelo": os.getenv("APP_MARCE_PASS", "marce2025"),
}

# ==============================
# FUNCIÓN: Obtener última actualización desde backend
# ==============================
@st.cache_data(ttl=600)
def obtener_ultima_actualizacion():
    """Consulta al backend la última fecha disponible en la base."""
    try:
        resp = requests.get(f"{API_URL}/ultima_actualizacion", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("ultima_actualizacion")
        return None
    except Exception as e:
        st.warning(f"No se pudo obtener la fecha de actualización: {e}")
        return None


# ==============================
# FUNCIÓN LOGIN
# ==============================
def login():
    """Renderiza formulario de login."""
    st.title("🔐 Acceso al Chat Comercial")
    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")
    login_button = st.button("Iniciar sesión")

    if login_button:
        if username in USERS and password == USERS[username]:
            st.session_state["auth"] = True
            st.session_state["user"] = username
            st.success(f"Bienvenido, {username} 👋")
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos.")


# ==============================
# APLICACIÓN PRINCIPAL
# ==============================
def main():
    st.set_page_config(page_title="MCP Chat", page_icon="💬", layout="centered")
    st.title("💬 Chat de Análisis Comercial (MCP + IA)")

    # Mostrar última actualización
    ultima_fecha = obtener_ultima_actualizacion()
    if ultima_fecha:
        st.info(f"📅 Datos actualizados al: **{ultima_fecha}**")
    else:
        st.warning("⚠️ No se pudo determinar la última fecha de actualización de datos.")

    # Conversación
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
                    st.session_state["messages"].append(
                        {"role": "assistant", "content": body}
                    )
                else:
                    st.error(resp)
            except Exception as e:
                st.error(f"Error de conexión: {e}")


# ==============================
# CONTROL DE AUTENTICACIÓN
# ==============================
if "auth" not in st.session_state or not st.session_state["auth"]:
    login()
else:
    # Botón para cerrar sesión
    with st.sidebar:
        st.info(f"👤 Usuario: {st.session_state.get('user','')}")
        if st.button("Cerrar sesión"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

    main()
