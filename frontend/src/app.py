import streamlit as st  # type: ignore
import requests
import os

# ==============================
# CONFIGURACIÓN
# ==============================
API_URL = os.getenv("API_URL", "http://api:8000")

# Usuarios válidos (podrías leerlos desde .env)
USERS = {
    "admin": os.getenv("APP_ADMIN_PASS", "1234"),
    "dani": os.getenv("APP_USER_PASS", "dani2025"),
}

# ==============================
# FUNCIÓN LOGIN
# ==============================
def login():
    """Renderiza formulario de login."""
    st.set_page_config(page_title="Login", page_icon="🔐", layout="centered")
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

    username = st.session_state.get("user", "anon")

    if "messages" not in st.session_state:
        st.session_state["messages"] = []

    # Mostrar historial del chat
    for msg in st.session_state["messages"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Entrada de chat
    if prompt := st.chat_input("Escribí tu pregunta..."):
        st.chat_message("user").markdown(prompt)
        st.session_state["messages"].append({"role": "user", "content": prompt})

        with st.spinner("Analizando..."):
            try:
                # ✅ enviamos también el user_id
                resp = requests.post(
                    f"{API_URL}/chat",
                    json={"prompt": prompt, "user_id": username},
                    timeout=60
                ).json()

                if "response" in resp:
                    sql_txt = resp.get("sql", "")
                    body = ""
                    if sql_txt:
                        body += f"**SQL generada:**\n```sql\n{sql_txt}\n```\n"
                    body += f"**Respuesta:**\n{resp['response']}\n"

                    st.chat_message("assistant").markdown(body)
                    st.session_state["messages"].append(
                        {"role": "assistant", "content": body}
                    )
                else:
                    st.error(f"⚠️ Error inesperado: {resp}")
            except Exception as e:
                st.error(f"🚨 Error de conexión con el backend: {e}")

# ==============================
# CONTROL DE AUTENTICACIÓN
# ==============================
if "auth" not in st.session_state or not st.session_state["auth"]:
    login()
else:
    # Barra lateral con logout
    with st.sidebar:
        st.info(f"👤 Usuario: {st.session_state.get('user','')}")
        if st.button("Cerrar sesión"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

    main()
