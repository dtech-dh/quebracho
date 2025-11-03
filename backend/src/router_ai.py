import logging
from fastapi import FastAPI, Body
from fastapi.responses import JSONResponse
from analyzer_ai import analyze_query

# =====================================================
# 🚀 Aplicación principal
# =====================================================
app = FastAPI(title="Quebracho Backend - MCP + IA")

# Memoria simple por usuario (para mantener contexto de conversación)
user_contexts = {}

# =====================================================
# 💬 CHAT ENDPOINT
# =====================================================
@app.post("/chat")
async def chat(req: dict = Body(...)):
    """
    Endpoint principal del chat comercial.
    Usa IA + Postgres (MCP) para responder preguntas en lenguaje natural.
    """
    prompt = req.get("prompt", "").strip()
    user_id = req.get("user_id", "anon")

    if not prompt:
        return JSONResponse(status_code=400, content={"error": "Prompt vacío"})

    logging.info(f"💬 ({user_id}) Pregunta: {prompt}")

    # Crear o recuperar contexto del usuario
    if user_id not in user_contexts:
        user_contexts[user_id] = []

    user_contexts[user_id].append({"role": "user", "content": prompt})

    try:
        # 🔍 Analizar y ejecutar consulta
        result = await analyze_query(prompt, context=user_contexts[user_id])

        # Guardar respuesta en el contexto
        user_contexts[user_id].append({"role": "assistant", "content": result["response"]})

        # Mantener solo las últimas 15 interacciones
        if len(user_contexts[user_id]) > 15:
            user_contexts[user_id] = user_contexts[user_id][-15:]

        logging.info(f"✅ ({user_id}) SQL: {result.get('sql')}")

        return {
            "user_id": user_id,
            "sql": result.get("sql"),
            "response": result.get("response"),
            "plan": result.get("plan"),
            "context_len": len(user_contexts[user_id]),
        }

    except Exception as e:
        logging.error(f"❌ Error en /chat ({user_id}): {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# =====================================================
# 🧭 HEALTHCHECK
# =====================================================
@app.get("/ping")
def ping():
    """Verifica que el backend esté activo."""
    return {"status": "ok", "msg": "Backend MCP + IA activo"}
