# 💬 CHAT ENDPOINT (con memoria extendida)
@app.post("/chat")
async def chat(req: dict = Body(...)):
    """
    Chat principal con IA + Postgres + memoria extendida persistente.
    Recupera las últimas interacciones del usuario y las reintegra en la conversación.
    """
    prompt = req.get("prompt", "").strip()
    user_id = req.get("user_id", "anon")

    if not prompt:
        return JSONResponse(status_code=400, content={"error": "Prompt vacío"})

    logging.info(f"💬 ({user_id}) Pregunta: {prompt}")

    try:
        # 🧠 Recuperar contexto previo desde DB (últimas 10 interacciones)
        context = memory.get_recent_context(user_id)

        # 🔍 Analizar y ejecutar con IA
        result = await analyze_query(prompt, context=context)

        # 💾 Guardar nueva interacción en la memoria persistente
        memory.save_interaction(
            user_id=user_id,
            prompt=prompt,
            response=result.get("response", ""),
            sql=result.get("sql", ""),
            resumen=result.get("plan", ""),
        )

        logging.info(f"✅ ({user_id}) SQL ejecutada: {result.get('sql')}")
        return {
            "user_id": user_id,
            "sql": result.get("sql"),
            "response": result.get("response"),
            "plan": result.get("plan"),
            "context_len": len(context) + 1,
        }

    except Exception as e:
        logging.error(f"❌ Error en /chat ({user_id}): {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
