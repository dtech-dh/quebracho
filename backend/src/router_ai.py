import os
import time
import logging
import psycopg2
from fastapi import FastAPI, Body, Response  # type: ignore
from fastapi.responses import JSONResponse  # type: ignore
from analyzer_ai import analyze_query
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, generate_latest  # type: ignore
from memory_manager import MemoryManager  # 🧠 Nuevo módulo de memoria persistente

# =====================================================
# ⚙️ Configuración base
# =====================================================
load_dotenv()

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "db"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

app = FastAPI(title="Quebracho Backend - MCP + IA")
memory = MemoryManager()

# Prometheus metrics
REQS = Counter("quebracho_requests_total", "Total de requests", ["route"])
LAT = Histogram("quebracho_latency_seconds", "Latencia por endpoint", ["route"])

@app.middleware("http")
async def metrics_middleware(request, call_next):
    route = request.url.path
    REQS.labels(route=route).inc()
    start = time.time()
    response = await call_next(request)
    LAT.labels(route=route).observe(time.time() - start)
    return response


# =====================================================
# 💬 CHAT ENDPOINT (con memoria persistente)
# =====================================================
@app.post("/chat")
async def chat(req: dict = Body(...)):
    """
    Endpoint principal del chat comercial.
    Usa IA + Postgres (MCP) con memoria conversacional en base de datos.
    """
    prompt = req.get("prompt", "").strip()
    user_id = req.get("user_id", "anon")

    if not prompt:
        return JSONResponse(status_code=400, content={"error": "Prompt vacío"})

    logging.info(f"💬 ({user_id}) Pregunta: {prompt}")

    try:
        # 🔹 Recuperar contexto previo (últimas 5 interacciones)
        context = memory.get_recent_context(user_id)

        # 🔹 Analizar y ejecutar consulta con contexto
        result = await analyze_query(prompt, context=context)

        # 🔹 Guardar interacción completa en memoria persistente
        memory.save_interaction(
            user_id,
            prompt,
            result.get("response", ""),
            result.get("sql", ""),
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


# =====================================================
# 🧠 CONTEXTO (Memoria)
# =====================================================
@app.get("/context/{user_id}")
def get_context(user_id: str):
    """Devuelve las últimas interacciones guardadas de un usuario."""
    try:
        data = memory.get_recent_context(user_id, limit=10)
        return {"user_id": user_id, "context": data}
    except Exception as e:
        logging.error(f"❌ Error al obtener contexto ({user_id}): {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.delete("/context/{user_id}")
def clear_context(user_id: str):
    """Limpia el historial conversacional de un usuario."""
    try:
        memory.clear_context(user_id)
        logging.info(f"🧹 Contexto de {user_id} eliminado correctamente.")
        return {"status": "ok", "msg": f"Contexto de {user_id} eliminado"}
    except Exception as e:
        logging.error(f"❌ Error al limpiar contexto ({user_id}): {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# =====================================================
# 🧭 HEALTHCHECK
# =====================================================
@app.get("/ping")
def ping():
    """Verifica que el backend esté activo."""
    return {"status": "ok", "msg": "Backend MCP + IA activo"}


# =====================================================
# 📅 FECHA DE ÚLTIMA ACTUALIZACIÓN
# =====================================================
@app.get("/last")
def ultima_actualizacion():
    """
    Devuelve la fecha máxima registrada en la tabla 'ventas'.
    Ideal para mostrar al usuario el rango temporal disponible.
    """
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
        cur.execute('SELECT MAX(fecha) FROM ventas;')
        result = cur.fetchone()
        conn.close()

        ultima_fecha = result[0].strftime("%Y-%m-%d") if result and result[0] else None
        logging.info(f"📅 Última fecha de datos: {ultima_fecha}")

        return {
            "status": "ok",
            "ultima_actualizacion": ultima_fecha,
            "descripcion": "Fecha más reciente encontrada en la tabla de ventas",
        }

    except Exception as e:
        logging.error(f"❌ Error al obtener última actualización: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# =====================================================
# 📊 MÉTRICAS (Prometheus)
# =====================================================
@app.get("/metrics")
def metrics():
    """Endpoint compatible con Prometheus."""
    return Response(generate_latest(), media_type="text/plain")
