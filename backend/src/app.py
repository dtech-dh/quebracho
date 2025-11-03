"""
Entry point principal del backend de Quebracho.
Este archivo no define rutas — solo importa la app principal desde router_ai.
Así evitamos instancias duplicadas de FastAPI y mantenemos coherencia.
"""

from router_ai import app  # Importa la app con /chat y /ping

# ✅ Este archivo se usa como punto de entrada para uvicorn o gunicorn
# Ejemplo de ejecución manual:
#   uvicorn app:app --host 0.0.0.0 --port 8000
#
# Ejemplo en producción (Dockerfile):
#   CMD ["uvicorn", "router_ai:app", "--host", "0.0.0.0", "--port", "8000"]
#
# Nota:
# - No se debe crear otra instancia de FastAPI aquí.
# - Todos los endpoints deben estar definidos en router_ai.py.
