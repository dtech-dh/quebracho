import logging, asyncio
from fastapi import APIRouter, HTTPException #type: ignore
from pydantic import BaseModel #type: ignore
from analyzer_ai import analyze_query

router = APIRouter()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

class ChatRequest(BaseModel):
    prompt: str

@router.post("/chat")
async def chat(req: ChatRequest):
    prompt = req.prompt.strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="Prompt vacío")

    logging.info(f"💬 Pregunta: {prompt}")

    try:
        result = await asyncio.wait_for(analyze_query(prompt), timeout=90)

        return {
            "plan": result.get("plan"),
            "sql": result.get("sql"),
            "response": result.get("response"),
            "data_preview": result.get("data_preview", [])
        }

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Timeout: la consulta tardó demasiado.")
    except Exception as e:
        logging.error(f"❌ Error en /chat: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/ping")
def ping():
    return {"status": "ok", "message": "AI + MCP API operativa"}
