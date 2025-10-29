from fastapi import FastAPI #type: ignore
from router_ai import router as ai_router
from mcp_postgres import PostgresMCP, get_table_schema

app = FastAPI(title="MCP + IA API")

pg = PostgresMCP()

@app.get("/ping")
def ping():
    return {"status": "ok", "msg": "API MCP + IA operativa"}

@app.post("/query_postgres")
async def query_postgres(body: dict):
    query = body.get("query")
    if not query:
        return {"error": "Falta parámetro 'query'"}
    result = pg.run_sql(query)
    return {"result": result.to_dict(orient="records") if not isinstance(result, dict) else result}

@app.post("/get_table_schema")
async def get_schema(body: dict):
    table = body.get("table_name", pg.table)
    schema = get_table_schema(table)
    return {"result": schema}

# 🧠 Rutas IA
app.include_router(ai_router, prefix="")
