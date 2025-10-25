from fastapi import FastAPI, Request
from router_ai import analyze_query
from mcp_postgres import PostgresMCP, get_table_schema

app = FastAPI(title="MCP + IA API")

pg = PostgresMCP()

@app.get("/ping")
def ping():
    return {"status": "ok", "msg": "API MCP + IA operativa"}

@app.post("/query_postgres")
async def query_postgres(req: Request):
    body = await req.json()
    query = body.get("query")
    if not query:
        return {"error": "Falta parámetro 'query'"}
    result = pg.run_sql(query)
    return {"result": result.to_dict(orient="records") if not isinstance(result, dict) else result}

@app.post("/get_table_schema")
async def get_schema(req: Request):
    body = await req.json()
    table = body.get("table_name", pg.table)
    schema = get_table_schema(table)
    return {"result": schema}

@app.post("/chat")
async def chat(req: Request):
    body = await req.json()
    prompt = body.get("prompt")
    if not prompt:
        return {"error": "Falta prompt"}
    response = await analyze_query(prompt)
    return response
