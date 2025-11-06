import os
import json
import logging
import pandas as pd
from typing import List, Dict, Any
from sqlalchemy import create_engine, text  # type: ignore
from datetime import datetime, date
from decimal import Decimal
import psycopg2

# =====================================================
# 🔧 Configuración base
# =====================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def safe_json_dumps(obj):
    """Convierte cualquier objeto a JSON seguro."""
    def default(o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, Decimal):
            return float(o)
        if pd.isna(o):
            return None
        return str(o)

    try:
        return json.dumps(obj, ensure_ascii=False, default=default)
    except Exception as e:
        logging.warning(f"⚠️ Error serializando objeto a JSON: {e}")
        return json.dumps(str(obj), ensure_ascii=False)


class MemoryManager:
    """Memoria conversacional persistente en Postgres."""

    def __init__(self):
        DB = os.getenv("POSTGRES_DB")
        USER = os.getenv("POSTGRES_USER")
        PWD = os.getenv("POSTGRES_PASSWORD")
        HOST = os.getenv("POSTGRES_HOST", "db")
        PORT = os.getenv("POSTGRES_PORT", "5432")

        if not all([DB, USER, PWD, HOST, PORT]):
            raise RuntimeError("❌ Faltan variables de entorno de Postgres para MemoryManager")

        self.engine = create_engine(
            f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}",
            pool_pre_ping=True,
        )

        self._ensure_table()

    # =====================================================
    def _ensure_table(self):
        """Crea la tabla chat_memory si no existe."""
        ddl = """
        CREATE TABLE IF NOT EXISTS chat_memory (
            id BIGSERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            prompt TEXT,
            response TEXT,
            sql TEXT,
            resumen JSONB,
            timestamp TIMESTAMPTZ DEFAULT NOW()
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(ddl))
        logging.info("🧠 Tabla chat_memory verificada/creada.")

    # =====================================================
    def save_interaction(
        self,
        user_id: str,
        prompt: str,
        response: str,
        sql: str | None = None,
        resumen: Any | None = None,
    ):
        """Guarda una interacción en chat_memory de forma segura."""
        resumen_json = safe_json_dumps(resumen or {})

        try:
            conn = self.engine.raw_connection()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO chat_memory (user_id, prompt, response, sql, resumen)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                """,
                (user_id or "anon", prompt or "", response or "", sql or "", resumen_json),
            )
            conn.commit()
            cur.close()
            conn.close()
            logging.info(f"💾 Guardada interacción de {user_id}")
        except Exception as e:
            logging.error(f"❌ Error guardando interacción: {e}")

    # =====================================================
    def get_recent_context(self, user_id: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Recupera las últimas interacciones del usuario."""
        safe_limit = max(1, min(int(limit or 5), 50))
        q = text(f"""
            SELECT timestamp, prompt, response, sql
            FROM chat_memory
            WHERE user_id = :user_id
            ORDER BY timestamp DESC
            LIMIT {safe_limit}
        """)
        with self.engine.begin() as conn:
            rows = conn.execute(q, {"user_id": user_id}).fetchall()

        if not rows:
            return []

        df = pd.DataFrame(rows, columns=["timestamp", "prompt", "response", "sql"])
        records = df.iloc[::-1].to_dict(orient="records")

        for record in records:
            if isinstance(record.get("timestamp"), (datetime, date)):
                record["timestamp"] = record["timestamp"].isoformat()
        return records

    # =====================================================
    def clear_context(self, user_id: str) -> None:
        """Elimina el historial de un usuario."""
        q = text("DELETE FROM chat_memory WHERE user_id = :user_id")
        with self.engine.begin() as conn:
            conn.execute(q, {"user_id": user_id})
        logging.info(f"🧹 Historial limpiado para {user_id}")
