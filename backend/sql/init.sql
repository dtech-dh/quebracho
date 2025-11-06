CREATE TABLE chat_memory (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT now(),
    prompt TEXT,
    response TEXT,
    sql TEXT,
    resumen TEXT
);

CREATE INDEX idx_chat_memory_user ON chat_memory (user_id);