"""
notebooks/ingest_weather_embeddings.py

Self-contained embedding ETL for the weather pipeline. Mirrors the shape of
notebooks/ingest_ticker_news_embeddings.py but is plain psycopg2 (NOT Spark —
spark.write.jdbc is not reliable against Lakebase in this environment).

What it does:
  1. Reads rows from weather_documents that don't have embeddings yet
     (via lakebase.get_connection(), same helper app.py uses).
  2. Chunks narrative_text using a sliding window (CHUNK_SIZE / CHUNK_OVERLAP
     characters). Most NWS alert/forecast text is short, so most documents
     end up as a single chunk — chunking mainly matters for long combined
     alert description+instruction text.
  3. Embeds each chunk with sentence-transformers/all-MiniLM-L6-v2 (384-dim),
     matching the model used by app.py's /weather/search endpoint.
  4. Batch-writes embeddings into weather_embeddings via
     psycopg2.extras.execute_values, casting to ::vector in SQL.

Run it directly:
    python notebooks/ingest_weather_embeddings.py

Or as a Databricks notebook cell / scheduled Workflow task (see the main
README's "Scheduling the embeddings notebook" section — swap the notebook
path for this file).
"""

import os
import sys

# Allow running this file directly from notebooks/ by adding the repo root
# to the path so `import lakebase` resolves.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from psycopg2.extras import execute_values
from sentence_transformers import SentenceTransformer

import lakebase

WEATHER_DOCS_TABLE = os.environ.get("WEATHER_DOCS_TABLE", "weather_documents")
WEATHER_EMBED_TABLE = os.environ.get("WEATHER_EMBED_TABLE", "weather_embeddings")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

# Most NWS text (alert description+instruction, detailedForecast) is well
# under 800 chars; chunking mainly kicks in for combined long alert text.
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 800))
CHUNK_OVERLAP = int(os.environ.get("CHUNK_OVERLAP", 100))


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """Sliding-window character chunking. Returns at least one chunk (or [] for empty text)."""
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    step = max(chunk_size - overlap, 1)
    while start < len(text):
        chunks.append(text[start:start + chunk_size])
        start += step
    return chunks


def fetch_unembedded_documents() -> list[dict]:
    """Documents in weather_documents with no rows yet in weather_embeddings."""
    return lakebase.run_query(
        f"""
        SELECT d.id, d.narrative_text
        FROM {WEATHER_DOCS_TABLE} d
        LEFT JOIN {WEATHER_EMBED_TABLE} e ON e.document_id = d.id
        WHERE e.id IS NULL AND d.narrative_text IS NOT NULL AND d.narrative_text <> ''
        """
    )


def ensure_embeddings_table():
    """Same DDL as app.py's ensure_weather_tables(), safe to re-run (idempotent)."""
    lakebase.run_write("CREATE EXTENSION IF NOT EXISTS vector")
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {WEATHER_EMBED_TABLE} (
            id SERIAL PRIMARY KEY,
            document_id TEXT NOT NULL REFERENCES {WEATHER_DOCS_TABLE}(id) ON DELETE CASCADE,
            chunk_index INT NOT NULL,
            chunk_text TEXT NOT NULL,
            embedding vector(384) NOT NULL,
            model_name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (document_id, chunk_index)
        )
        """
    )
    lakebase.run_write(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{WEATHER_EMBED_TABLE}_cosine
        ON {WEATHER_EMBED_TABLE}
        USING hnsw (embedding vector_cosine_ops)
        """
    )


def main():
    ensure_embeddings_table()

    documents = fetch_unembedded_documents()
    print(f"Found {len(documents)} document(s) to embed.")
    if not documents:
        return

    model = SentenceTransformer(EMBEDDING_MODEL)

    rows_to_insert = []
    for doc in documents:
        chunks = chunk_text(doc["narrative_text"])
        if not chunks:
            continue
        embeddings = model.encode(chunks)
        for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            rows_to_insert.append((
                doc["id"],
                idx,
                chunk,
                embedding.tolist(),
                EMBEDDING_MODEL,
            ))

    if not rows_to_insert:
        print("No chunks produced (all documents had empty narrative_text).")
        return

    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {WEATHER_EMBED_TABLE}
                    (document_id, chunk_index, chunk_text, embedding, model_name)
                VALUES %s
                ON CONFLICT (document_id, chunk_index) DO UPDATE
                SET chunk_text = EXCLUDED.chunk_text,
                    embedding = EXCLUDED.embedding,
                    model_name = EXCLUDED.model_name
                """,
                rows_to_insert,
                template="(%s, %s, %s, %s::vector, %s)",
            )
        conn.commit()

    print(f"Wrote {len(rows_to_insert)} embedding row(s) for {len(documents)} document(s).")


if __name__ == "__main__":
    main()
