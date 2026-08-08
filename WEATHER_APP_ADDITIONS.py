"""
WEATHER_APP_ADDITIONS.py
=========================
This is NOT meant to run standalone. Copy each section into your forked
app.py at the marked spot. It follows the exact same conventions as the
existing Massive-sync code (lakebase.run_query / lakebase.run_write,
ensure_table() pattern, JSON error handler already registered in app.py).

--------------------------------------------------------------------------
1) ADD THESE IMPORTS near the top of app.py (with the other imports)
--------------------------------------------------------------------------
"""
import hashlib as _hashlib  # noqa: F401  (already imported as plain hashlib below if you prefer)
from sentence_transformers import SentenceTransformer

import lakebase
from weather_client import WeatherClient

# Load once at module level — NOT per-request (per assignment requirement).
_embedding_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

WEATHER_DOCS_TABLE = os.environ.get("WEATHER_DOCS_TABLE", "weather_documents")
WEATHER_EMBED_TABLE = os.environ.get("WEATHER_EMBED_TABLE", "weather_embeddings")
DEFAULT_WEATHER_LOCATIONS = [
    loc.strip()
    for loc in os.environ.get("WEATHER_LOCATIONS", "Chicago, IL,Austin, TX").split(",")
    if loc.strip()
]


"""
--------------------------------------------------------------------------
2) ADD THESE TABLE-CREATION HELPERS next to ensure_table()/ensure_news_table()
--------------------------------------------------------------------------
"""
def ensure_weather_tables():
    """Create weather_documents + weather_embeddings (with pgvector) if missing."""
    lakebase.run_write("CREATE EXTENSION IF NOT EXISTS vector")

    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {WEATHER_DOCS_TABLE} (
            id TEXT PRIMARY KEY,
            location TEXT NOT NULL,
            source_type TEXT NOT NULL CHECK (source_type IN ('alert', 'forecast')),
            headline TEXT,
            narrative_text TEXT NOT NULL,
            issued_at TIMESTAMPTZ,
            effective_at TIMESTAMPTZ,
            payload JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    lakebase.run_write(
        f"CREATE INDEX IF NOT EXISTS idx_{WEATHER_DOCS_TABLE}_location "
        f"ON {WEATHER_DOCS_TABLE} (location)"
    )

    # 384 dims to match sentence-transformers/all-MiniLM-L6-v2
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
    # HNSW index for fast cosine-similarity retrieval.
    lakebase.run_write(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{WEATHER_EMBED_TABLE}_cosine
        ON {WEATHER_EMBED_TABLE}
        USING hnsw (embedding vector_cosine_ops)
        """
    )


"""
--------------------------------------------------------------------------
3) ADD THESE ROUTES next to the other @app.route definitions
--------------------------------------------------------------------------
"""
@app.route("/weather/sync", methods=["POST"])
def sync_weather():
    """
    Fetch active alerts + forecast narratives for a list of locations from
    the NWS API and upsert them into weather_documents.

    Body (optional JSON): {"locations": ["Chicago, IL", "Austin, TX"], "limit": 50}
    """
    ensure_weather_tables()
    body = request.json if request.is_json else {}
    locations = body.get("locations") or DEFAULT_WEATHER_LOCATIONS
    limit = int(body.get("limit", 50))

    client = WeatherClient()
    total = 0
    errors = []
    for location in locations:
        try:
            docs = client.get_documents_for_location(location, limit=limit)
        except Exception as exc:  # noqa: BLE001 — surface per-location failures, keep syncing others
            errors.append({"location": location, "error": str(exc)})
            continue
        total += _upsert_weather_batch(docs)

    response = {"synced": total, "locations": locations}
    if errors:
        response["errors"] = errors
    return jsonify(response)


@app.route("/weather/search", methods=["POST"])
def search_weather():
    """
    Semantic search over ingested weather documents.

    Body: {"query": "flash flood risk this weekend", "top_k": 5}
    """
    body = request.json if request.is_json else {}
    query = (body.get("query") or "").strip()
    if not query:
        return jsonify({"error": "Field 'query' is required and cannot be empty"}), 400

    try:
        top_k = int(body.get("top_k", 5))
    except (TypeError, ValueError):
        return jsonify({"error": "'top_k' must be an integer"}), 400
    top_k = max(1, min(top_k, 20))  # clamp to 1-20

    count_row = lakebase.run_query(f"SELECT COUNT(*) AS n FROM {WEATHER_EMBED_TABLE}")
    if not count_row or count_row[0]["n"] == 0:
        return jsonify({
            "query": query,
            "results": [],
            "message": "No weather documents have been embedded yet — run /weather/sync "
                       "then the ingest_weather_embeddings notebook first.",
        })

    query_embedding = _embedding_model.encode(query).tolist()

    rows = lakebase.run_query(
        f"""
        SELECT d.id, d.location, d.headline, d.narrative_text, e.chunk_text,
               1 - (e.embedding <=> %s::vector) AS similarity
        FROM {WEATHER_EMBED_TABLE} e
        JOIN {WEATHER_DOCS_TABLE} d ON d.id = e.document_id
        ORDER BY e.embedding <=> %s::vector
        LIMIT %s
        """,
        (query_embedding, query_embedding, top_k),
    )

    return jsonify({"query": query, "top_k": top_k, "results": rows})


"""
--------------------------------------------------------------------------
4) ADD THIS HELPER next to _upsert_batch() / _upsert_news_batch()
--------------------------------------------------------------------------
"""
def _upsert_weather_batch(docs: list[dict]) -> int:
    """Upsert normalized weather documents into weather_documents."""
    import json as _json

    count = 0
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            for doc in docs:
                cur.execute(
                    f"""
                    INSERT INTO {WEATHER_DOCS_TABLE} (
                        id, location, source_type, headline, narrative_text,
                        issued_at, effective_at, payload, synced_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (id) DO UPDATE
                    SET location = EXCLUDED.location,
                        source_type = EXCLUDED.source_type,
                        headline = EXCLUDED.headline,
                        narrative_text = EXCLUDED.narrative_text,
                        issued_at = EXCLUDED.issued_at,
                        effective_at = EXCLUDED.effective_at,
                        payload = EXCLUDED.payload,
                        synced_at = EXCLUDED.synced_at
                    """,
                    (
                        doc["id"],
                        doc["location"],
                        doc["source_type"],
                        doc.get("headline"),
                        doc["narrative_text"],
                        doc.get("issued_at"),
                        doc.get("effective_at"),
                        _json.dumps(doc["payload"]),
                    ),
                )
                count += 1
        conn.commit()
    return count
