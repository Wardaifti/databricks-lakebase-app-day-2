# Weather Intelligence — Unstructured Data → Lakebase Vector Search → REST API

## Data source

**National Weather Service API (`api.weather.gov`)** — chosen because it's
free, requires no API key, and returns rich free-text narrative fields
(`description`/`instruction` on active alerts, `detailedForecast` on
forecast periods) that are exactly the kind of unstructured text this
assignment asks us to embed and retrieve semantically.

## Schema decisions

**`weather_documents`** — one row per alert or forecast period:
- `id` (TEXT PK) — stable dedup key: the NWS alert's own `id` for alerts,
  or a SHA1 hash of `location|startTime|periodName` for forecast periods
  (forecast periods have no natural stable ID from the API)
- `location`, `source_type` (`alert`/`forecast`), `headline`,
  `narrative_text` (the field we embed), `issued_at`, `effective_at`,
  `payload` (raw JSON for provenance), `synced_at`

**`weather_embeddings`** — one row per chunk:
- `id` (SERIAL PK), `document_id` (FK → `weather_documents.id`,
  `ON DELETE CASCADE`), `chunk_index`, `chunk_text`, `embedding vector(384)`,
  `model_name`, `created_at`
- `UNIQUE (document_id, chunk_index)` so re-running the ingest script is
  idempotent (upsert via `ON CONFLICT`)
- HNSW index on `embedding` with `vector_cosine_ops` for fast `<=>` search

**Chunking:** `CHUNK_SIZE=800`, `CHUNK_OVERLAP=100` (same defaults as the
ticker-news pipeline). Most NWS text is well under 800 characters — chunking
mainly activates for combined alert `description + instruction` text, which
can run long for severe weather events.

**Embedding model:** `sentence-transformers/all-MiniLM-L6-v2` (384-dim) —
same model as the existing ticker-news pipeline, so both stay queryable with
the same `<=>` cosine-distance convention and nothing needs a schema change
to stay compatible.

## Running the pipeline end-to-end

1. **Sync raw documents from NWS:**
   ```
   POST /weather/sync
   {"locations": ["Chicago, IL", "Austin, TX"], "limit": 50}
   ```
   This creates `weather_documents`/`weather_embeddings` (if they don't
   exist yet) and upserts alerts + forecast periods for each location.

2. **Embed the synced documents:**
   ```
   python notebooks/ingest_weather_embeddings.py
   ```
   Reads any `weather_documents` rows without embeddings yet, chunks +
   embeds `narrative_text`, and batch-writes into `weather_embeddings`.

3. **Search semantically:**
   ```
   POST /weather/search
   {"query": "flash flood risk this weekend", "top_k": 5}
   ```
   Returns the top `top_k` chunks ranked by cosine similarity, each with
   `location`, `headline`, `chunk_text`, and `similarity`.

## Known limitations / what I'd improve

- The location resolver uses a small hardcoded city→lat/lon lookup table
  since NWS has no geocoding endpoint — a real app would call a geocoding
  API (e.g. Census Geocoder, which is also free) to accept any city name.
- `/weather/sync` and the embedding step are two separate manual calls;
  scheduling the embedding notebook as a Databricks Workflow (like the
  existing ticker-news job) would make this fully automatic.
- Alerts expire and forecasts roll forward every few hours, so without a
  scheduled re-sync, `weather_documents` goes stale quickly — a cron/Workflow
  every 15-30 minutes would keep it current.

## Reflection

*(fill in 3–5 sentences for submission)*

**What was the most difficult part?**
—

**How is Lakebase different from storing this data in a traditional
analytics table?**
—

**What feature would you add next?**
—
