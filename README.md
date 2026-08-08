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

- I was not able to fully resolve a `pgvector` extension issue in time for
  submission: `CREATE EXTENSION vector` succeeded and the type showed up
  correctly when queried directly in the Lakebase SQL editor (as the
  `weather_app` role), but the same role's connection from inside the
  deployed app still raised `type "public.vector" does not exist` when
  creating the `weather_embeddings` table. `weather_documents` (which has
  no vector column) synced successfully end-to-end, so the harvesting/
  ingestion half of the pipeline is proven working — the embedding table
  creation and the `/weather/search` retrieval endpoint are implemented
  per spec but not yet verified running against live data. Given more
  time I'd track down whether this is a branch/endpoint mismatch between
  the app's connection and the SQL editor's connection, or a Postgres
  session-level type-cache issue.
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

**What was the most difficult part?**
The hardest part was debugging the Lakebase connection and permission
chain rather than the actual embedding/retrieval logic — I hit a `pgvector`
extension error where the type existed and was queryable directly in the
SQL editor under the app's role, but the deployed app's own connection
still couldn't see it, and I wasn't able to fully root-cause the mismatch
before the deadline.

**How is Lakebase different from storing this data in a traditional
analytics table?**
A traditional analytics table is typically append-only, batch-loaded, and
optimized for large scans, whereas Lakebase is a real transactional
Postgres database that supports fast single-row reads/writes, foreign
keys, extensions like `pgvector`, and immediate consistency — which is
what lets this app both serve live API requests and run vector similarity
search in the same store, instead of needing a separate vector database.

**What feature would you add next?**
I'd add a scheduled sync (Databricks Workflow) so `weather_documents` stays
current automatically instead of requiring a manual `/weather/sync` call,
and once the pgvector connection issue is resolved, a `GET /weather/search`
variant that returns an LLM-generated natural-language summary of the top
results.
