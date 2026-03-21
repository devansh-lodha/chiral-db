# Assignment 1: Adaptive Ingestion & Hybrid Backend Placement

**Course:** CS 432 – Databases (Semester II, 2025 - 2026)
**Instructor:** Dr. Yogesh K. Meena

**Team Members:**
*   **Deep Buha** (24110082) - `24110082@iitgn.ac.in`
*   **Devansh Lodha** (23110091) - `devansh.lodha@iitgn.ac.in`
*   **Laxmidhar Panda** (24110185) - `24110185@iitgn.ac.in`
*   **Rathod Yuvraj** (24110293) - `yuvraj.rathod@iitgn.ac.in`
*   **Viraj Solanki** (24110348) - `viraj.solanki@iitgn.ac.in`

---

## 1. Quick Start (Evaluator Mode)

### Prerequisites
*   **Docker** (Must be running)
*   **Just** (Command Runner):
    *   **macOS:** `brew install just`
    *   **Linux:** `sudo apt install just` (Ubuntu/Debian) or `sudo dnf install just` (Fedora)
    *   **Windows:** `choco install just` or `scoop install just`
*   **uv** (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

### Setup Configuration
Before running the demo, create the environment file:
```bash
cp .env.example .env
# OR manually create .env with:
# POSTGRES_USER=chiral
# POSTGRES_PASSWORD=chiral
# POSTGRES_DB=chiral_db
```

### Run the Demo
The following command will wipe previous data, start the containers, launch the API, run the TA's simulation server, ingest 1000 records, and automatically print a **Verification Report**.

```bash
just demo2
```
*   **PostgreSQL:** Will show a record count and a list of dynamically learned columns (e.g., `altitude`, `steps`, `battery`).
*   **Verification Report:** A text summary will appear at the end of the console output confirming the hybrid storage state.

### Cleanup
To stop background processes and remove containers:
```bash
just stop
just down
```

---

## 2. Mandatory Report Questions

### 1. Normalization Strategy
**Q: How did your system automatically detect repeating entities and generate normalised SQL tables?**

**A:** The system uses a three-stage pipeline to detect and normalize repeating entities:

1. **Schema Inference & Type Stability Analysis** (`analyzer.py::analyze_staging`): Analyzes first 100 incoming JSON records to extract field names and infer dominant types. For each field:
   - Computes `type_confidence`: ratio of records sharing the dominant type
    - If input types diverge, `type_confidence` drops; when it falls below configured threshold, field routes to JSONB
   - Calculates `field_stability_ratio`: (non-null presence) × (type confidence)
   - If stability < 0.75 (default threshold) → field routes to JSONB
2. **Decomposition Detection** (`migrator.py::_extract_decomposed_child_items`): Scans for array-type fields (e.g., `comments[]`, `events[]`) which signal repeating child entities. These are extracted into separate items.
3. **Dynamic Table Generation** (`db/schema.py::materialize_decomposition_tables`): For each detected child entity, a normalized SQL table is created with:
   - Auto-incrementing primary key (`id`)
    - Foreign key linking to parent via generated key spec (e.g., `chiral_data_id`)
    - One typed SQL column per detected child field (e.g., INTEGER, DOUBLE PRECISION, BOOLEAN, TEXT)
   - One JSONB column for complex nested objects (`overflow_data`)

**Example**: A record with `{comments: [{comment_id, text, score, is_flagged, meta}]}` triggers decomposition metadata with `child_column_types`, and `chiral_data_comments` columns are materialized as native types such as `comment_id` (INTEGER), `text` (TEXT), `score` (DOUBLE PRECISION), `is_flagged` (BOOLEAN), with `overflow_data` holding nested objects like `meta`.

### 2. Table Creation Logic
**Q: What rules were used to decide primary keys and foreign keys?**

**A:** Primary and foreign key decisions follow these rules:

1. **Primary Keys**:
    - Parent table: Uses surrogate primary key `id` for joins and indexing, while preserving `username`, `sys_ingested_at`, and `t_stamp` for traceability.
   - Child tables: Auto-incrementing `id` (sequential PK for easy indexing).

2. **Foreign Keys**:
    - Child tables reference parent via generated FK column (typically `<parent_table>_id`, e.g., `chiral_data_id`).
    - Generated via `build_dynamic_child_key_spec(parent_table, source_field)` which normalizes identifiers and creates referential integrity constraints.

3. **Index Strategy**:
   - PK columns auto-indexed.
   - Foreign key columns indexed for join efficiency.
   - JSONB columns use GIN indexes for fast nested-field queries (when decomposition is incomplete).

### 3. JSONB Design Strategy
**Q: How does your system decide between embedded documents and separate collections?**

**A:** The system employs a hybrid placement strategy:

1. **Separate SQL Tables When**:
   - Field is an array of objects (child entities): Normalize into dedicated child table.
   - Field appears consistently across 90%+ of records: Treat as schema-validated column.
   - Cardinality is high (10+ distinct values per entity): Create indexed SQL column.

2. **JSONB Embedding When**:
   - Field exhibits type drift: If same field has different types across records (int in record 1, string in record 2) → JSONB.
   - Field has low stability: If `field_stability_ratio` (presence × type_confidence) < 0.75 → JSONB.
   - Field is deeply nested (depth ≥ 1): Preserve structure in JSONB to avoid recursive decomposition.
   - Array or dict type detected: Route to JSONB.

3. **Decision Logic** (`normalization.py::evaluate_jsonb_strategy`):
    - type_confidence ≥ threshold (configurable) AND field_stability_ratio ≥ 0.75 AND max_nesting_depth below threshold → SQL column.
   - ANY previous condition fails → JSONB overflow (`overflow_data`).
   - Array/dict type OR nesting_depth ≥ 1 → JSONB or child table.

**Example**: `username` (always string, always present) → SQL column (`type_confidence=1.0`, `stability=1.0`). `temperature` (int/float mismatch in 2/100 records) remains SQL under default threshold (`type_confidence=0.98`, `stability=0.96`), but can route to JSONB under strict threshold tuning. `device` (string 85 records, int 15 records) tends toward JSONB as stability/consensus drops.

### 4. Metadata System
**Q: What information is stored in metadata and how is it used to generate queries?**

**A:** Metadata is stored in the `session_metadata` table (one row per ingestion session) with the following JSON structure:

```json
{
    "session_id": "session_assignment_1",
    "schema_json": {
        "attribute": {
            "unique": false,
            "unique_confidence": 0.04,
            "target": "sql",
            "routing_reason": "stable_scalar",
            "type": "str",
            "type_confidence": 1.0,
            "max_nesting_depth": 0,
            "field_stability_ratio": 1.0,
            "explainability": {
                "type_reason": "dominant_type_selected",
                "tie_break_applied": false,
                "strategy_rule": "stable_scalar_field",
                "type_confidence_threshold": 0.8,
                "uniqueness_confidence_threshold": 1.0,
                "nesting_depth_threshold": 1,
                "field_stability_ratio_threshold": 0.75
            }
        }
    },
    "schema_version": 1,
    "drift_events": [
        {
            "event": "column_migrated_to_jsonb",
            "column": "device_count",
            "previous_type": "int",
            "target": "jsonb",
            "timestamp": "2026-03-21T10:15:30.500Z"
        },
        {
            "event": "decomposition_plan_updated",
            "parent_table": "chiral_data",
            "entity_count": 2,
            "timestamp": "2026-03-21T10:15:45.200Z"
        }
    ],
    "safety_events": [
        {
            "event": "guardrail_route_to_jsonb",
            "column": "metadata",
            "reason": "field_size_exceeded",
            "size_bytes": 2500,
            "nesting_depth": 4,
            "timestamp": "2026-03-21T10:15:32.100Z"
        },
        {
            "event": "guardrail_route_to_jsonb",
            "column": "nested_tags",
            "reason": "field_nesting_exceeded",
            "size_bytes": 512,
            "nesting_depth": 6,
            "timestamp": "2026-03-21T10:15:35.400Z"
        }
    ],
    "migration_metrics": [
        {
            "phase": "ingest_parent_records",
            "rows_processed": 1000,
            "rows_inserted": 1000,
            "rows_per_second": 250.5,
            "jsonb_spill_ratio": 0.15,
            "drift_rate": 0.003,
            "guardrail_event_count": 12,
            "timestamp": "2026-03-21T10:16:00.100Z"
        },
        {
            "phase": "migrate_child_entities",
            "rows_processed": 2400,
            "rows_inserted": 2400,
            "rows_per_second": 300.2,
            "jsonb_spill_ratio": 0.08,
            "drift_rate": 0.0,
            "guardrail_event_count": 5,
            "timestamp": "2026-03-21T10:17:30.500Z"
        }
    ]
}
```

**Field Explanations**:

**1. `tie_break_applied` (Boolean)**
   - **Purpose**: Indicates whether a type tie-break rule was invoked during type inference.
   - **When true**: Multiple types had equal frequency in the sample (e.g., 5 records with `int` and 5 with `str`). System defaults to `str` type to avoid fragmentation.
   - **When false**: One type dominated (e.g., 90% `int`, 10% `str`), so the dominant type was selected.
   - **Impact**: If `true` + type_confidence < 0.8, the field will route to JSONB (type_drift detected).

**2. `drift_events` (Array)**
   - **Purpose**: Tracks schema evolution events—when fields migrate to JSONB or decomposition plan changes.
   - **Event Types**:
     - `column_migrated_to_jsonb`: Emitted when a column transitions from SQL to JSONB due to type drift. Stores `previous_type` for audit trail.
     - `decomposition_plan_updated`: Emitted when child entity extraction is first applied or updated. Stores `entity_count` (count of discovered repeating entities).
   - **Use Case**: Audit trail for schema mutations; enables rollback/remediation workflows; tracks when new child tables created.
   - **Guardrail**: Bounded to 200 events per session (configurable via `GUARDRAIL_MAX_DRIFT_EVENTS_PER_SESSION`).

**3. `safety_events` (Array)**
   - **Purpose**: Tracks guardrail triggers—when fields breached size or nesting constraints and were routed to JSONB for safety.
   - **Event Types**:
     - `guardrail_route_to_jsonb` (reason: `field_size_exceeded`): Field JSON serialization exceeded max bytes (default 5MB per field).
     - `guardrail_route_to_jsonb` (reason: `field_nesting_exceeded`): Field nesting depth exceeded threshold (default 10 levels).
   - **Use Case**: Identifies pathological data patterns; alerts operators when ingest-time data exceeds expected structure constraints.
   - **Telemetry**: Includes `size_bytes` and `nesting_depth` for forensics.
   - **Guardrail**: Bounded to 500 events per session (configurable via `GUARDRAIL_MAX_SAFETY_EVENTS_PER_SESSION`).

**4. `migration_metrics` (Array)**
   - **Purpose**: Performance and decomposition telemetry; records ingest throughput, JSONB spillage, and drift rates per phase.
   - **Fields**:
     - `phase`: Phase name (e.g., `ingest_parent_records`, `migrate_child_entities`).
     - `rows_processed`: Total records analyzed in this phase.
     - `rows_inserted`: Total records persisted to DB.
     - `rows_per_second`: Throughput (rows / elapsed time).
     - `jsonb_spill_ratio`: Ratio of fields routed to JSONB vs total fields (0.15 = 15% spillage).
     - `drift_rate`: Drift events per record (e.g., 0.003 = 3 drift events per 1000 records).
     - `guardrail_event_count`: Count of safety guardrail triggers in this phase.
     - `timestamp`: Completion timestamp.
   - **Use Case**: Monitors ingestion health; alerts if spill_ratio > 0.5 (excessive JSONB routing); identifies performance bottlenecks.

**Usage in Query Generation**:
- **Routing**: When a SELECT request references `comments.score`, metadata's `decomposition_plan` is consulted to determine a LEFT JOIN is needed.
- **Typed Child Metadata**: `child_column_types` is emitted per child entity and propagated to inferred joins.
- **Dynamic Joins**: `query_service.py::_build_inferred_joins_for_request` reads decomposition_plan to auto-generate join SQL.
- **Typed Filter Coercion**: Joined child SQL filters are coerced by inferred type (e.g., `"120"` → `120` for int, `"true"` → `True` for bool), and invalid typed values fail fast.
- **Safety Validation**: Drift/safety events provide retroactive justification for decomposition decisions; enable audit compliance workflows.

### 5. CRUD Query Generation
**Q: How does your system translate a user JSON request into SQL and JSONB queries?**

**A:** The translation pipeline in `query_service.py` follows these steps:

1. **Route Detection** (`QueryTranslateRequest`):
   - Parses incoming JSON: `{operation, table, select, filter, update, decomposition_plan}`.
   - Extracts `decomposition_plan` from request or from stored `analysis_metadata`.

2. **Inferred Join Generation** (`_build_inferred_joins_for_request`):
   - Scans `select` and `filter` fields for child-prefixed references (e.g., `comments.score`).
   - For each match, retrieves join metadata from decomposition_plan.
    - Builds `InferredJoin` objects with typed metadata: `{source_field, child_table, parent_fk_column, child_column_types}`.

3. **SQL Generation** (`CrudQueryBuilder.build_select/insert/update/delete`):
   - **SELECT**: Builds base query on parent table; if inferred_joins present, adds LEFT JOIN clauses.
    - **SELECT (typed child filters)**: Coerces joined child SQL filter values by inferred type (int/float/bool/timestamp) before binding params.
   - **INSERT**: Validates JSONB fields with type-safe regex + casting (e.g., numeric JSONB fields filtered as `::double precision`).
   - **UPDATE**: Routes unknown fields to `overflow_data` JSONB; known fields updated in SQL columns.
   - **DELETE**: Cascades to child tables via FK constraints.

4. **Execution** (`execute_json_request`):
   - Translates request to SQL + params.
   - Executes via async SQLAlchemy session.
   - Returns `{rows, row_count}` for read or `{affected_rows}` for write.

**Example**:
```json
Input: {
  "operation": "select",
  "table": "chiral_data",
  "select": ["username", "temperature", "comments.text", "comments.score"],
  "filter": {"comments.score": {">": 50}},
}
```
Generates SQL:
```sql
SELECT parent.username, parent.temperature, child.text, child.score
FROM chiral_data parent
LEFT JOIN chiral_data_comments child ON parent.id = child.parent_id
WHERE child.score > 50
```

### 6. Performance Considerations
**Q: How does your design reduce query complexity or document rewriting?**

**A:** The system reduces query overhead through strategic design choices:

1. **Inferred Joins Eliminate Manual Join Logic**:
   - Decomposition plan metadata enables one-pass join generation.
   - No runtime reflection or recursive traversal needed.
   - Join structure is deterministic and cacheable.

2. **JSONB Storage Avoids Deep Normalization**:
   - Type-drift fields (mixed types across records) stored as JSONB (`overflow_data`) to avoid schema conflicts.
   - Sparse/optional fields with low stability also route to JSONB (deferred validation).
   - GIN indexes on JSONB enable fast queries: `overflow_data @> '{"tag": "urgent"}'` is O(log N).

3. **Type Coercion at Ingest Time**:
   - Type-stable SQL columns are schema-validated (type_confidence ≥ 0.8, no casting needed).
   - Type-drift fields stored as JSONB (PostgreSQL handles mixed types natively).
    - Child table scalar fields are cast using `child_column_types`; cast failures spill safely to child `overflow_data`.

4. **No Document Rewriting on Schema Evolution**:
   - New fields route to `overflow_data` (read lazily via JSONB queries).
   - No need to rewrite entire parent document or create new tables.
   - Existing normalized columns remain unchanged and indexed.

5. **Session-Based Materialization**:
   - Decomposed child entities created once per ingestion session (in migrator worker).
   - Queries operate on materialized tables (not computed views or on-the-fly extractions).
   - Enables index strategy and execution plan optimization by PostgreSQL query planner.

### 7. Sources of Information
**Q: What documentation, research papers, or books helped guide your implementation?**

**A:** This implementation draws from:

   - [JSON Types & Operators](https://www.postgresql.org/docs/current/datatype-json.html)
   - [JSON Functions and Operators](https://www.postgresql.org/docs/9.5/functions-json.html)
   - [GIN Indexes](https://www.postgresql.org/docs/current/indexes-types.html) – JSONB query optimization.
---

## 3. Architecture & Constraints

| Constraint | Implementation Detail |
| :--- | :--- |
| **Bi-Temporal Timestamps** | We generate `sys_ingested_at` (Server Time via FastAPI `datetime.utcnow()`) upon receipt and preserve `t_stamp` (Client Time) from the JSON payload. Parent-table joins use surrogate key `id`, while temporal fields remain first-class for audit and time-travel style analysis. |
| **No Hardcoding** | Field mappings are learned dynamically. The `session_metadata` table stores evolved schema JSON with `__analysis_metadata.decomposition_plan` containing child entity relationships, field types, and storage decisions (SQL vs JSONB). Survives across API restarts via persistent PostgreSQL storage. |
| **Dynamic Schema Evolution** | New fields appearing in later records are routed to `overflow_data` JSONB column (for parent) or child `overflow_data` (for nested objects), avoiding table schema mutations. Existing SQL columns remain indexed and stable. |
| **Type Safety via Drift Detection** | Ingest-time type analysis routes type-stable fields to SQL and drift-prone fields to JSONB. For child entities, inferred `child_column_types` drives typed child-column DDL and payload casting; invalid casts are moved to child `overflow_data` instead of failing inserts. |
| **Inferred Join Infrastructure** | Query requests can optionally include `decomposition_plan` metadata. If present, `_build_inferred_joins_for_request()` detects child-prefixed field references (e.g., `comments.score`) and auto-generates LEFT JOIN SQL. No manual join logic required. Enables transparent query generation from JSON. |
| **Hybrid Storage** | Flat, type-stable fields → SQL columns (indexed). Optional/sparse/weakly-typed fields → JSONB `overflow_data` (GIN-indexed). Repeating arrays → normalized child SQL tables with typed scalar columns + JSONB overflow for nested child objects. |
| **Async Execution** | All database operations use SQLAlchemy async ORM + asyncpg driver. Worker pool processes child decomposition concurrently while main API serves requests. `/ingest` endpoint returns immediately; data migration happens asynchronously in background worker. |
| **Traceability & Auditability** | Parent records preserve `username`, `sys_ingested_at`, and `t_stamp`; child records preserve parent linkage through generated FK columns (for example `chiral_data_id`). Schema decisions are logged in metadata, and `verify_assignment.py` checks consistency across sessions. |

---

## 4. File Structure

### Core API & Services
*   `src/chiral/main.py`: **FastAPI application entry point.** Exposes:
    - `POST /ingest`: Route for accepting JSON records (async, returns 202 Accepted).
    - `POST /flush/{session_id}`: Signal to finalize and persist session.
    - `POST /query/translate`: Translate JSON query request to SQL (for inspection).
    - `POST /query/execute`: Translate + execute JSON query request (returns rows).
    - `GET /schema/{session_id}`: Retrieve evolved schema metadata.

### Schema & Database
*   `src/chiral/db/schema.py`: **Schema management & table creation.** Functions:
    - `get_decomposition_plan(analysis)`: Extracts decomposition metadata from `schema_json` envelope.
    - `materialize_decomposition_tables(...)`: Creates/extends child decomposition tables and typed child columns.
    - Maps inferred child analysis types to SQL types (INTEGER, DOUBLE PRECISION, BOOLEAN, TEXT, TIMESTAMP).

*   `src/chiral/db/connection.py`: **Database connection pooling & lifecycle.** Provides:
    - Async engine creation with asyncpg.
    - Connection pool configuration.
    - Context managers for session management.

*   `src/chiral/db/sessions.py`: **Session-scoped query execution.** Decorator:
    - `@with_session`: Injects SQLAlchemy async session into route handlers.

### Query Translation & Execution
*   `src/chiral/core/query_service.py`: **JSON-to-SQL translation & execution engine.** Core functions:
    - `translate_json_request(request)`: Maps JSON CRUD to `BuiltQuery` (sql + params).
    - `_build_inferred_joins_for_request(request, table_name)`: Auto-detects child field references; returns list of `InferredJoin` objects.
    - `execute_json_request(request, sql_session)`: End-to-end translate + execute; returns rows/affected_rows.

*   `src/chiral/db/query_builder.py`: **SQL generation for CRUD operations.** Classes:
    - `CrudQueryBuilder`: Builds SELECT/INSERT/UPDATE/DELETE SQL.
    - `InferredJoin` model: Captures source_field, child_table, parent_fk_column, child_column_types.
    - `build_select(...)`: If inferred_joins present, generates LEFT JOIN SQL with typed joined-child filter coercion and JSONB-safe numeric casting when needed.

### Data Ingestion & Migration
*   `src/chiral/worker/analyzer.py`: **Schema inference engine.** Functions:
    - `analyze_staging()`: Fetches first 100 records from staging; infers types and calculates stability metrics.
    - `infer_dominant_type(values)`: Computes type confidence as ratio of records with dominant type; if ANY record has different type, confidence < 1.0.
    - `calculate_field_stability_ratio(values, type_confidence)`: Combines null-presence ratio × type confidence.
    - `evaluate_jsonb_strategy(...)`: Routes fields to SQL (if type_confidence ≥ 0.8 AND stability ≥ 0.75 AND no nesting) or JSONB (type drift, low stability, nested structures).

*   `src/chiral/worker/migrator.py`: **Data migration & child decomposition.** Functions:
    - `process_staged_record(staging_row)`: Extracts child entities, creates normalized child records.
    - `_extract_decomposed_child_items(parent_item, decomposition_plan)`: Splits nested arrays into separate child entities.
    - `_build_child_insert_payload(parent_table, session_id, parent_id, entity, child_doc)`: Builds child insert payload using `child_column_types` for typed casting and overflow fallback on cast failure.
    - `cast_value(value, expected_type)`: Shared cast helper used for SQL-targeted value conversion.

### Utilities
*   `src/chiral/utils/clock.py`: Time utilities:
    - `get_server_timestamp()`: Returns UTC server timestamp for `sys_ingested_at`.

*   `src/chiral/utils/heuristics.py`: Helper functions:
    - `build_dynamic_child_key_spec()`: Generates child FK column name from parent table + entity name.
    - `normalize_identifier()`: Cleans SQL identifiers.

### Scripts & Testing
*   `feed_data.py`: **TA-provided simulation data consumer.** Streams from `/stream` endpoint, posts to `/ingest`, calls `/flush`.

*   `feed_data2.py`: **Enhanced nested-data feeder.** Generates 1000 records with:
    - Nested structures: `comments[]` (comment_id, text, score, is_flagged, meta JSONB) and `events[]` (event_id, event_type, amount, is_conversion, extra JSONB).
    - Posts to `/ingest` endpoint via async httpx.

*   `demo2.py`: **End-to-end demo & verification.** Showcases:
    - `show_schema_summary(conn)`: Prints compact schema (attributes, types, storage, table count, decomposition plan).
    - `show_example_queries(decomposition_plan)`: Executes 5 example queries (Q1–Q5) via `/query/execute`, including inferred JOIN typed child filter coercion.
    - Displays for each query: JSON input payload, SQL, params, sample rows (first 3).

*   `verify_assignment.py`: **Auditing & regression script.** Validates:
    - Schema consistency across sessions.
    - Parent-child relationships via FK constraints.
    - Data integrity (no orphaned child records).

*   `tests/test_query_phase6.py`: **Query translation & execution tests.**
    - Mixed SQL/JSONB filtering, inferred join generation, typed child filter coercion, type-safe numeric casting, end-to-end execute path.

*   `tests/test_migration_step4_helpers.py`: **Child decomposition & type coercion tests.**
    - Child entity extraction, overflow routing, typed child casting, and cast-failure overflow fallback.

*   `tests/test_config.py`: Configuration validation tests.


## License
MIT
