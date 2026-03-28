# Assignment 2: Autonomous Normalization & CRUD Engine

**Course:** CS 432 – Databases (Semester II, 2025 - 2026)
**Instructor:** Dr. Yogesh K. Meena

**Team Members:**
* Deep Buha (24110082) - `24110082@iitgn.ac.in`
* Devansh Lodha (23110091) - `devansh.lodha@iitgn.ac.in`
* Laxmidhar Panda (24110185) - `24110185@iitgn.ac.in`
* Yuvraj Rathod (24110293) - `yuvraj.rathod@iitgn.ac.in`
* Viraj Solanki (24110348) - `viraj.solanki@iitgn.ac.in`

---

## 1. Overview

This repository implements a session-scoped, self-adaptive database framework. It dynamically infers schemas from raw JSON streams, normalizes repeating entities into strictly typed SQL tables, routes sparse or drift-prone fields to GIN-indexed `JSONB` document stores, and exposes a fully abstracted logical CRUD query API.

By replacing the physical SQL/MongoDB split with a unified PostgreSQL + JSONB architecture, the system guarantees ACID compliance, eliminates distributed transaction overhead, and allows the native query planner to optimize joins between normalized tables and schemaless documents.

---

## 2. Steps to Execute the Code

### Prerequisites
* **Docker Engine** (Must be running for PostgreSQL container)
* **Just** (Command runner): `brew install just` / `apt install just` / `choco install just`
* **uv** (Python package manager): `curl -LsSf https://astral.sh/uv/install.sh | sh`

### Setup & Configuration
Clone the repository and set up the environment variables:
```bash
cp .env.example .env
```
*(If `.env.example` is unavailable, manually create `.env` with `POSTGRES_USER=chiral`, `POSTGRES_PASSWORD=chiral`, `POSTGRES_DB=chiral_db`, `POSTGRES_PORT=5432`)*

### Run the End-to-End Demo (Evaluator Mode)
The following command completely resets the environment, spins up the database, launches the FastAPI server, streams 1,000 highly nested records via the TA simulation, waits for the background normalization workers, and executes a 5-query CRUD showcase proving the inferred join logic.

```bash
just demo2
```

### Run the Webapp Dashboard (Docker)
To build and run the dashboard in a container and expose it locally:

```bash
just webapp
```

Open: **http://localhost:5173**

To stop only the dashboard container:

```bash
just webapp-stop
```

**Expected Output:**
1. Record ingestion logs.
2. A schema summary demonstrating physical table generation (e.g., `chiral_data`, `chiral_data_comments`, `chiral_data_events`).
3. Five executed queries displaying the logical JSON input, the compiled physical SQL (with `LEFT JOIN` and `JSONB` operators), and the returned record subset.

### Teardown
To terminate the background API processes and destroy the database containers/volumes:
```bash
just stop
just down
```

---

## 3. Project Structure

```text
chiral-db/
├── demo2.py                          # E2E evaluation script (schema tracking & query generation showcase)
├── feed_data2.py                     # High-throughput nested JSON simulator
├── Justfile                          # Automation commands (setup, demo, teardown)
├── src/chiral/
│   ├── main.py                       # FastAPI entry point (/ingest, /query/execute)
│   ├── core/
│   │   ├── ingestion.py              # Bi-temporal timestamping & JSONB staging buffer
│   │   └── query_service.py          # AST compiler: Logical JSON to Physical SQL + Inferred Joins
│   ├── db/
│   │   ├── query_builder.py          # Dynamic string building for SELECT, INSERT, UPDATE, DELETE
│   │   ├── schema.py                 # DDL Materialization for dynamic parent and child tables
│   │   └── metadata_store.py         # Persistence for decomposition_plan and schema drift events
│   ├── domain/
│   │   ├── key_policy.py             # Deterministic PK/FK generation and constraint logic
│   │   └── normalization.py          # Array homogeneity scanning & JSONB routing heuristics
│   └── worker/
│       ├── analyzer.py               # Statistical observation phase (type confidence, sparsity)
│       └── migrator.py               # Data splitting, zero-casting SQL inserts, JSONB overflow routing
└── tests/                            # Pytest suite targeting AST translation and normalization logic
```

---

## 4. Key Architectural Implementations

* **Zero-Casting Ingestion:** The system enforces strict type compliance (`type_confidence = 1.0`). During ingestion, data is never mutated to fit a schema. If a field deviates from its historical type, the system executes an automated drift-event, migrating the entire physical SQL column into the `overflow_data` JSONB column dynamically.
* **Autonomous Normalization:** The `analyzer` statistically evaluates arrays of objects for `occurrence_ratio` and `homogeneity_ratio`. Valid repeating entities are extracted and materialized into distinct SQL tables linked via `ON DELETE CASCADE` foreign keys.
* **Inferred Joins (CRUD Engine):** The query API (`/query/execute`) completely abstracts the relational structure. If a user queries `{"field": "comments.score", "op": "gt", "value": 50}`, the AST compiler intercepts the `comments` prefix, looks up the `decomposition_plan`, and automatically injects a `LEFT JOIN` to the `chiral_data_comments` table with type-safe filter coercion.
* **Synchronous Insert Orchestration:** While raw data is typically staged and processed asynchronously, explicitly nested `create` payloads sent to the query engine trigger a synchronous decomposition. The engine maps the payload against the known schema, issues the parent insert, retrieves the `RETURNING id`, and executes the child inserts atomically.
