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
*   **Just** (`brew install just` or `apt install just`)
*   **uv** (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

### Setup Configuration
Before running the demo, create the environment file:
```bash
cp .env.example .env
# OR manually create .env with:
# POSTGRES_USER=chiral
# POSTGRES_PASSWORD=chiral
# POSTGRES_DB=chiral_db
# MONGO_INITDB_ROOT_USERNAME=admin
# MONGO_INITDB_ROOT_PASSWORD=admin
```

### Run the Demo
The following command will wipe previous data, start the containers, launch the API, run the TA's simulation server, ingest 1000 records, and automatically print a **Verification Report**.

```bash
just demo
```
*   **PostgreSQL:** Will show a record count and a list of dynamically learned columns (e.g., `altitude`, `steps`, `battery`).
*   **MongoDB:** Will show a document count representing the overflow/audit trail.
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
**Q: How did you resolve type naming ambiguities? What rules did you follow to ensure they didn’t create duplicate columns?**

**A:**
*   **Attribute Naming:** Per the instructor's specific clarification for this assignment, we treat casing differences (e.g., `ip` vs `IP` vs `IpAddress`) as **distinct logical attributes**. We do not normalize them into a single column (e.g., `ip_address`). This preserves the exact semantics of the incoming stream.
*   **Type Normalization:** We map disparate Python/JSON value types to standard SQL ISO types:
    *   `int` $\rightarrow$ `INTEGER`
    *   `float` $\rightarrow$ `DOUBLE PRECISION`
    *   `bool` $\rightarrow$ `BOOLEAN`
    *   `str` $\rightarrow$ `TEXT`
*   **Duplicate Prevention:** We enforce a strict **1-to-1 mapping** between a logical attribute key and a physical column. We do *not* create versioned columns (e.g., `age_int`, `age_string`). If a specific attribute key (e.g., `age`) changes type mid-stream (Type Drift), we do not create a second column; instead, we migrate the attribute to MongoDB (see Question 5).

### 2. Placement Heuristics
**Q: What specific thresholds (e.g., frequency %) were used to decide between SQL and MongoDB?**

**A:** We utilize **Shannon Entropy ($H$)** calculated during the analysis phase (first 100 records).
*   **Wheat (SQL):** Fields with $H \approx 0$ (High Type Stability) AND primitive types (`int`, `float`, `bool`).
*   **Chaff (MongoDB):** Fields with $H > 0$ (Mixed Types) OR Complex Structure (`dict`, `list`).
*   **Logic:** Nested structures are *always* routed to MongoDB. Flat fields are routed to SQL only if they maintain type consistency.

### 3. Uniqueness
**Q: How did you identify which fields should be marked as UNIQUE in SQL versus those that are just frequent?**

**A:**
*   **Detection:** During the initial analysis phase (first 100 records), we check if `count(distinct values) == count(total values)`. If true, the column is created with a `UNIQUE` constraint in PostgreSQL.
*   **Adaptation:** If a subsequent insert violates this constraint (due to sample bias), we catch the `IntegrityError`, dynamically **DROP** the unique constraint on that column, and retry the insert. This ensures the system prefers availability over strict constraint enforcement for inferred schemas.

### 4. Value Interpretation
**Q: How did your system differentiate between a string representing an IP ("1.2.3.4") and a float (1.2)?**

**A:** We employ strict Python type inference.
*   The system attempts to cast values in this order: `int` -> `float` -> `bool`.
*   `"1.2"` successfully casts to `float`.
*   `"1.2.3.4"` raises a `ValueError` when casting to `float`, so it falls back to `str`.
*   This inferred type is stored in the metadata map. If a column is defined as `FLOAT` and a `str` arrives later, it triggers a Drift Event.

### 5. Mixed Data Handling
**Q: How did your system react when a field’s data type changed mid-stream?**

**A:** This is handled via **"Retroactive Drift Migration"**.
1.  The system attempts to write to the SQL column (Optimistic Write).
2.  If a `DataError` (Type Mismatch) occurs (e.g., string in integer column), the transaction is rolled back.
3.  The system flags the column as "Drifted".
4.  **Action:** The *entire* history of that column is migrated from SQL to the MongoDB `permanent` collection, and the metadata map is updated to route all future values for that column to MongoDB. This ensures zero data loss.

---

## 3. Architecture & Constraints

| Constraint | Implementation Detail |
| :--- | :--- |
| **Bi-Temporal Timestamps** | We generate `sys_ingested_at` (Server Time) upon receipt and preserve `t_stamp` (Client Time) from the JSON payload. `sys_ingested_at` serves as the join key. |
| **No Hardcoding** | Field mappings are learned dynamically. The `session_metadata` table stores the evolved schema JSON. |
| **Persistence** | All decisions are durable. The `verify_assignment.py` script proves that schema definitions survive across restarts. |
| **Traceability** | The `username` and `sys_ingested_at` fields are replicated in both SQL and MongoDB partitions to allow logical reconstruction (JOINs) of the object. |

---

## 4. File Structure

*   `src/chiral/main.py`: API Entry point.
*   `src/chiral/worker/analyzer.py`: Calculates Entropy and infers schema.
*   `src/chiral/worker/migrator.py`: Handles data movement and retroactive drift handling.
*   `feed_data.py`: Bridge script that consumes the TA's simulation server and feeds the Chiral API.
*   `verify_assignment.py`: Auditing script that generates the final report.

## License
MIT
