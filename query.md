# Query Guide (`/query/translate` and `/query/execute`)

This file documents how to query Chiral DB using JSON requests.

## Endpoints

- `POST /query/translate`
  - Returns generated SQL + bind params only.
- `POST /query/execute`
  - Returns SQL + params + data (`rows`) for reads.
  - For `create`, returns `affected_rows` plus contract fields: `mode`, `parent_id`, `child_insert_counts`.
  - For `update`/`delete`, returns `affected_rows`.

Base URL (local): `http://127.0.0.1:8000`

---

## Request Shape

```json
{
  "operation": "read | create | update | delete",
  "table": "chiral_data",
  "select": ["username", "comments.score"],
  "filters": [
    {"field": "session_id", "op": "eq", "value": "session_assignment_2"}
  ],
  "payload": {},
  "updates": {},
  "limit": 10,
  "offset": 0
}
```

Use only the fields needed for the selected operation.

### Decomposition Plan Resolution (important)

- You do **not** need to send `decomposition_plan` in query requests.
- For `read` / `update` / `delete`, query execution auto-loads the decomposition plan from `session_metadata.schema_json.__analysis_metadata__.decomposition_plan` using `session_id` found in the request.
- If a request already contains `decomposition_plan`, that value is used directly.
- If no plan is found for the session, child-prefixed fields (for example `comments.comment_id`) cannot be resolved.

To allow child-field filters without explicitly passing `decomposition_plan`, include `session_id` in request filters (or top-level `session_id`).

---

## Filter Operators

Supported operators:

- `eq`
- `ne`
- `gt`
- `gte`
- `lt`
- `lte`
- `contains` (for `overflow_data.<key>` paths)

Example filter item:

```json
{"field": "temperature", "op": "gt", "value": 25}
```

---

## 1) Read Queries

### A. Read from parent SQL columns

```json
{
  "operation": "read",
  "table": "chiral_data",
  "select": ["username", "sys_ingested_at"],
  "filters": [
    {"field": "session_id", "op": "eq", "value": "session_assignment_2"}
  ],
  "limit": 5
}
```

### B. Read parent JSONB key

```json
{
  "operation": "read",
  "table": "chiral_data",
  "select": ["username", "overflow_data.metadata"],
  "filters": [
    {"field": "session_id", "op": "eq", "value": "session_assignment_2"}
  ],
  "limit": 5
}
```

### C. Join-inferred child projection (automatic)

```json
{
  "operation": "read",
  "table": "chiral_data",
  "select": ["username", "comments.score", "comments.is_flagged"],
  "filters": [
    {"field": "session_id", "op": "eq", "value": "session_assignment_2"}
  ],
  "limit": 5
}
```

### D. Join-inferred child typed filters (automatic)

```json
{
  "operation": "read",
  "table": "chiral_data",
  "select": ["username", "comments.score", "comments.is_flagged"],
  "filters": [
    {"field": "session_id", "op": "eq", "value": "session_assignment_2"},
    {"field": "comments.score", "op": "gte", "value": "0.5"},
    {"field": "comments.is_flagged", "op": "eq", "value": "true"}
  ],
  "limit": 10
}
```

Note:
- Child typed filter values can be provided as strings (for example, `"0.5"`, `"true"`); query logic coerces them using inferred child column types.

### E. Child JSONB nested filter path

```json
{
  "operation": "read",
  "table": "chiral_data",
  "select": ["username", "comments.overflow_data.meta"],
  "filters": [
    {"field": "session_id", "op": "eq", "value": "session_assignment_2"},
    {"field": "comments.overflow_data.score", "op": "gte", "value": 0.5}
  ],
  "limit": 10
}
```

---

## 2) Create Query

```json
{
  "operation": "create",
  "table": "chiral_data",
  "payload": {
    "session_id": "session_assignment_2",
    "username": "new_user",
    "sys_ingested_at": 1742643301.25,
    "t_stamp": 1742643301.25,
    "overflow_data": "{}"
  }
}
```

Create execute response (`mode: "migrated_sync"`):

```json
{
  "sql": "INSERT INTO ...",
  "params": {"session_id": "session_assignment_2", "username": "new_user"},
  "affected_rows": 1,
  "mode": "migrated_sync",
  "parent_id": null,
  "child_insert_counts": {}
}
```

Queued create response (`mode: "queued_async"`):

```json
{
  "sql": null,
  "params": {},
  "affected_rows": 0,
  "mode": "queued_async",
  "parent_id": null,
  "child_insert_counts": {},
  "queue_reason": "analysis_timeout",
  "fallback_trigger": "metadata_resolution",
  "worker_triggered": false,
  "staging_count": 3
}
```

Nested create behavior:

- If nested payload fields are present and decomposition entities are available in metadata, `/query/execute` performs synchronous parent+child migration and returns `mode: "migrated_sync"` with populated `parent_id` and `child_insert_counts`.
- If nested payload fields are present but decomposition entities are not available yet, request falls back to staging+worker and returns `mode: "queued_async"`.
- Deterministic fallback triggers also return `mode: "queued_async"` with `queue_reason` and `fallback_trigger`:
  - `analysis_timeout` (`fallback_trigger: "metadata_resolution"`)
  - `metadata_lock_contention` (`fallback_trigger: "metadata_resolution"`)
  - `ddl_conflict` / `retriable_insert_conflict` (`fallback_trigger: "sync_migration"` or `"flat_insert"`)

Rollout safety flag:

- Set `CREATE_ORCHESTRATION_ENABLED=false` to bypass create orchestration and use legacy direct create behavior.
- Default is enabled (`true`).

Validation failure shape for `create`:

```json
{
  "detail": {
    "mode": "failed_validation",
    "error": "create operation requires object payload"
  }
}
```

---

## 3) Update Query

```json
{
  "operation": "update",
  "table": "chiral_data",
  "updates": {
    "username": "renamed_user"
  },
  "filters": [
    {"field": "session_id", "op": "eq", "value": "session_assignment_2"},
    {"field": "username", "op": "eq", "value": "new_user"}
  ]
}
```

### Update with child filter (no client plan required)

```json
{
  "operation": "update",
  "table": "chiral_data",
  "updates": {
    "username": "renamed_user"
  },
  "filters": [
    {"field": "session_id", "op": "eq", "value": "session_assignment_2"},
    {"field": "comments.comment_id", "op": "eq", "value": 30}
  ]
}
```

Note:
- For `update`/`delete`, child-field filters are translated as `EXISTS (...)` subqueries against inferred child tables, not as top-level joins.

---

## 4) Delete Query

```json
{
  "operation": "delete",
  "table": "chiral_data",
  "filters": [
    {"field": "session_id", "op": "eq", "value": "session_assignment_2"},
    {"field": "username", "op": "eq", "value": "renamed_user"}
  ]
}
```

### Delete with child filter (your use case)

```json
{
  "session_id": "session_assignment_2",
  "operation": "delete",
  "table": "chiral_data",
  "filters": [
    {"field": "session_id", "op": "eq", "value": "session_assignment_2"},
    {"field": "comments.comment_id", "op": "eq", "value": 30}
  ]
}
```

---

## cURL Examples

### Translate only

```bash
curl -X POST http://127.0.0.1:8000/query/translate \
  -H "Content-Type: application/json" \
  -d '{
    "operation": "read",
    "table": "chiral_data",
    "select": ["username", "comments.score"],
    "filters": [{"field": "session_id", "op": "eq", "value": "session_assignment_2"}],
    "limit": 5
  }'
```

### Translate + execute

```bash
curl -X POST http://127.0.0.1:8000/query/execute \
  -H "Content-Type: application/json" \
  -d '{
    "operation": "read",
    "table": "chiral_data",
    "select": ["username", "comments.score", "comments.is_flagged"],
    "filters": [
      {"field": "session_id", "op": "eq", "value": "session_assignment_2"},
      {"field": "comments.score", "op": "gte", "value": "0.5"}
    ],
    "limit": 10
  }'
```

---

## Common Mistakes

- Using `contains` on non-JSONB fields.
- Using non-numeric filter values for numeric range operations on JSONB paths.
- Misspelling filter operator names (`gte` not `=>`, `eq` not `=`).
- Omitting `session_id` when relying on metadata auto-hydration for child-prefixed fields.
