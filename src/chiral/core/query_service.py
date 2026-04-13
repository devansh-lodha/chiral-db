# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""JSON request to SQL/JSONB query translation service."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from chiral.core.ingestion import ingest_data
from chiral.core.orchestrator import trigger_worker
from chiral.db.metadata_store import (
    apply_decomposition_plan_to_metadata,
    apply_drift_to_metadata,
    bounded_append_events,
    load_metadata_snapshot,
)
from chiral.db.query_builder import BuiltQuery, CrudQueryBuilder, InferredJoin
from chiral.db.schema import init_metadata_table
from chiral.db.sessions import session
from chiral.domain.key_policy import build_dynamic_child_key_spec
from chiral.domain.normalization import detect_repeating_entities
from chiral.worker.migrator import migrate_single_create_payload

logger = logging.getLogger(__name__)
SYSTEM_CREATE_FIELDS = {"id", "session_id", "username", "sys_ingested_at", "t_stamp", "overflow_data"}

CreateExecutionMode = Literal["migrated_sync", "queued_async", "failed_validation"]


class CreateExecutionValidationError(ValueError):
    """Typed validation error for create execution contract."""


def _build_create_execution_response(
    *,
    built: BuiltQuery | None,
    affected_rows: int,
    mode: CreateExecutionMode,
    parent_id: int | None = None,
    child_insert_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    return {
        "sql": built.sql if built else None,
        "params": built.params if built else {},
        "affected_rows": affected_rows,
        "mode": mode,
        "parent_id": parent_id,
        "child_insert_counts": child_insert_counts or {},
    }


def _build_queued_async_response(
    *,
    queue_reason: str,
    ingest_result: dict[str, Any],
    fallback_trigger: str | None = None,
) -> dict[str, Any]:
    response = {
        "sql": None,
        "params": {},
        "affected_rows": 0,
        "mode": "queued_async",
        "parent_id": None,
        "child_insert_counts": {},
        "queue_reason": queue_reason,
        "worker_triggered": bool(ingest_result.get("worker_triggered", False)),
        "staging_count": int(ingest_result.get("count", 0) or 0),
    }
    if fallback_trigger:
        response["fallback_trigger"] = fallback_trigger
    return response


def _classify_create_fallback_reason(exc: Exception) -> str | None:
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return "analysis_timeout"

    if isinstance(exc, OperationalError):
        message = str(exc).lower()
        if "could not obtain lock" in message or "lock timeout" in message or "deadlock" in message:
            rtv = "metadata_lock_contention"
        else:
            rtv = "sql_operational_error"
        return rtv

    if isinstance(exc, IntegrityError):
        return "retriable_insert_conflict"

    if isinstance(exc, SQLAlchemyError):
        message = str(exc).lower()
        return "ddl_conflict" if "already exists" in message or "duplicate" in message else "sqlalchemy_error"

    return None


def _is_create_orchestration_enabled() -> bool:
    return os.getenv("CREATE_ORCHESTRATION_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}


async def _execute_create_request_legacy(
    request: dict[str, Any],
    sql_session: AsyncSession,
) -> dict[str, Any]:
    payload = request.get("payload", {})
    if not isinstance(payload, dict):
        msg = "create operation requires object payload"
        raise CreateExecutionValidationError(msg)

    session_id = _extract_session_id(request)
    if not session_id:
        msg = "create operation requires session_id in request, payload, or filters"
        raise CreateExecutionValidationError(msg)

    hydrated_request = dict(request)
    hydrated_request["session_id"] = session_id

    try:
        built = translate_json_request(hydrated_request)
    except ValueError as exc:
        msg = str(exc) or "Invalid create request"
        raise CreateExecutionValidationError(msg) from exc

    try:
        begin_nested_cm = sql_session.begin_nested()
    except AttributeError:
        result = await sql_session.execute(text(built.sql), built.params)
    else:
        async with begin_nested_cm:
            result = await sql_session.execute(text(built.sql), built.params)

    affected_rows = int(getattr(result, "rowcount", 0) or 0)
    return _build_create_execution_response(
        built=built,
        affected_rows=affected_rows,
        mode="migrated_sync",
        parent_id=None,
        child_insert_counts={},
    )


async def _enqueue_create_for_async_processing(
    *,
    payload: dict[str, Any],
    session_id: str,
    queue_reason: str,
    fallback_trigger: str | None = None,
) -> dict[str, Any]:
    ingest_result = await ingest_data(data=dict(payload), session_id=session_id)
    if ingest_result.get("worker_triggered"):
        incremental = bool(ingest_result.get("incremental", False))
        task = asyncio.create_task(trigger_worker(session_id, incremental=incremental))

        def _handle_task_result(task: asyncio.Task) -> None:
            try:
                task.result()
            except Exception as e:
                logger.exception("Background task failed", exc_info=e)

        task.add_done_callback(_handle_task_result)

    logger.warning(
        "Create request queued for async processing: session_id=%s queue_reason=%s fallback_trigger=%s",
        session_id,
        queue_reason,
        fallback_trigger,
    )
    return _build_queued_async_response(
        queue_reason=queue_reason,
        ingest_result=ingest_result,
        fallback_trigger=fallback_trigger,
    )


def _payload_contains_nested_data(payload: dict[str, Any]) -> bool:
    for key, value in payload.items():
        if key == "overflow_data":
            continue
        if isinstance(value, (dict, list)):
            return True
    return False


def _decide_create_execution_mode(
    payload: dict[str, Any], decomposition_plan: dict[str, Any]
) -> tuple[CreateExecutionMode, str]:
    entities = decomposition_plan.get("entities", [])
    has_entities = isinstance(entities, list) and len(entities) > 0

    if _payload_contains_nested_data(payload) and not has_entities:
        return "queued_async", "nested_payload_without_materialized_decomposition_plan"

    if _payload_contains_nested_data(payload) and has_entities:
        return "migrated_sync", "nested_payload_sync_decomposition"

    return "migrated_sync", "flat_payload_direct_insert"


async def _initialize_session_metadata_for_create(sql_session: AsyncSession, session_id: str) -> None:
    await init_metadata_table(sql_session)
    await sql_session.execute(
        text(
            """
            INSERT INTO session_metadata (session_id, record_count, schema_version, drift_events, safety_events, migration_metrics)
            VALUES (:sid, 0, 1, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb)
            ON CONFLICT (session_id) DO NOTHING
            """
        ),
        {"sid": session_id},
    )


async def _lock_session_metadata_row(sql_session: AsyncSession, session_id: str) -> None:
    await sql_session.execute(
        text("SELECT session_id FROM session_metadata WHERE session_id = :sid FOR UPDATE"),
        {"sid": session_id},
    )


def _normalize_decomposition_plan(plan: dict[str, Any], *, parent_table: str) -> dict[str, Any]:
    entities = plan.get("entities", []) if isinstance(plan, dict) else []
    if not isinstance(entities, list):
        entities = []

    normalized_entities = [entity for entity in entities if isinstance(entity, dict)]
    return {
        "version": int(plan.get("version", 1) or 1) if isinstance(plan, dict) else 1,
        "parent_table": str(plan.get("parent_table", parent_table)) if isinstance(plan, dict) else parent_table,
        "entities": normalized_entities,
    }


def _extract_source_fields_from_plan(plan: dict[str, Any]) -> set[str]:
    entities = plan.get("entities", []) if isinstance(plan, dict) else []
    if not isinstance(entities, list):
        return set()

    source_fields: set[str] = set()
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        source_field = entity.get("source_field")
        if isinstance(source_field, str) and source_field:
            source_fields.add(source_field)
    return source_fields


def _should_attempt_create_plan_inference(payload: dict[str, Any], existing_plan: dict[str, Any]) -> bool:
    nested_source_fields = {key for key, value in payload.items() if isinstance(value, (dict, list))}
    if not nested_source_fields:
        return False

    existing_source_fields = _extract_source_fields_from_plan(existing_plan)
    return not nested_source_fields.issubset(existing_source_fields)


async def _load_staging_docs_for_create_analysis(
    sql_session: AsyncSession,
    *,
    session_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []

    result = await sql_session.execute(
        text("SELECT data FROM staging_data WHERE session_id = :sid ORDER BY id DESC LIMIT :lim"),
        {"sid": session_id, "lim": limit},
    )

    docs: list[dict[str, Any]] = []
    for row in result.fetchall():
        raw = row[0]
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                continue
        if isinstance(raw, dict):
            docs.append(raw)
    return docs


def _build_decomposition_plan_from_docs(docs: list[dict[str, Any]], *, parent_table: str) -> dict[str, Any]:
    repeating_entities = detect_repeating_entities(
        docs,
        parent_table=parent_table,
        min_occurrence_ratio=float(os.getenv("DECOMPOSITION_MIN_OCCURRENCE_RATIO", "0.2")),
        min_homogeneity_ratio=float(os.getenv("DECOMPOSITION_MIN_HOMOGENEITY_RATIO", "0.7")),
        min_average_cardinality=float(os.getenv("DECOMPOSITION_MIN_AVG_CARDINALITY", "1.0")),
        stable_key_ratio_threshold=float(os.getenv("DECOMPOSITION_STABLE_KEY_RATIO_THRESHOLD", "0.6")),
    )

    return {
        "version": 1,
        "parent_table": parent_table,
        "entities": [
            {
                "source_field": entity.source_field,
                "child_table": entity.child_table,
                "relationship": entity.relationship,
                "occurrence_ratio": entity.occurrence_ratio,
                "homogeneity_ratio": entity.homogeneity_ratio,
                "average_cardinality": entity.average_cardinality,
                "child_columns": entity.child_columns,
                "child_column_types": entity.child_column_types,
                "reason": entity.reason,
            }
            for entity in repeating_entities
        ],
    }


async def _infer_decomposition_plan_for_create(
    sql_session: AsyncSession,
    *,
    session_id: str,
    payload: dict[str, Any],
    parent_table: str,
) -> dict[str, Any]:
    max_docs = max(1, int(os.getenv("CREATE_ANALYSIS_MAX_DOCS", "8")))
    timeout_ms = max(25, int(os.getenv("CREATE_ANALYSIS_TIMEOUT_MS", "300")))

    staged_docs = await _load_staging_docs_for_create_analysis(
        sql_session,
        session_id=session_id,
        limit=max(0, max_docs - 1),
    )
    docs = [dict(payload), *staged_docs]
    if len(docs) > max_docs:
        docs = docs[:max_docs]

    inferred = await asyncio.wait_for(
        asyncio.to_thread(_build_decomposition_plan_from_docs, docs, parent_table=parent_table),
        timeout=timeout_ms / 1000,
    )
    return _normalize_decomposition_plan(inferred, parent_table=parent_table)


def _merge_decomposition_plans(
    existing_plan: dict[str, Any], inferred_plan: dict[str, Any], *, parent_table: str
) -> dict[str, Any]:
    normalized_existing = _normalize_decomposition_plan(existing_plan, parent_table=parent_table)
    normalized_inferred = _normalize_decomposition_plan(inferred_plan, parent_table=parent_table)

    merged_by_source: dict[str, dict[str, Any]] = {}
    for entity in normalized_existing["entities"]:
        source_field = entity.get("source_field")
        if isinstance(source_field, str) and source_field:
            merged_by_source[source_field] = entity

    for entity in normalized_inferred["entities"]:
        source_field = entity.get("source_field")
        if isinstance(source_field, str) and source_field:
            merged_by_source[source_field] = entity

    return {
        "version": max(int(normalized_existing.get("version", 1)), int(normalized_inferred.get("version", 1))),
        "parent_table": parent_table,
        "entities": list(merged_by_source.values()),
    }


def _detect_payload_drift_columns(schema: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    drifted_columns: list[str] = []

    def _raise_value_error() -> None:
        raise ValueError

    for key, value in payload.items():
        if key in SYSTEM_CREATE_FIELDS:
            continue
        if isinstance(value, (dict, list)):
            continue

        meta = schema.get(key)
        if not isinstance(meta, dict):
            continue
        if str(meta.get("target", "")).lower() != "sql":
            continue

        expected_type = str(meta.get("type", "str")).strip().lower()
        try:
            if expected_type == "int":
                if isinstance(value, bool):
                    _raise_value_error()
                int(value)
            elif expected_type == "float":
                if isinstance(value, bool):
                    _raise_value_error()
                float(value)
            elif expected_type == "bool":
                if isinstance(value, str):
                    lowered = value.strip().lower()
                    if lowered not in {"true", "false", "1", "0", "yes", "no", "y", "n"}:
                        _raise_value_error()
                elif not isinstance(value, bool):
                    _raise_value_error()
            else:
                str(value)
        except (ValueError, TypeError):
            drifted_columns.append(key)

    return drifted_columns


async def _resolve_create_metadata_and_plan(
    sql_session: AsyncSession,
    *,
    session_id: str,
    payload: dict[str, Any],
    table_name: str,
    current_plan: dict[str, Any],
) -> dict[str, Any]:
    resolved_plan = _normalize_decomposition_plan(current_plan, parent_table=table_name)

    if _should_attempt_create_plan_inference(payload, resolved_plan):
        inferred_plan = await _infer_decomposition_plan_for_create(
            sql_session,
            session_id=session_id,
            payload=payload,
            parent_table=table_name,
        )
        resolved_plan = _merge_decomposition_plans(resolved_plan, inferred_plan, parent_table=table_name)

    snapshot = await load_metadata_snapshot(sql_session, session_id)
    existing_schema = snapshot.schema if snapshot else {}
    existing_drift_events = snapshot.drift_events if snapshot else []
    base_schema_version = snapshot.schema_version if snapshot else 1

    updated_schema, updated_drift_events, schema_version_increment = apply_decomposition_plan_to_metadata(
        schema=existing_schema,
        drift_events=existing_drift_events,
        decomposition_plan=resolved_plan,
        previous_decomposition_plan=current_plan,
    )

    drift_columns = _detect_payload_drift_columns(updated_schema, payload)
    drift_increment = 0
    for column_name in drift_columns:
        updated_schema, updated_drift_events, increment = apply_drift_to_metadata(
            updated_schema,
            updated_drift_events,
            column_name,
        )
        drift_increment += increment

    total_version_increment = schema_version_increment + drift_increment
    if total_version_increment > 0:
        new_events = updated_drift_events[len(existing_drift_events) :]
        max_drift_events = max(1, int(os.getenv("GUARDRAIL_MAX_DRIFT_EVENTS_PER_SESSION", "200")))
        bounded_drift_events = bounded_append_events(existing_drift_events, new_events, max_drift_events)
        await sql_session.execute(
            text(
                "UPDATE session_metadata SET schema_json = :schema, schema_version = :schema_version, "
                "drift_events = CAST(:drift_events AS jsonb) WHERE session_id = :sid"
            ),
            {
                "sid": session_id,
                "schema": json.dumps(updated_schema),
                "schema_version": base_schema_version + total_version_increment,
                "drift_events": json.dumps(bounded_drift_events),
            },
        )

    return resolved_plan


async def _execute_create_request(
    request: dict[str, Any],
    sql_session: AsyncSession,
) -> dict[str, Any]:
    if not _is_create_orchestration_enabled():
        logger.warning("Create orchestration disabled by CREATE_ORCHESTRATION_ENABLED; using legacy direct create path")
        return await _execute_create_request_legacy(request=request, sql_session=sql_session)

    payload = request.get("payload", {})
    if not isinstance(payload, dict):
        msg = "create operation requires object payload"
        raise CreateExecutionValidationError(msg)

    session_id = _extract_session_id(request)
    if not session_id:
        msg = "create operation requires session_id in request, payload, or filters"
        raise CreateExecutionValidationError(msg)

    await _initialize_session_metadata_for_create(sql_session, session_id)
    await _lock_session_metadata_row(sql_session, session_id)

    table_name = str(request.get("table", "chiral_data"))
    decomposition_plan = await _load_decomposition_plan_from_metadata(sql_session, session_id)
    try:
        try:
            begin_nested_cm = sql_session.begin_nested()
        except AttributeError:
            decomposition_plan = await _resolve_create_metadata_and_plan(
                sql_session,
                session_id=session_id,
                payload=payload,
                table_name=table_name,
                current_plan=decomposition_plan,
            )
        else:
            async with begin_nested_cm:
                decomposition_plan = await _resolve_create_metadata_and_plan(
                    sql_session,
                    session_id=session_id,
                    payload=payload,
                    table_name=table_name,
                    current_plan=decomposition_plan,
                )
    except Exception as exc:
        fallback_reason = _classify_create_fallback_reason(exc)
        if fallback_reason is not None:
            return await _enqueue_create_for_async_processing(
                payload=payload,
                session_id=session_id,
                queue_reason=fallback_reason,
                fallback_trigger="metadata_resolution",
            )
        raise

    mode, mode_reason = _decide_create_execution_mode(payload, decomposition_plan)

    if mode == "queued_async":
        return await _enqueue_create_for_async_processing(
            payload=payload,
            session_id=session_id,
            queue_reason=mode_reason,
        )
    if _payload_contains_nested_data(payload):
        max_field_bytes = max(128, int(os.getenv("GUARDRAIL_MAX_FIELD_BYTES", "65536")))
        max_nesting_depth = max(1, int(os.getenv("GUARDRAIL_MAX_NESTING_DEPTH", "8")))
        try:
            try:
                begin_nested_cm = sql_session.begin_nested()
            except AttributeError:
                create_result = await migrate_single_create_payload(
                    payload=payload,
                    session_id=session_id,
                    decomposition_plan=decomposition_plan,
                    table_name=table_name,
                    sql_session=sql_session,
                    max_field_bytes=max_field_bytes,
                    max_nesting_depth=max_nesting_depth,
                )
            else:
                async with begin_nested_cm:
                    create_result = await migrate_single_create_payload(
                        payload=payload,
                        session_id=session_id,
                        decomposition_plan=decomposition_plan,
                        table_name=table_name,
                        sql_session=sql_session,
                        max_field_bytes=max_field_bytes,
                        max_nesting_depth=max_nesting_depth,
                    )
        except Exception as exc:
            fallback_reason = _classify_create_fallback_reason(exc)
            if fallback_reason is not None:
                return await _enqueue_create_for_async_processing(
                    payload=payload,
                    session_id=session_id,
                    queue_reason=fallback_reason,
                    fallback_trigger="sync_migration",
                )
            raise

        await sql_session.execute(
            text(
                "UPDATE session_metadata SET status = 'migrated' WHERE session_id = :sid "
                "AND status IN ('collecting', 'analyzing', 'migrating_incremental')"
            ),
            {"sid": session_id},
        )

        raw_parent_id = create_result.get("parent_id")
        parent_id: int | None
        if isinstance(raw_parent_id, int):
            parent_id = raw_parent_id
        elif isinstance(raw_parent_id, str) and raw_parent_id.strip():
            parent_id = int(raw_parent_id)
        else:
            parent_id = None

        return _build_create_execution_response(
            built=None,
            affected_rows=1,
            mode="migrated_sync",
            parent_id=parent_id,
            child_insert_counts=create_result.get("child_insert_counts", {}),
        )

    hydrated_request = dict(request)
    hydrated_request["session_id"] = session_id
    if isinstance(decomposition_plan.get("entities"), list) and decomposition_plan.get("entities"):
        hydrated_request["decomposition_plan"] = decomposition_plan

    try:
        built = translate_json_request(hydrated_request)
    except ValueError as exc:
        msg = str(exc) or "Invalid create request"
        raise CreateExecutionValidationError(msg) from exc

    try:
        try:
            begin_nested_cm = sql_session.begin_nested()
        except AttributeError:
            result = await sql_session.execute(text(built.sql), built.params)
        else:
            async with begin_nested_cm:
                result = await sql_session.execute(text(built.sql), built.params)
    except Exception as exc:
        fallback_reason = _classify_create_fallback_reason(exc)
        if fallback_reason is not None:
            return await _enqueue_create_for_async_processing(
                payload=payload,
                session_id=session_id,
                queue_reason=fallback_reason,
                fallback_trigger="flat_insert",
            )
        raise
    affected_rows = int(getattr(result, "rowcount", 0) or 0)

    await sql_session.execute(
        text(
            "UPDATE session_metadata SET status = 'migrated' WHERE session_id = :sid "
            "AND status IN ('collecting', 'analyzing', 'migrating_incremental')"
        ),
        {"sid": session_id},
    )

    return _build_create_execution_response(
        built=built,
        affected_rows=affected_rows,
        mode="migrated_sync",
        parent_id=None,
        child_insert_counts={},
    )


def _extract_decomposition_plan(request: dict[str, Any]) -> dict[str, Any]:
    direct_plan = request.get("decomposition_plan")
    if isinstance(direct_plan, dict):
        return direct_plan

    metadata = request.get("analysis_metadata")
    if isinstance(metadata, dict):
        nested = metadata.get("decomposition_plan")
        if isinstance(nested, dict):
            return nested

    return {"version": 1, "parent_table": "chiral_data", "entities": []}


def _extract_session_id(request: dict[str, Any]) -> str | None:
    direct = request.get("session_id")
    if isinstance(direct, str) and direct:
        return direct

    payload = request.get("payload")
    if isinstance(payload, dict):
        payload_session_id = payload.get("session_id")
        if isinstance(payload_session_id, str) and payload_session_id:
            return payload_session_id

    updates = request.get("updates")
    if isinstance(updates, dict):
        updates_session_id = updates.get("session_id")
        if isinstance(updates_session_id, str) and updates_session_id:
            return updates_session_id

    filters = request.get("filters", [])
    if isinstance(filters, list):
        for item in filters:
            if not isinstance(item, dict):
                continue
            if str(item.get("field", "")).lower() != "session_id":
                continue
            value = item.get("value")
            if isinstance(value, str) and value:
                return value

    return None


async def _load_decomposition_plan_from_metadata(sql_session: AsyncSession, session_id: str) -> dict[str, Any]:
    schema = await _load_schema_from_metadata(sql_session, session_id)
    return _extract_decomposition_plan_from_schema(schema)


def _extract_decomposition_plan_from_schema(schema: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(schema, dict):
        return {"version": 1, "parent_table": "chiral_data", "entities": []}

    metadata = schema.get("__analysis_metadata__", {})
    if not isinstance(metadata, dict):
        return {"version": 1, "parent_table": "chiral_data", "entities": []}

    decomposition_plan = metadata.get("decomposition_plan", {})
    if not isinstance(decomposition_plan, dict):
        return {"version": 1, "parent_table": "chiral_data", "entities": []}

    entities = decomposition_plan.get("entities", [])
    if not isinstance(entities, list):
        entities = []

    return {
        "version": int(decomposition_plan.get("version", 1) or 1),
        "parent_table": str(decomposition_plan.get("parent_table", "chiral_data")),
        "entities": entities,
    }


def _rewrite_updates_for_jsonb_targets(
    updates: dict[str, Any],
    schema: dict[str, Any],
) -> dict[str, Any]:
    plan = _extract_decomposition_plan_from_schema(schema)
    child_entities = {
        e.get("source_field") for e in plan.get("entities", []) if isinstance(e, dict) and e.get("source_field")
    }

    rewritten: dict[str, Any] = {}
    for key, value in updates.items():
        if not isinstance(key, str):
            rewritten[key] = value
            continue

        if key == "overflow_data" or key.startswith("overflow_data."):
            rewritten[key] = value
            continue

        # Do not rewrite child entities or dot-notated fields
        if "." in key or key in child_entities:
            rewritten[key] = value
            continue

        meta = schema.get(key)
        if isinstance(meta, dict) and str(meta.get("target", "")).lower() == "jsonb":
            rewritten[f"overflow_data.{key}"] = value
        else:
            rewritten[key] = value

    return rewritten


def _rewrite_select_for_jsonb_targets(select_fields: list[str], schema: dict[str, Any]) -> list[str]:
    plan = _extract_decomposition_plan_from_schema(schema)
    child_entities = {
        e.get("source_field") for e in plan.get("entities", []) if isinstance(e, dict) and e.get("source_field")
    }

    rewritten = []
    for field in select_fields:
        if "." in field or field in child_entities:
            rewritten.append(field)
            continue
        meta = schema.get(field)
        if isinstance(meta, dict) and str(meta.get("target", "")).lower() == "jsonb":
            rewritten.append(f"overflow_data.{field}")
        else:
            rewritten.append(field)
    return rewritten


def _rewrite_filters_for_jsonb_targets(filters: list[dict[str, Any]], schema: dict[str, Any]) -> list[dict[str, Any]]:
    plan = _extract_decomposition_plan_from_schema(schema)
    child_entities = {
        e.get("source_field") for e in plan.get("entities", []) if isinstance(e, dict) and e.get("source_field")
    }

    rewritten = []
    for f in filters:
        field = str(f.get("field", ""))
        if "." in field or field in child_entities:
            rewritten.append(f)
            continue
        meta = schema.get(field)
        if isinstance(meta, dict) and str(meta.get("target", "")).lower() == "jsonb":
            new_f = dict(f)
            new_f["field"] = f"overflow_data.{field}"
            rewritten.append(new_f)
        else:
            rewritten.append(f)
    return rewritten


async def _load_schema_from_metadata(sql_session: AsyncSession, session_id: str) -> dict[str, Any]:
    result = await sql_session.execute(
        text("SELECT schema_json FROM session_metadata WHERE session_id = :sid"),
        {"sid": session_id},
    )
    row = result.fetchone()
    if not row:
        return {}

    raw_schema = row[0]
    if isinstance(raw_schema, str):
        try:
            schema = json.loads(raw_schema)
        except json.JSONDecodeError:
            schema = {}
    elif isinstance(raw_schema, dict):
        schema = raw_schema
    else:
        schema = {}

    if not isinstance(schema, dict):
        return {}

    return schema


async def _hydrate_request_with_decomposition_plan(
    request: dict[str, Any],
    sql_session: AsyncSession,
) -> dict[str, Any]:
    hydrated = dict(request)
    session_id = _extract_session_id(request)

    loaded_schema: dict[str, Any] | None = None

    updates = request.get("updates")
    should_rewrite_updates = isinstance(updates, dict)

    existing_plan = _extract_decomposition_plan(request)
    existing_entities = existing_plan.get("entities", [])
    needs_plan_hydration = not (isinstance(existing_entities, list) and existing_entities)

    if session_id and (should_rewrite_updates or needs_plan_hydration):
        loaded_schema = await _load_schema_from_metadata(sql_session, session_id)

    if should_rewrite_updates and loaded_schema is not None and isinstance(updates, dict):
        hydrated["updates"] = _rewrite_updates_for_jsonb_targets(updates, loaded_schema)

    if loaded_schema is not None:
        if isinstance(request.get("select"), list):
            hydrated["select"] = _rewrite_select_for_jsonb_targets(request["select"], loaded_schema)
        if isinstance(request.get("filters"), list):
            hydrated["filters"] = _rewrite_filters_for_jsonb_targets(request["filters"], loaded_schema)

    if not needs_plan_hydration:
        return hydrated

    if not session_id:
        return hydrated

    if loaded_schema is None:
        loaded_schema = await _load_schema_from_metadata(sql_session, session_id)

    metadata_plan = _extract_decomposition_plan_from_schema(loaded_schema)
    entities = metadata_plan.get("entities", [])
    if not isinstance(entities, list) or not entities:
        return hydrated

    hydrated["decomposition_plan"] = metadata_plan
    return hydrated


def _build_inferred_joins_for_request(request: dict[str, Any], table_name: str) -> list[InferredJoin]:
    plan = _extract_decomposition_plan(request)
    entities = plan.get("entities", [])
    if not isinstance(entities, list) or not entities:
        return []

    referenced_prefixes: set[str] = set()
    select_fields = request.get("select", ["*"])
    filters = request.get("filters", [])

    if isinstance(select_fields, list):
        for field in select_fields:
            if isinstance(field, str) and "." in field:
                prefix = field.split(".", 1)[0]
                if prefix != "overflow_data":
                    referenced_prefixes.add(prefix)

    if isinstance(filters, list):
        for item in filters:
            if not isinstance(item, dict):
                continue
            field = item.get("field")
            if isinstance(field, str) and "." in field:
                prefix = field.split(".", 1)[0]
                if prefix != "overflow_data":
                    referenced_prefixes.add(prefix)

    inferred: list[InferredJoin] = []
    for entity in entities:
        if not isinstance(entity, dict):
            continue

        source_field = entity.get("source_field")
        child_table = entity.get("child_table")
        if not isinstance(source_field, str) or not isinstance(child_table, str):
            continue
        if source_field not in referenced_prefixes:
            continue

        raw_child_column_types = entity.get("child_column_types", {})
        child_column_types = {
            str(column): str(inferred_type)
            for column, inferred_type in raw_child_column_types.items()
            if isinstance(column, str) and isinstance(inferred_type, str)
        }

        key_spec = build_dynamic_child_key_spec(parent_table=table_name, source_field=source_field)
        parent_fk_column = key_spec.foreign_keys[0]["local_column"]
        inferred.append(
            InferredJoin(
                source_field=source_field,
                child_table=child_table,
                parent_fk_column=parent_fk_column,
                child_column_types=child_column_types,
            )
        )

    return inferred


def translate_json_request(request: dict[str, Any]) -> BuiltQuery:
    """Translate a user JSON CRUD request into a parameterized SQL query.

    Supported operation values: read, create, update, delete.
    """
    operation = str(request.get("operation", "")).lower()
    table_name = str(request.get("table", "chiral_data"))
    inferred_joins = _build_inferred_joins_for_request(request, table_name)
    builder = CrudQueryBuilder(table_name=table_name, inferred_joins=inferred_joins)

    if operation == "read":
        return builder.build_select(
            select_fields=request.get("select", ["*"]),
            filters=request.get("filters", []),
            limit=request.get("limit"),
            offset=request.get("offset"),
        )

    if operation == "create":
        payload = request.get("payload", {})
        if not isinstance(payload, dict):
            msg = "create operation requires object payload"
            raise ValueError(msg)
        return builder.build_insert(payload)

    if operation == "update":
        updates = request.get("updates", {})
        if not isinstance(updates, dict):
            msg = "update operation requires object updates"
            raise ValueError(msg)
        return builder.build_update(updates=updates, filters=request.get("filters", []))

    if operation == "delete":
        return builder.build_delete(filters=request.get("filters", []))

    msg = f"Unsupported operation: {operation}"
    raise ValueError(msg)


@session
async def translate_json_request_with_metadata(
    request: dict[str, Any],
    sql_session: AsyncSession,
) -> BuiltQuery:
    """Translate JSON request, auto-hydrating decomposition plan from session metadata when absent."""
    hydrated_request = await _hydrate_request_with_decomposition_plan(request, sql_session)
    return translate_json_request(hydrated_request)


@session
async def execute_json_request(
    request: dict[str, Any],
    sql_session: AsyncSession,
) -> dict[str, Any]:
    """Translate and execute a JSON CRUD request against SQL storage."""
    return await _execute_json_request_impl(request=request, sql_session=sql_session)


async def _execute_json_request_impl(
    request: dict[str, Any],
    sql_session: AsyncSession,
) -> dict[str, Any]:
    """Execute translated requests (testable without session decorator)."""
    operation = str(request.get("operation", "")).lower()

    if operation == "create":
        return await _execute_create_request(request=request, sql_session=sql_session)

    hydrated_request = await _hydrate_request_with_decomposition_plan(request, sql_session)
    built = translate_json_request(hydrated_request)

    result = await sql_session.execute(text(built.sql), built.params)

    if operation == "read":
        raw_rows = [dict(row) for row in result.mappings().all()]

        # Phase 2: Logical Data Reconstruction (Merge duplicate parent rows into nested arrays)
        merged_results = {}
        child_keys = (
            [join.source_field for join in built.inferred_joins]
            if hasattr(built, "inferred_joins") and built.inferred_joins
            else []
        )

        for row in raw_rows:
            # Create a unique hash for the parent entity based on non-child fields
            parent_tuple = tuple((k, str(v)) for k, v in row.items() if k not in child_keys)

            if parent_tuple not in merged_results:
                # Initialize the logical parent document
                merged_results[parent_tuple] = {k: v for k, v in row.items() if k not in child_keys}
                for child_key in child_keys:
                    merged_results[parent_tuple][child_key] = []

            # Append child entities to their respective arrays
            for child_key in child_keys:
                child_obj = row.get(child_key)
                if child_obj is not None and child_obj not in merged_results[parent_tuple][child_key]:
                    # Avoid duplicate child objects in case of multiple joins
                    merged_results[parent_tuple][child_key].append(child_obj)

        logical_rows = list(merged_results.values())

        return {
            "sql": built.sql,
            "params": built.params,
            "rows": logical_rows,
            "row_count": len(logical_rows),
        }

    affected_rows = int(getattr(result, "rowcount", 0) or 0)
    return {
        "sql": built.sql,
        "params": built.params,
        "affected_rows": affected_rows,
    }
