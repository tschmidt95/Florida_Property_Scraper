from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from florida_property_scraper.enrichment.providers.model import EvidenceItem, compute_request_fingerprint
from florida_property_scraper.enrichment.providers.registry import get_property_providers
from florida_property_scraper.registry import list_counties, normalize_county_slug
from florida_property_scraper.storage import SQLiteStore
from florida_property_scraper.triggers.evidence_rules import evaluate_triggers_from_evidence


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name),),
    ).fetchone()
    return bool(row and row[0])


def _table_columns(conn: Any, table_name: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except Exception:
        return set()
    out: set[str] = set()
    for row in rows:
        try:
            out.add(str(row[1]).strip())
        except Exception:
            continue
    return out


def _trigger_result_hash(*, county: str, parcel_id: str, trigger_id: str, reason: str, evidence_ids: list[int]) -> str:
    payload = {
        "county": str(county or "").strip().lower(),
        "parcel_id": str(parcel_id or "").strip(),
        "trigger_id": str(trigger_id or "").strip(),
        "reason": str(reason or "").strip(),
        "evidence_ids": sorted({int(x) for x in evidence_ids if int(x) > 0}),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str)
    import hashlib

    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _collect_parcel_ids_for_county(*, store: SQLiteStore, county: str, max_parcels: int) -> list[str]:
    county_key = (county or "").strip().lower()
    if not county_key:
        return []

    conn = store.conn
    ids: list[str] = []
    seen: set[str] = set()

    def add(v: Any) -> None:
        pid = str(v or "").strip()
        if not pid:
            return
        if pid in seen:
            return
        seen.add(pid)
        ids.append(pid)

    # Prefer parcel IDs that already appear in provider_evidence (highest likelihood of trigger-ready records).
    if _table_exists(conn, "provider_evidence"):
        rows = conn.execute(
            """
            SELECT parcel_id
            FROM provider_evidence
            WHERE county=?
            GROUP BY parcel_id
            ORDER BY MAX(retrieved_at) DESC
            LIMIT ?
            """,
            (county_key, max(1, int(max_parcels))),
        ).fetchall()
        for r in rows:
            add(r[0])

    # Pull additional IDs from public-source tables.
    table_specs: list[tuple[str, str, str, tuple[str, ...]]] = [
        ("official_records", "county", "parcel_id", ("observed_at", "rec_date", "id")),
        ("permits", "county", "parcel_id", ("observed_at", "issue_date", "id")),
        ("tax_collector_events", "county", "parcel_id", ("observed_at", "event_date", "id")),
        ("code_enforcement_events", "county", "parcel_id", ("observed_at", "event_date", "id")),
    ]

    for table, county_col, parcel_col, order_candidates in table_specs:
        if len(ids) >= int(max_parcels):
            break
        if not _table_exists(conn, table):
            continue
        cols = _table_columns(conn, table)
        if county_col not in cols or parcel_col not in cols:
            continue

        usable_order_cols = [c for c in order_candidates if c in cols]
        if not usable_order_cols:
            order_expr = "id"
        elif len(usable_order_cols) == 1:
            order_expr = usable_order_cols[0]
        else:
            order_expr = f"COALESCE({', '.join(usable_order_cols)})"

        rows = conn.execute(
            f"""
            SELECT {parcel_col}
            FROM {table}
            WHERE lower(trim({county_col}))=? AND nullif(trim({parcel_col}), '') IS NOT NULL
            GROUP BY {parcel_col}
            ORDER BY MAX({order_expr}) DESC
            LIMIT ?
            """,
            (county_key, max(1, int(max_parcels))),
        ).fetchall()
        for r in rows:
            add(r[0])
            if len(ids) >= int(max_parcels):
                break

    # Optional PA table support for counties where pa_properties exists.
    if len(ids) < int(max_parcels) and _table_exists(conn, "pa_properties"):
        cols = _table_columns(conn, "pa_properties")
        county_col = "county" if "county" in cols else None
        parcel_col = "parcel_id" if "parcel_id" in cols else ("pa_parcel_id" if "pa_parcel_id" in cols else None)
        order_col = "updated_at" if "updated_at" in cols else ("retrieved_at" if "retrieved_at" in cols else None)
        if county_col and parcel_col:
            order_sql = f"ORDER BY COALESCE({order_col}, id) DESC" if order_col else "ORDER BY id DESC"
            rows = conn.execute(
                f"""
                SELECT {parcel_col}
                FROM pa_properties
                WHERE lower(trim({county_col}))=? AND nullif(trim({parcel_col}), '') IS NOT NULL
                GROUP BY {parcel_col}
                {order_sql}
                LIMIT ?
                """,
                (county_key, max(1, int(max_parcels))),
            ).fetchall()
            for r in rows:
                add(r[0])
                if len(ids) >= int(max_parcels):
                    break

    return ids[: max(1, int(max_parcels))]


def run_statewide_refresh_tick(
    *,
    db_path: str,
    counties: list[str] | None = None,
    batch_size: int = 100,
    max_parcels_per_county: int = 1000,
    min_confidence_label: str = "low",
) -> dict[str, Any]:
    """Refresh enrichment snapshots + trigger evaluations for counties using public source tables.

    This is intentionally DB-first and provider-driven so it can be run by cron or API startup loops.
    """

    now = _utc_now_iso()
    bs = max(1, min(int(batch_size or 100), 500))
    max_per_county = max(1, min(int(max_parcels_per_county or 1000), 20000))

    county_keys = [normalize_county_slug(c) for c in (counties or [])]
    county_keys = [c for c in county_keys if c]
    if not county_keys:
        county_keys = [c.slug for c in list_counties()]

    out: dict[str, Any] = {
        "ok": True,
        "now": now,
        "db": db_path,
        "counties": [],
        "stats": {
            "counties_attempted": 0,
            "counties_processed": 0,
            "parcels_considered": 0,
            "parcels_enriched": 0,
            "provider_fetches": 0,
            "evidence_rows_written": 0,
            "trigger_results_written": 0,
            "rollups_rebuilt": 0,
        },
        "errors": [],
    }

    store = SQLiteStore(db_path)
    try:
        for county in county_keys:
            out["stats"]["counties_attempted"] += 1
            parcel_ids = _collect_parcel_ids_for_county(store=store, county=county, max_parcels=max_per_county)
            if not parcel_ids:
                continue

            providers = [p for p in get_property_providers(county=county) if getattr(p, "implemented", False)]
            county_stat: dict[str, Any] = {
                "county": county,
                "parcels": len(parcel_ids),
                "providers": [p.key for p in providers],
                "evidence_rows_written": 0,
                "trigger_results_written": 0,
                "errors": [],
            }

            for i in range(0, len(parcel_ids), bs):
                batch = parcel_ids[i : i + bs]
                if not batch:
                    continue

                for pid in batch:
                    for provider in providers:
                        out["stats"]["provider_fetches"] += 1
                        started = time.perf_counter()
                        try:
                            result = provider.fetch(
                                county=county,
                                parcel_id=pid,
                                dry_run=False,
                                fixture_mode=False,
                                store=store,
                            )
                            evidence_ids: list[int] = []
                            for ev in result.evidence:
                                if not isinstance(ev, EvidenceItem):
                                    continue
                                provider_name = str(ev.provider_name or "").strip() or str(result.provider_key or "").strip()
                                ev_id = store.upsert_provider_evidence(
                                    provider_key=result.provider_key,
                                    provider_name=provider_name,
                                    county=county,
                                    parcel_id=pid,
                                    field=ev.field,
                                    value=ev.value,
                                    confidence=ev.confidence_score,
                                    confidence_label=ev.confidence_label,
                                    source_type=ev.source.source_type,
                                    source_url=ev.source.url,
                                    fetched_at=ev.fetched_at,
                                    retrieved_at=ev.retrieved_at,
                                    content_hash=ev.content_hash,
                                    extract_method=ev.extract_method,
                                    raw_reference=ev.raw_reference,
                                    raw_snippet=ev.raw_snippet,
                                    raw_id=ev.raw_ref,
                                )
                                if ev_id:
                                    evidence_ids.append(int(ev_id))

                            county_stat["evidence_rows_written"] += len(evidence_ids)
                            out["stats"]["evidence_rows_written"] += len(evidence_ids)

                            elapsed_ms = int((time.perf_counter() - started) * 1000)
                            store.log_provider_fetch(
                                provider_key=result.provider_key,
                                county=county,
                                parcel_id=pid,
                                request_fingerprint=compute_request_fingerprint(
                                    provider_key=result.provider_key,
                                    county=county,
                                    parcel_id=pid,
                                    url=result.raw_artifacts[0].url if result.raw_artifacts else f"scheduler://{result.provider_key}",
                                ),
                                url=result.raw_artifacts[0].url if result.raw_artifacts else f"scheduler://{result.provider_key}",
                                http_status=200 if result.ok else None,
                                fetched_at=result.fetched_at,
                                duration_ms=elapsed_ms,
                                ok=result.ok,
                                error="; ".join(result.errors) if result.errors else None,
                            )
                        except Exception as exc:
                            county_stat["errors"].append(f"provider={provider.key} parcel={pid} error={exc}")

                # Rebuild snapshots + trigger results for this batch.
                rows = store.list_provider_evidence_for_parcels(county=county, parcel_ids=batch)
                by_parcel: dict[str, list[dict[str, Any]]] = {}
                for row in rows:
                    pid = str(row.get("parcel_id") or "").strip()
                    if not pid:
                        continue
                    by_parcel.setdefault(pid, []).append(row)

                for pid in batch:
                    merged, ev_ids = store.build_enriched_fields(
                        evidence_rows=by_parcel.get(pid, []),
                        min_confidence_label=min_confidence_label,
                    )
                    if merged:
                        out["stats"]["parcels_enriched"] += 1
                    store.save_parcel_enrichment_snapshot(
                        county=county,
                        parcel_id=pid,
                        snapshot_at=now,
                        merged_fields=merged,
                        evidence_ids=ev_ids,
                    )

                trig_rows = evaluate_triggers_from_evidence(
                    county=county,
                    evidence_rows=rows,
                    parcel_ids=batch,
                    now_iso=now,
                )
                to_persist: list[dict[str, Any]] = []
                for tr in trig_rows:
                    if not tr.fired:
                        continue
                    evidence_ids = [int(x) for x in (tr.evidence_ids or []) if int(x) > 0]
                    to_persist.append(
                        {
                            "parcel_id": tr.parcel_id,
                            "trigger_id": tr.trigger_key,
                            "reason": tr.reason,
                            "evidence_ids": evidence_ids,
                            "evaluated_at": tr.evaluated_at,
                            "result_hash": _trigger_result_hash(
                                county=county,
                                parcel_id=str(tr.parcel_id or ""),
                                trigger_id=str(tr.trigger_key or ""),
                                reason=str(tr.reason or ""),
                                evidence_ids=evidence_ids,
                            ),
                        }
                    )

                if to_persist:
                    run_id = f"statewide_refresh:{county}:{uuid.uuid4().hex[:8]}"
                    wrote = store.upsert_trigger_results(run_id=run_id, county=county, results=to_persist)
                    county_stat["trigger_results_written"] += len(wrote)
                    out["stats"]["trigger_results_written"] += len(wrote)

            try:
                store.rebuild_parcel_trigger_rollups(county=county, rebuilt_at=now)
                out["stats"]["rollups_rebuilt"] += 1
            except Exception as exc:
                county_stat["errors"].append(f"rollups error={exc}")

            out["stats"]["counties_processed"] += 1
            out["stats"]["parcels_considered"] += len(parcel_ids)
            out["counties"].append(county_stat)

        return out
    finally:
        store.close()


def run_statewide_refresh(
    *,
    db_path: str,
    interval_seconds: int = 900,
    loop: bool = False,
    counties: list[str] | None = None,
    batch_size: int = 100,
    max_parcels_per_county: int = 1000,
    min_confidence_label: str | None = None,
) -> dict[str, Any]:
    interval = max(30, int(interval_seconds or 900))
    min_conf = str(min_confidence_label or os.getenv("FPS_EVIDENCE_MIN_CONFIDENCE", "low")).strip().lower() or "low"

    if not loop:
        res = run_statewide_refresh_tick(
            db_path=db_path,
            counties=counties,
            batch_size=batch_size,
            max_parcels_per_county=max_parcels_per_county,
            min_confidence_label=min_conf,
        )
        res = dict(res)
        res["mode"] = "once"
        return res

    ticks: list[dict[str, Any]] = []
    while True:
        res = run_statewide_refresh_tick(
            db_path=db_path,
            counties=counties,
            batch_size=batch_size,
            max_parcels_per_county=max_parcels_per_county,
            min_confidence_label=min_conf,
        )
        ticks.append(res)
        if len(ticks) > 3:
            ticks = ticks[-3:]
        time.sleep(interval)
