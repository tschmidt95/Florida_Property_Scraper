from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Tuple

from florida_property_scraper.storage import SQLiteStore

from .connectors.base import TriggerConnector
from .models import RawEvent, TriggerEvent
from .taxonomy import TriggerKey


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _iso_minus_days(now_iso: str, days: int) -> str:
    try:
        dt = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)
    dt = dt.astimezone(timezone.utc)
    out = dt - timedelta(days=int(days))
    return out.replace(microsecond=0).isoformat()


def run_connector_once(
    *,
    store: SQLiteStore,
    connector: TriggerConnector,
    county: str,
    now_iso: str | None = None,
    limit: int = 50,
) -> Dict[str, Any]:
    now_iso = (now_iso or "").strip() or utc_now_iso()
    county_key = (county or "").strip().lower()
    run_id = f"triggers:{connector.connector_key}:{county_key}:{uuid.uuid4().hex[:10]}"

    raw_events = connector.poll(county=county_key, now_iso=now_iso, limit=limit)
    raw_ids = store.insert_trigger_raw_events(raw_events=raw_events, run_id=run_id)

    normalized: list[TriggerEvent] = []
    for raw, raw_id in zip(raw_events, raw_ids, strict=False):
        te = connector.normalize(raw, now_iso=now_iso)
        if te is None:
            continue
        normalized.append(
            TriggerEvent(
                county=te.county,
                parcel_id=te.parcel_id,
                trigger_key=te.trigger_key,
                trigger_at=te.trigger_at,
                severity=int(te.severity),
                source_connector_key=te.source_connector_key,
                source_event_type=te.source_event_type,
                source_event_id=int(raw_id) if raw_id is not None else None,
                details=te.details or {},
            )
        )

    trig_ids = store.insert_trigger_events(trigger_events=normalized, run_id=run_id)

    # Evaluate alerts based on recent triggers.
    alerts_written = evaluate_and_upsert_alerts(
        store=store,
        county=county_key,
        now_iso=now_iso,
        new_trigger_rows=list(zip(normalized, trig_ids, strict=False)),
    )

    return {
        "ok": True,
        "run_id": run_id,
        "county": county_key,
        "connector": connector.connector_key,
        "now": now_iso,
        "raw_events": len(raw_events),
        "trigger_events": len(normalized),
        "alerts_written": alerts_written,
    }


def evaluate_and_upsert_alerts(
    *,
    store: SQLiteStore,
    county: str,
    now_iso: str,
    new_trigger_rows: list[tuple[TriggerEvent, int]]
    | list[tuple[TriggerEvent, int | None]],
    window_days: int = 30,
) -> int:
    county_key = (county or "").strip().lower()
    since_iso = _iso_minus_days(now_iso, window_days)

    # Pull recent triggers from DB, plus the ones we just produced.
    # Deduplicate by trigger_event_id to avoid double-counting.
    # Track per parcel: trigger_event_id -> (trigger_key, severity)
    by_parcel: dict[str, dict[int, tuple[str, int]]] = defaultdict(dict)

    recent = store.list_trigger_events_for_county(county=county_key, since_iso=since_iso, limit=5000)
    for r in recent:
        pid = str(r.get("parcel_id") or "")
        key = str(r.get("trigger_key") or "")
        rid = int(r.get("id") or 0)
        sev = int(r.get("severity") or 1)
        if pid and key and rid:
            by_parcel[pid][rid] = (key, sev)

    for te, tid in new_trigger_rows:
        if tid is None:
            continue
        by_parcel[te.parcel_id][int(tid)] = (te.trigger_key, int(te.severity))

    wrote = 0
    for parcel_id, items_by_id in by_parcel.items():
        items = list(items_by_id.items())
        keys = {k for (_, (k, _)) in items}
        ids_by_key: dict[str, list[int]] = defaultdict(list)
        tier_counts = {"critical": 0, "strong": 0, "support": 0}
        trigger_ids_all: list[int] = []
        for rid, (k, sev) in items:
            ids_by_key[k].append(int(rid))
            trigger_ids_all.append(int(rid))

            try:
                s = int(sev)
            except Exception:
                s = 1
            if s >= 5:
                tier_counts["critical"] += 1
            elif s >= 4:
                tier_counts["strong"] += 1
            elif s >= 2:
                tier_counts["support"] += 1

        permit_keys = {k for k in keys if (k or "").startswith("permit_")}
        permit_ids: list[int] = []
        for pk in permit_keys:
            permit_ids.extend(ids_by_key.get(pk, []))
        permit_ids = sorted(set(int(x) for x in permit_ids if x))

        # 1) Simple alert: permit activity
        if permit_ids:
            wrote += int(
                store.upsert_trigger_alert(
                    county=county_key,
                    parcel_id=parcel_id,
                    alert_key="permit_activity",
                    severity=2,
                    first_seen_at=now_iso,
                    last_seen_at=now_iso,
                    status="open",
                    trigger_event_ids=permit_ids,
                    details={"window_days": window_days},
                )
            )

        # 2) Simple alert: owner moved
        if str(TriggerKey.OWNER_MAILING_CHANGED) in keys:
            wrote += int(
                store.upsert_trigger_alert(
                    county=county_key,
                    parcel_id=parcel_id,
                    alert_key="owner_moved",
                    severity=3,
                    first_seen_at=now_iso,
                    last_seen_at=now_iso,
                    status="open",
                    trigger_event_ids=ids_by_key[str(TriggerKey.OWNER_MAILING_CHANGED)],
                    details={"window_days": window_days},
                )
            )

        # 3) Stacked alert: redevelopment signal
        if permit_ids and str(TriggerKey.OWNER_MAILING_CHANGED) in keys:
            wrote += int(
                store.upsert_trigger_alert(
                    county=county_key,
                    parcel_id=parcel_id,
                    alert_key="redevelopment_signal",
                    severity=4,
                    first_seen_at=now_iso,
                    last_seen_at=now_iso,
                    status="open",
                    trigger_event_ids=sorted(
                        set(
                            permit_ids
                            + ids_by_key[str(TriggerKey.OWNER_MAILING_CHANGED)]
                        )
                    ),
                    details={"window_days": window_days, "rule": "permit+mailing_change"},
                )
            )

        # 4) Seller intent: tiered stacking rule
        c = int(tier_counts.get("critical") or 0)
        s = int(tier_counts.get("strong") or 0)
        p = int(tier_counts.get("support") or 0)

        seller_score = int(SQLiteStore._compute_seller_score(critical=c, strong=s, support=p))
        rule: str | None = None
        if c >= 1:
            rule = "critical>=1"
        elif s >= 2:
            rule = "strong>=2"
        elif (s + p) >= 4:
            rule = "mixed>=4"

        if rule is not None:
            sev = 3
            if seller_score >= 100:
                sev = 5
            elif seller_score >= 85:
                sev = 4
            elif seller_score >= 70:
                sev = 3

            wrote += int(
                store.upsert_trigger_alert(
                    county=county_key,
                    parcel_id=parcel_id,
                    alert_key="seller_intent",
                    severity=int(sev),
                    first_seen_at=now_iso,
                    last_seen_at=now_iso,
                    status="open",
                    trigger_event_ids=sorted(set(int(x) for x in trigger_ids_all if x)),
                    details={
                        "window_days": window_days,
                        "rule": rule,
                        "seller_score": seller_score,
                        "counts": {"critical": c, "strong": s, "support": p},
                        "trigger_keys": sorted(k for k in keys if k),
                    },
                )
            )

    return wrote
