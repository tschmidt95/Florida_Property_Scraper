from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from florida_property_scraper.triggers.taxonomy import TriggerKey, default_severity_for_trigger


@dataclass
class TriggerEvaluation:
    trigger_key: str
    fired: bool
    severity: int
    reason: str
    evidence_ids: list[int]
    fields_used: list[str]
    parcel_id: str
    evaluated_at: str


def _parse_iso(value: str | None) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _recent_within_days(value: str | None, days: int) -> bool:
    dt = _parse_iso(value)
    if not dt:
        return False
    return dt >= datetime.now(timezone.utc) - timedelta(days=days)


def _best_evidence(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None

    def rank(ev: dict[str, Any]) -> tuple[float, int]:
        conf = float(ev.get("confidence") or 0.0)
        ts = _parse_iso(str(ev.get("retrieved_at") or ""))
        return (conf, int(ts.timestamp()) if ts else 0)

    return sorted(rows, key=rank, reverse=True)[0]


def evaluate_triggers_from_evidence(
    *,
    county: str,
    evidence_rows: list[dict[str, Any]],
    parcel_ids: list[str] | None = None,
    now_iso: str | None = None,
) -> list[TriggerEvaluation]:
    now = now_iso or _now_iso()
    by_parcel: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for row in evidence_rows:
        pid = str(row.get("parcel_id") or "").strip()
        field = str(row.get("field") or "").strip()
        if not pid or not field:
            continue
        by_parcel.setdefault(pid, {}).setdefault(field, []).append(row)

    out: list[TriggerEvaluation] = []

    parcel_list = [str(x or "").strip() for x in (parcel_ids or [])]
    parcel_list = [x for x in parcel_list if x]
    if not parcel_list:
        parcel_list = list(by_parcel.keys())

    for parcel_id in parcel_list:
        fields = by_parcel.get(parcel_id, {})
        def add_result(*, key: str, fired: bool, reason: str, evidence: list[dict[str, Any]], used_fields: list[str]) -> None:
            ids = []
            for ev in evidence:
                try:
                    ids.append(int(ev.get("id") or 0))
                except Exception:
                    continue
            ids = [x for x in ids if x > 0]
            out.append(
                TriggerEvaluation(
                    trigger_key=key,
                    fired=fired,
                    severity=default_severity_for_trigger(key),
                    reason=reason,
                    evidence_ids=ids,
                    fields_used=used_fields,
                    parcel_id=parcel_id,
                    evaluated_at=now,
                )
            )

        def add_missing(*, key: str, used_fields: list[str]) -> None:
            add_result(
                key=key,
                fired=False,
                reason="insufficient_evidence",
                evidence=[],
                used_fields=used_fields,
            )

        # permit_recent_major
        permit_ev = _best_evidence(fields.get("permit_last_major_date") or [])
        fired = False
        reason = ""
        if permit_ev:
            fired = _recent_within_days(str(permit_ev.get("value") or ""), 365)
            reason = f"permit_last_major_date={permit_ev.get('value')}"
            add_result(
                key=TriggerKey.PERMIT_RECENT_MAJOR,
                fired=fired,
                reason=reason,
                evidence=[permit_ev],
                used_fields=["permit_last_major_date"],
            )
        else:
            add_missing(key=TriggerKey.PERMIT_RECENT_MAJOR, used_fields=["permit_last_major_date"])

        # code_enforcement_open_case
        code_status_ev = _best_evidence(fields.get("code_enforcement_status") or [])
        code_date_ev = _best_evidence(fields.get("code_case_opened_date") or [])
        fired = False
        reason = ""
        evidence_used: list[dict[str, Any]] = []
        used_fields: list[str] = []
        if code_status_ev:
            status = str(code_status_ev.get("value") or "").strip().lower()
            fired = status == "open"
            reason = f"code_enforcement_status={status}"
            evidence_used.append(code_status_ev)
            used_fields.append("code_enforcement_status")
        if not fired and code_date_ev:
            fired = _recent_within_days(str(code_date_ev.get("value") or ""), 365)
            reason = f"code_case_opened_date={code_date_ev.get('value')}"
            evidence_used.append(code_date_ev)
            used_fields.append("code_case_opened_date")
        if evidence_used:
            add_result(
                key=TriggerKey.CODE_ENFORCEMENT_OPEN_CASE,
                fired=fired,
                reason=reason,
                evidence=evidence_used,
                used_fields=used_fields,
            )
        else:
            add_missing(
                key=TriggerKey.CODE_ENFORCEMENT_OPEN_CASE,
                used_fields=["code_enforcement_status", "code_case_opened_date"],
            )

        # tax_delinquent
        tax_status_ev = _best_evidence(fields.get("tax_status") or [])
        tax_years_ev = _best_evidence(fields.get("tax_delinquent_years") or [])
        fired = False
        reason = ""
        evidence_used = []
        used_fields = []
        if tax_status_ev:
            status = str(tax_status_ev.get("value") or "").strip().lower()
            fired = "delinquent" in status
            reason = f"tax_status={status}"
            evidence_used.append(tax_status_ev)
            used_fields.append("tax_status")
        if not fired and tax_years_ev:
            try:
                years = int(tax_years_ev.get("value") or 0)
            except Exception:
                years = 0
            fired = years > 0
            reason = f"tax_delinquent_years={years}"
            evidence_used.append(tax_years_ev)
            used_fields.append("tax_delinquent_years")
        if evidence_used:
            add_result(
                key=TriggerKey.TAX_DELINQUENT,
                fired=fired,
                reason=reason,
                evidence=evidence_used,
                used_fields=used_fields,
            )
        else:
            add_missing(
                key=TriggerKey.TAX_DELINQUENT,
                used_fields=["tax_status", "tax_delinquent_years"],
            )

        # deed_transfer_recent
        sale_ev = _best_evidence(fields.get("last_sale_date") or [])
        if sale_ev:
            fired = _recent_within_days(str(sale_ev.get("value") or ""), 365)
            reason = f"last_sale_date={sale_ev.get('value')}"
            add_result(
                key=TriggerKey.DEED_TRANSFER_RECENT,
                fired=fired,
                reason=reason,
                evidence=[sale_ev],
                used_fields=["last_sale_date"],
            )
        else:
            add_missing(key=TriggerKey.DEED_TRANSFER_RECENT, used_fields=["last_sale_date"])

        # foreclosure_or_lis_pendens
        doc_ev = _best_evidence(fields.get("official_record_doc_type") or [])
        if doc_ev:
            doc_type = str(doc_ev.get("value") or "").strip().lower()
            fired = "lis pendens" in doc_type or "foreclosure" in doc_type
            reason = f"official_record_doc_type={doc_type}"
            add_result(
                key=TriggerKey.FORECLOSURE_OR_LIS_PENDENS,
                fired=fired,
                reason=reason,
                evidence=[doc_ev],
                used_fields=["official_record_doc_type"],
            )
        else:
            add_missing(
                key=TriggerKey.FORECLOSURE_OR_LIS_PENDENS,
                used_fields=["official_record_doc_type"],
            )

        # new_recording
        rec_ev = _best_evidence(fields.get("official_record_rec_date") or [])
        doc_ev = _best_evidence(fields.get("official_record_doc_type") or [])
        evidence_used = []
        used_fields = []
        if rec_ev:
            evidence_used.append(rec_ev)
            used_fields.append("official_record_rec_date")
        if doc_ev:
            evidence_used.append(doc_ev)
            used_fields.append("official_record_doc_type")
        if rec_ev:
            rec_date = str(rec_ev.get("value") or "")
            doc_type = str(doc_ev.get("value") or "") if doc_ev else ""
            reason = f"official_record_rec_date={rec_date}" + (f" doc_type={doc_type}" if doc_type else "")
            add_result(
                key=TriggerKey.NEW_RECORDING,
                fired=True,
                reason=reason,
                evidence=evidence_used,
                used_fields=used_fields,
            )
        else:
            add_missing(
                key=TriggerKey.NEW_RECORDING,
                used_fields=["official_record_rec_date", "official_record_doc_type"],
            )

        # owner_mailing_change
        mail_ev = _best_evidence(fields.get("owner_mailing_changed") or [])
        mail_date_ev = _best_evidence(fields.get("owner_mailing_change_date") or [])
        fired = False
        reason = ""
        evidence_used = []
        used_fields = []
        if mail_ev:
            fired = bool(mail_ev.get("value"))
            reason = f"owner_mailing_changed={mail_ev.get('value')}"
            evidence_used.append(mail_ev)
            used_fields.append("owner_mailing_changed")
        if not fired and mail_date_ev:
            fired = _recent_within_days(str(mail_date_ev.get("value") or ""), 365)
            reason = f"owner_mailing_change_date={mail_date_ev.get('value')}"
            evidence_used.append(mail_date_ev)
            used_fields.append("owner_mailing_change_date")
        if evidence_used:
            add_result(
                key=TriggerKey.OWNER_MAILING_CHANGE,
                fired=fired,
                reason=reason,
                evidence=evidence_used,
                used_fields=used_fields,
            )
        else:
            add_missing(
                key=TriggerKey.OWNER_MAILING_CHANGE,
                used_fields=["owner_mailing_changed", "owner_mailing_change_date"],
            )

    return out
