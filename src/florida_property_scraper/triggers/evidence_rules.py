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


def _to_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).strip().replace(",", "")
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _to_int(value: object) -> int | None:
    try:
        v = _to_float(value)
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _norm_addr(value: object) -> str:
    try:
        s = str(value or "").strip().upper()
    except Exception:
        return ""
    if not s:
        return ""
    return " ".join(s.split())


def _extract_state(value: object) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    parts = [p.strip() for p in raw.replace("  ", " ").split(",") if p.strip()]
    if parts:
        tail = parts[-1].split()
        if len(tail) >= 2 and len(tail[-2]) == 2:
            return tail[-2]
        if len(tail) >= 1 and len(tail[-1]) == 2:
            return tail[-1]
    return ""


def _years_since(value: str | None) -> int | None:
    dt = _parse_iso(value)
    if not dt:
        return None
    try:
        days = (datetime.now(timezone.utc) - dt).days
        if days < 0:
            return None
        return int(days // 365.25)
    except Exception:
        return None


def _best_evidence(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None

    def rank(ev: dict[str, Any]) -> tuple[float, int]:
        conf = float(ev.get("confidence") or 0.0)
        ts = _parse_iso(str(ev.get("retrieved_at") or ""))
        return (conf, int(ts.timestamp()) if ts else 0)

    return sorted(rows, key=rank, reverse=True)[0]


def _doc_matches_any(doc_type: str, needles: list[str]) -> bool:
    dt = str(doc_type or "").strip().lower()
    if not dt:
        return False
    for n in needles:
        if str(n or "").strip().lower() in dt:
            return True
    return False


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
        handled_keys: set[str] = set()

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
            handled_keys.add(str(key))

        def add_missing(*, key: str, used_fields: list[str], reason: str | None = None) -> None:
            missing_reason = reason or (
                "unavailable: missing_fields=" + ",".join([str(x) for x in (used_fields or []) if str(x)])
            )
            add_result(
                key=key,
                fired=False,
                reason=missing_reason,
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
            add_missing(
                key=TriggerKey.PERMIT_RECENT_MAJOR,
                used_fields=["permit_last_major_date"],
                reason="unavailable: permit_last_major_date_missing",
            )

        # permit_recent_minor
        permit_minor_ev = _best_evidence(fields.get("permit_last_minor_date") or [])
        fired = False
        reason = ""
        if permit_minor_ev:
            fired = _recent_within_days(str(permit_minor_ev.get("value") or ""), 365)
            reason = f"permit_last_minor_date={permit_minor_ev.get('value')}"
            add_result(
                key=TriggerKey.PERMIT_RECENT_MINOR,
                fired=fired,
                reason=reason,
                evidence=[permit_minor_ev],
                used_fields=["permit_last_minor_date"],
            )
        else:
            add_missing(
                key=TriggerKey.PERMIT_RECENT_MINOR,
                used_fields=["permit_last_minor_date"],
                reason="unavailable: permit_last_minor_date_missing",
            )

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
            add_result(
                key=TriggerKey.DELINQUENT_TAX,
                fired=fired,
                reason=reason,
                evidence=evidence_used,
                used_fields=used_fields,
            )
        else:
            add_missing(
                key=TriggerKey.TAX_DELINQUENT,
                used_fields=["tax_status", "tax_delinquent_years"],
                reason="unavailable: tax_fields_missing",
            )
            add_missing(
                key=TriggerKey.DELINQUENT_TAX,
                used_fields=["tax_status", "tax_delinquent_years"],
                reason="unavailable: tax_fields_missing",
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
            doc_evidence: dict[str, Any] = doc_ev
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

        # Official-record document-type taxonomy (distinguishes all record/court-style triggers)
        doc_ev = _best_evidence(fields.get("official_record_doc_type") or [])
        if doc_ev:
            doc_type = str(doc_ev.get("value") or "").strip().lower()

            def doc_rule(key: str, needles: list[str]) -> None:
                fired_local = _doc_matches_any(doc_type, needles)
                add_result(
                    key=key,
                    fired=fired_local,
                    reason=f"official_record_doc_type={doc_type}",
                    evidence=[doc_evidence],
                    used_fields=["official_record_doc_type"],
                )

            doc_rule(TriggerKey.LIS_PENDENS, ["lis pendens"])
            doc_rule(TriggerKey.FORECLOSURE_FILING, ["foreclosure", "notice of default"])
            doc_rule(TriggerKey.FORECLOSURE_JUDGMENT, ["foreclosure judgment", "final judgment"])
            doc_rule(TriggerKey.FORECLOSURE, ["foreclosure", "lis pendens"])
            doc_rule(TriggerKey.TAX_DEED_APPLICATION, ["tax deed application", "tax deed"])
            doc_rule(TriggerKey.PROBATE_OPENED, ["probate"])
            doc_rule(TriggerKey.DIVORCE_FILED, ["divorce", "dissolution of marriage"])
            doc_rule(TriggerKey.EVICTION_FILING, ["eviction", "unlawful detainer"])

            doc_rule(TriggerKey.DEED_RECORDED, ["deed"])
            doc_rule(TriggerKey.DEED_WARRANTY, ["warranty deed"])
            doc_rule(TriggerKey.DEED_QUITCLAIM, ["quitclaim", "quit claim"])
            doc_rule(TriggerKey.MORTGAGE_RECORDED, ["mortgage"])
            doc_rule(TriggerKey.MORTGAGE_SATISFACTION, ["satisfaction"])
            doc_rule(TriggerKey.MORTGAGE_ASSIGNMENT, ["assignment of mortgage", "mortgage assignment"])

            doc_rule(TriggerKey.MECHANICS_LIEN, ["mechanic", "construction lien"])
            doc_rule(TriggerKey.HOA_LIEN, ["hoa lien", "homeowners association lien"])
            doc_rule(TriggerKey.IRS_TAX_LIEN, ["irs tax lien", "federal tax lien"])
            doc_rule(TriggerKey.STATE_TAX_LIEN, ["state tax lien"])
            doc_rule(TriggerKey.CODE_ENFORCEMENT_LIEN, ["code enforcement lien"])
            doc_rule(TriggerKey.JUDGMENT_LIEN, ["judgment lien"])
            doc_rule(TriggerKey.UTILITY_LIEN, ["utility lien"])
            doc_rule(TriggerKey.LIEN_RECORDED, ["lien"])
        else:
            missing_doc_keys = [
                TriggerKey.LIS_PENDENS,
                TriggerKey.FORECLOSURE_FILING,
                TriggerKey.FORECLOSURE_JUDGMENT,
                TriggerKey.FORECLOSURE,
                TriggerKey.TAX_DEED_APPLICATION,
                TriggerKey.PROBATE_OPENED,
                TriggerKey.DIVORCE_FILED,
                TriggerKey.EVICTION_FILING,
                TriggerKey.DEED_RECORDED,
                TriggerKey.DEED_WARRANTY,
                TriggerKey.DEED_QUITCLAIM,
                TriggerKey.MORTGAGE_RECORDED,
                TriggerKey.MORTGAGE_SATISFACTION,
                TriggerKey.MORTGAGE_ASSIGNMENT,
                TriggerKey.MECHANICS_LIEN,
                TriggerKey.HOA_LIEN,
                TriggerKey.IRS_TAX_LIEN,
                TriggerKey.STATE_TAX_LIEN,
                TriggerKey.CODE_ENFORCEMENT_LIEN,
                TriggerKey.JUDGMENT_LIEN,
                TriggerKey.UTILITY_LIEN,
                TriggerKey.LIEN_RECORDED,
            ]
            for mk in missing_doc_keys:
                add_missing(
                    key=mk,
                    used_fields=["official_record_doc_type"],
                    reason="unavailable: official_record_doc_type_missing",
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
                reason="unavailable: mailing_change_history_missing",
            )

        # absentee_owner
        mailing_ev = _best_evidence(fields.get("owner_mailing_address") or [])
        situs_ev = _best_evidence(fields.get("situs_address") or [])
        fired = False
        reason = ""
        evidence_used = []
        used_fields = []
        if mailing_ev and situs_ev:
            mailing = _norm_addr(mailing_ev.get("value"))
            situs = _norm_addr(situs_ev.get("value"))
            fired = bool(mailing and situs and mailing != situs)
            reason = f"mailing_vs_situs={mailing}!={situs}" if mailing and situs else "mailing_vs_situs=missing"
            evidence_used = [mailing_ev, situs_ev]
            used_fields = ["owner_mailing_address", "situs_address"]
        if evidence_used:
            add_result(
                key=TriggerKey.ABSENTEE_OWNER,
                fired=fired,
                reason=reason,
                evidence=evidence_used,
                used_fields=used_fields,
            )
        else:
            add_missing(
                key=TriggerKey.ABSENTEE_OWNER,
                used_fields=["owner_mailing_address", "situs_address"],
                reason="unavailable: mailing_or_situs_missing",
            )

        # out_of_state_owner
        mailing_state_ev = _best_evidence(fields.get("mailing_state") or [])
        mailing_addr_ev = _best_evidence(fields.get("owner_mailing_address") or [])
        fired = False
        reason = ""
        evidence_used = []
        used_fields = []
        state_val = ""
        if mailing_state_ev:
            state_val = str(mailing_state_ev.get("value") or "").strip().upper()
            evidence_used.append(mailing_state_ev)
            used_fields.append("mailing_state")
        elif mailing_addr_ev:
            state_val = _extract_state(mailing_addr_ev.get("value"))
            evidence_used.append(mailing_addr_ev)
            used_fields.append("owner_mailing_address")

        if evidence_used:
            fired = bool(state_val and state_val != "FL")
            reason = f"mailing_state={state_val or 'UNKNOWN'}"
            add_result(
                key=TriggerKey.OUT_OF_STATE_OWNER,
                fired=fired,
                reason=reason,
                evidence=evidence_used,
                used_fields=used_fields,
            )
        else:
            add_missing(
                key=TriggerKey.OUT_OF_STATE_OWNER,
                used_fields=["mailing_state", "owner_mailing_address"],
                reason="unavailable: mailing_state_missing",
            )

        # mortgage_recent
        mortgage_date_ev = _best_evidence(fields.get("mortgage_date") or [])
        mortgage_amount_ev = _best_evidence(fields.get("mortgage_amount") or [])
        fired = False
        reason = ""
        evidence_used = []
        used_fields = []
        if mortgage_date_ev:
            evidence_used.append(mortgage_date_ev)
            used_fields.append("mortgage_date")
            fired = _recent_within_days(str(mortgage_date_ev.get("value") or ""), 365)
            reason = f"mortgage_date={mortgage_date_ev.get('value')}"
        if mortgage_amount_ev:
            evidence_used.append(mortgage_amount_ev)
            used_fields.append("mortgage_amount")
        if evidence_used:
            add_result(
                key=TriggerKey.MORTGAGE_RECENT,
                fired=fired,
                reason=reason or "mortgage_date=missing",
                evidence=evidence_used,
                used_fields=used_fields,
            )
        else:
            add_missing(
                key=TriggerKey.MORTGAGE_RECENT,
                used_fields=["mortgage_date", "mortgage_amount"],
                reason="unavailable: mortgage_fields_missing",
            )

        # equity_high / equity_low
        value_ev = (
            _best_evidence(fields.get("total_value") or [])
            or _best_evidence(fields.get("assessed_value") or [])
            or _best_evidence(fields.get("last_sale_price") or [])
        )
        mortgage_ev = _best_evidence(fields.get("mortgage_amount") or [])
        equity_ratio = None
        evidence_used = []
        used_fields = []
        if value_ev and mortgage_ev:
            value_amt = _to_float(value_ev.get("value"))
            mortgage_amt = _to_float(mortgage_ev.get("value"))
            if value_amt and value_amt > 0 and mortgage_amt is not None and mortgage_amt >= 0:
                equity_ratio = max(0.0, (value_amt - mortgage_amt) / value_amt)
            evidence_used = [value_ev, mortgage_ev]
            used_fields = [str(value_ev.get("field") or "value"), "mortgage_amount"]

        if equity_ratio is not None:
            add_result(
                key=TriggerKey.EQUITY_HIGH,
                fired=equity_ratio >= 0.7,
                reason=f"equity_ratio={equity_ratio:.2f}",
                evidence=evidence_used,
                used_fields=used_fields,
            )
            add_result(
                key=TriggerKey.EQUITY_LOW,
                fired=equity_ratio <= 0.2,
                reason=f"equity_ratio={equity_ratio:.2f}",
                evidence=evidence_used,
                used_fields=used_fields,
            )
        else:
            add_missing(
                key=TriggerKey.EQUITY_HIGH,
                used_fields=["total_value", "assessed_value", "last_sale_price", "mortgage_amount"],
                reason="unavailable: equity_inputs_missing",
            )
            add_missing(
                key=TriggerKey.EQUITY_LOW,
                used_fields=["total_value", "assessed_value", "last_sale_price", "mortgage_amount"],
                reason="unavailable: equity_inputs_missing",
            )

        # ownership_long_term / ownership_short_term
        ownership_ev = _best_evidence(fields.get("last_sale_date") or [])
        if ownership_ev:
            years = _years_since(str(ownership_ev.get("value") or ""))
            if years is None:
                add_missing(
                    key=TriggerKey.OWNERSHIP_LONG_TERM,
                    used_fields=["last_sale_date"],
                    reason="unavailable: last_sale_date_invalid",
                )
                add_missing(
                    key=TriggerKey.OWNERSHIP_SHORT_TERM,
                    used_fields=["last_sale_date"],
                    reason="unavailable: last_sale_date_invalid",
                )
            else:
                add_result(
                    key=TriggerKey.OWNERSHIP_LONG_TERM,
                    fired=years >= 10,
                    reason=f"ownership_years={years}",
                    evidence=[ownership_ev],
                    used_fields=["last_sale_date"],
                )
                add_result(
                    key=TriggerKey.OWNERSHIP_SHORT_TERM,
                    fired=years <= 2,
                    reason=f"ownership_years={years}",
                    evidence=[ownership_ev],
                    used_fields=["last_sale_date"],
                )
        else:
            add_missing(
                key=TriggerKey.OWNERSHIP_LONG_TERM,
                used_fields=["last_sale_date"],
                reason="unavailable: last_sale_date_missing",
            )
            add_missing(
                key=TriggerKey.OWNERSHIP_SHORT_TERM,
                used_fields=["last_sale_date"],
                reason="unavailable: last_sale_date_missing",
            )

        for key in TriggerKey:
            if str(key) in handled_keys:
                continue
            add_missing(
                key=str(key),
                used_fields=[],
                reason="unavailable: no_evidence_rule",
            )

    return out
