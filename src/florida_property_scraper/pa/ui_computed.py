from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Optional


def _norm_addr(v: Optional[str]) -> str:
    if not v:
        return ""
    # Normalize only for comparison; do not infer new data.
    return " ".join(str(v).strip().upper().split())


def _parse_iso_date(v: Optional[str]) -> Optional[date]:
    if not v:
        return None
    try:
        return datetime.strptime(v, "%Y-%m-%d").date()
    except Exception:
        return None


def compute_ui_fields(pa: Optional[Dict[str, Any]], *, today: Optional[date] = None) -> Dict[str, Any]:
    """Compute UI-only fields derived strictly from PA data.

    Rules (PA-only):
    - If the necessary source field is missing, return null/false/0 as appropriate.
    - Never infer data from non-PA sources.
    """

    if today is None:
        today = date.today()

    if not pa:
        return {
            "absentee": False,
            "ownership_years": None,
            "out_of_state_owner": False,
        }

    situs = _norm_addr(pa.get("situs_address"))
    mailing = _norm_addr(pa.get("mailing_address"))

    absentee = False
    if mailing:
        # Only compare when mailing exists.
        absentee = bool(situs and mailing and mailing != situs)

    last_sale_date = _parse_iso_date(pa.get("last_sale_date"))
    ownership_years: Optional[int] = None
    if last_sale_date is not None:
        days = (today - last_sale_date).days
        if days >= 0:
            ownership_years = int(days // 365.25)

    mailing_state = (pa.get("mailing_state") or "").strip().upper()
    out_of_state_owner = bool(mailing_state and mailing_state != "FL")

    return {
        "absentee": bool(absentee),
        "ownership_years": ownership_years,
        "out_of_state_owner": bool(out_of_state_owner),
    }
