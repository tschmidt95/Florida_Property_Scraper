import hashlib
from typing import Dict, List, Optional, Tuple

from florida_property_scraper.normalize import normalize_address, normalize_text


def compute_property_uid(
    item: Dict[str, object],
) -> Tuple[Optional[str], Optional[str], List[str]]:
    warnings: List[str] = []
    county = (
        (item.get("county") or "").strip()
        if isinstance(item.get("county"), str)
        else ""
    )
    if not county:
        warnings.append("Missing county; cannot compute stable property_uid.")
        return None, None, warnings
    parcel_id = item.get("parcel_id")
    parcel_id_value = str(parcel_id).strip() if parcel_id not in (None, "") else ""
    if parcel_id_value:
        return f"{county}:{parcel_id_value}", parcel_id_value, warnings
    situs = (
        normalize_address(item.get("situs_address"))
        if isinstance(item.get("situs_address"), str)
        else ""
    )
    owner = (
        normalize_text(item.get("owner_name"))
        if isinstance(item.get("owner_name"), str)
        else ""
    )
    fallback_seed = f"{county}|{situs}|{owner}"
    if not situs and not owner:
        warnings.append(
            "Missing parcel_id and address/owner; fallback identity may be unstable."
        )
    warnings.append("Used fallback identity (county+situs_address+owner_name hash).")
    digest = hashlib.sha256(fallback_seed.encode("utf-8")).hexdigest()
    return f"{county}:{digest}", None, warnings
