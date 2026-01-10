from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Literal


FieldType = Literal["str", "int", "float", "bool", "date"]
Op = Literal[
    "equals",
    "not_equals",
    "contains",
    "gte",
    "lte",
    "between",
    "in_list",
    "is_true",
]


@dataclass(frozen=True)
class FieldDefinition:
    name: str
    type: FieldType
    ui_label: str
    ops_supported: List[Op]
    db_column: str


# Checkbox-driven filter registry (extend over time).
FILTER_FIELDS: Dict[str, FieldDefinition] = {
    "county": FieldDefinition(
        name="county",
        type="str",
        ui_label="County",
        ops_supported=["equals", "not_equals", "in_list"],
        db_column="county",
    ),
    "parcel_id": FieldDefinition(
        name="parcel_id",
        type="str",
        ui_label="Parcel ID",
        ops_supported=["equals", "not_equals", "contains"],
        db_column="parcel_id",
    ),
    "zip": FieldDefinition(
        name="zip",
        type="str",
        ui_label="ZIP",
        ops_supported=["equals", "not_equals", "in_list"],
        db_column="zip",
    ),
    "land_use_code": FieldDefinition(
        name="land_use_code",
        type="str",
        ui_label="Land Use Code",
        ops_supported=["equals", "not_equals", "in_list"],
        db_column="land_use_code",
    ),
    "year_built": FieldDefinition(
        name="year_built",
        type="int",
        ui_label="Year Built",
        ops_supported=["equals", "not_equals", "gte", "lte", "between"],
        db_column="year_built",
    ),
    "building_sf": FieldDefinition(
        name="building_sf",
        type="float",
        ui_label="Building SF",
        ops_supported=["equals", "not_equals", "gte", "lte", "between"],
        db_column="building_sf",
    ),
    "last_sale_date": FieldDefinition(
        name="last_sale_date",
        type="date",
        ui_label="Last Sale Date",
        ops_supported=["equals", "not_equals", "gte", "lte", "between"],
        db_column="last_sale_date",
    ),
    "last_sale_price": FieldDefinition(
        name="last_sale_price",
        type="float",
        ui_label="Last Sale Price",
        ops_supported=["equals", "not_equals", "gte", "lte", "between"],
        db_column="last_sale_price",
    ),
    "assessed_value": FieldDefinition(
        name="assessed_value",
        type="float",
        ui_label="Assessed Value",
        ops_supported=["equals", "not_equals", "gte", "lte", "between"],
        db_column="assessed_value",
    ),
}
