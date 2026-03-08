"""
BMRS adapter — FUELHH dataset.

Half-hourly electricity generation by fuel type in Great Britain.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, List

import pandas as pd
import requests

from app.ingestion.core.base_adapter import BaseAdapter
from app.ingestion.transformer import clean_json_payload


FUELHH_ENDPOINT = "https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELHH"


def _make_series_id(dataset_id: str, fuel_type: str) -> str:
    """Create canonical BMRS series id."""
    slug = fuel_type.upper().replace(" ", "_")
    return f"BMRS_{dataset_id}_{slug}"


def _settlement_to_timestamp(start_time: str) -> datetime:
    """
    Convert settlement startTime to UTC timestamp.

    Example:
    2026-03-08T11:00:00Z
    """
    return pd.to_datetime(start_time, utc=True)


class BmrsFuelHHAdapter(BaseAdapter):
    """Adapter for BMRS FUELHH dataset."""

    DATASET_ID = "FUELHH"

    def fetch(
        self,
        from_date=None,
        to_date=None,
        fuel_types=None,
        settlement_date=None,
        settlement_period=None,
        **kwargs
    ):

        params = {}

        if from_date:
            params["from"] = from_date

        if to_date:
            params["to"] = to_date

        if fuel_types:
            params["fuelType"] = ",".join(fuel_types)

        if settlement_date:
            params["settlementDate"] = settlement_date

        if settlement_period:
            params["settlementPeriod"] = settlement_period

        response = requests.get(FUELHH_ENDPOINT, params=params, timeout=60)
        response.raise_for_status()

        payload = response.json()
        data = payload.get("data", [])

        return pd.DataFrame(data)

    def parse(self, raw: Any) -> List[Any]:
        """Convert DataFrame → list of row dicts."""
        if not isinstance(raw, pd.DataFrame):
            raise TypeError("FUELHH adapter expects DataFrame from fetch")

        if raw.empty:
            return []

        return [row.to_dict() for _, row in raw.iterrows()]

    def normalize(self, record: Any) -> List[dict]:
        """
        Convert one raw BMRS record → one canonical observation.
        """
        if not isinstance(record, dict):
            return []

        fuel_type = record.get("fuelType")
        generation = record.get("generation")
        start_time = record.get("startTime")

        if fuel_type is None or generation is None or start_time is None:
            return []

        series_id = _make_series_id(self.DATASET_ID, fuel_type)

        observation_time = _settlement_to_timestamp(start_time)

        return [{
            "series_id": series_id,
            "observation_time": observation_time,
            "value": float(generation),
            "quality_flag": None,
            "raw_payload": clean_json_payload(record),
        }]

    def define_series(self, normalized_records: List[Any]) -> List[dict]:
        """Generate metadata for discovered fuel type series."""
        seen = set()
        out = []

        for r in normalized_records:
            if not isinstance(r, dict):
                continue

            sid = r.get("series_id")
            if not sid or sid in seen:
                continue

            seen.add(sid)

            fuel_type = sid.split("_")[-1]

            out.append({
                "series_id": sid,
                "source": "BMRS",
                "dataset_id": self.DATASET_ID,
                "data_item": fuel_type,
                "description": f"{fuel_type} electricity generation in Great Britain",
                "unit": "MW",
                "frequency": "half_hourly",
                "timezone_source": "UTC",
                "is_active": True,
            })

        return out

    def get_time_field(self) -> str:
        return "observation_time"
    

"""
The orchestrator will then automatically:

1. store raw payload

2. run validation

3. register series

4. upsert into data_observations

5. track ingestion_runs

# Observation Shema
- series_id
- observation_time
- value
- quality_flag
- raw_payload  

"""