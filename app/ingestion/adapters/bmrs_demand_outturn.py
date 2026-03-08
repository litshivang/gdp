"""
BMRS adapter — DEMAND_OUTTURN dataset.

Half-hourly electricity demand in Great Britain.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, List

import pandas as pd
import requests

from app.ingestion.core.base_adapter import BaseAdapter
from app.ingestion.transformer import clean_json_payload


DEMAND_ENDPOINT = "https://data.elexon.co.uk/bmrs/api/v1/datasets/ITSDO"


def _make_series_id(dataset_id: str) -> str:
    """Create canonical BMRS series id."""
    return f"BMRS_{dataset_id}_DEMAND"


def _settlement_to_timestamp(date_str: str, period: int) -> datetime:
    """
    Convert settlementDate + settlementPeriod → UTC timestamp.

    Period mapping:
    1 → 00:00
    2 → 00:30
    ...
    48 → 23:30
    """
    base = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    offset = timedelta(minutes=(period - 1) * 30)
    return base + offset


class BmrsDemandOutturnAdapter(BaseAdapter):
    """Adapter for BMRS DEMAND_OUTTURN dataset."""

    DATASET_ID = "DEMAND_OUTTURN"

    def fetch(
        self,
        from_date=None,
        to_date=None,
        settlement_date=None,
        settlement_period=None,
        **kwargs
    ):

        params = {}

        if from_date:
            params["from"] = from_date

        if to_date:
            params["to"] = to_date

        if settlement_date:
            params["settlementDate"] = settlement_date

        if settlement_period:
            params["settlementPeriod"] = settlement_period

        response = requests.get(DEMAND_ENDPOINT, params=params, timeout=60)
        response.raise_for_status()

        payload = response.json()
        data = payload.get("data", [])

        if not data:
            return pd.DataFrame()

        return pd.DataFrame(data)

    def parse(self, raw: Any) -> List[Any]:
        """DataFrame → list of row dicts."""
        if not isinstance(raw, pd.DataFrame):
            raise TypeError("DEMAND_OUTTURN adapter expects DataFrame from fetch")

        if raw.empty:
            return []

        return [row.to_dict() for _, row in raw.iterrows()]

    def normalize(self, record: Any) -> List[dict]:
        """Convert raw BMRS demand record → canonical observation."""
        if not isinstance(record, dict):
            return []

        settlement_date = record.get("settlementDate")
        settlement_period = record.get("settlementPeriod")
        demand = record.get("demand")

        if settlement_date is None or settlement_period is None or demand is None:
            return []

        observation_time = _settlement_to_timestamp(
            settlement_date,
            int(settlement_period),
        )

        return [{
            "series_id": _make_series_id(self.DATASET_ID),
            "observation_time": observation_time,
            "value": float(demand),
            "quality_flag": None,
            "raw_payload": clean_json_payload(record),
        }]

    def define_series(self, normalized_records: List[Any]) -> List[dict]:
        """Return canonical series metadata."""
        series_id = _make_series_id(self.DATASET_ID)

        return [{
            "series_id": series_id,
            "source": "BMRS",
            "dataset_id": self.DATASET_ID,
            "data_item": "DEMAND",
            "description": "Electricity demand outturn in Great Britain",
            "unit": "MW",
            "frequency": "half_hourly",
            "timezone_source": "UTC",
            "is_active": True,
        }]

    def get_time_field(self) -> str:
        return "observation_time"
    

"""
Adapter Responsibility:

fetch
parse
normalize
define_series
get_time_field


The orchestrator will automatically perform:

raw_events ingestion
validation
delete policy
series registration
data_observations upsert
ingestion_runs tracking

"""    