"""
Validation script for BMRS ingestion.

Run from project root:

python -m scripts.validate_api.validate_bmrs
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from sqlalchemy import text
from app.db.connection import engine
from app.ingestion.core import registry, Orchestrator
import app.ingestion.adapters  # noqa: F401


DATASETS = ["FUELHH", "DEMAND_OUTTURN"]


def get_counts(dataset):
    with engine.connect() as conn:
        raw = conn.execute(
            text("SELECT COUNT(*) FROM raw_events WHERE dataset_id = :d"),
            {"d": dataset},
        ).scalar()

        obs = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM data_observations
                WHERE series_id LIKE :prefix
            """),
            {"prefix": f"BMRS_{dataset}%"},
        ).scalar()

    return int(raw), int(obs)


def run_dataset(dataset):

    print("=" * 60)
    print(f"Validating dataset: {dataset}")
    print("=" * 60)

    raw_before, obs_before = get_counts(dataset)

    print(f"Before: raw_events={raw_before}, observations={obs_before}")

    orch = Orchestrator(registry)
    orch.run(dataset)

    raw_after, obs_after = get_counts(dataset)

    print(f"After: raw_events={raw_after}, observations={obs_after}")

    if raw_after >= raw_before and obs_after >= obs_before:
        print("Validation PASSED")
    else:
        print("Validation FAILED")
        sys.exit(1)


def main():
    for d in DATASETS:
        run_dataset(d)


if __name__ == "__main__":
    main()