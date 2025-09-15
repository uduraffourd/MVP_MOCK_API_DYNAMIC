from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone
import pandas as pd
import os

# Colonnes attendues (alignées avec api.py)
LOSS_VALUE_COLS = [f"loss_value_{i}" for i in range(1, 7)]
LOSS_VALID_COLS = [f"loss_valid_id_{i}" for i in range(1, 7)]
BASE_COLS = (
    ["hpp_id", "hpp_name", "ts_utc", "E_prod_kWh", "prod_valid_id"]
    + LOSS_VALUE_COLS
    + LOSS_VALID_COLS
)

class BaseStore:
    """Interface minimale pour interchanger CSV/DB"""
    def load(self) -> None:  # pragma: no cover
        raise NotImplementedError

    def query(self, hpp_id: int, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

@dataclass
class CSVDataStore(BaseStore):
    csv_path: str
    df: Optional[pd.DataFrame] = None

    def load(self) -> None:
        if not os.path.exists(self.csv_path):
            raise RuntimeError(f"CSV not found at {self.csv_path}")
        df = pd.read_csv(self.csv_path)

        missing = [c for c in BASE_COLS if c not in df.columns]
        if missing:
            raise RuntimeError(f"CSV missing columns: {missing}")

        # ts_utc en UTC-naïf (sortie formatée en Z côté API)
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True).dt.tz_convert(timezone.utc).dt.tz_localize(None)

        # Dtypes
        df["hpp_id"] = df["hpp_id"].astype(int)
        df["E_prod_kWh"] = df["E_prod_kWh"].astype(float)
        df["prod_valid_id"] = df["prod_valid_id"].astype(int)
        for c in LOSS_VALUE_COLS:
            df[c] = df[c].astype(float)
        for c in LOSS_VALID_COLS:
            df[c] = df[c].astype(int)

        self.df = df

    def query(self, hpp_id: int, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        if self.df is None:
            raise RuntimeError("DataStore not initialized")
        sdf = self.df[self.df["hpp_id"] == hpp_id]
        if sdf.empty:
            # Laisse l'API décider du 404
            return sdf
        s_naive = start_dt.replace(tzinfo=None)
        e_naive = end_dt.replace(tzinfo=None)
        window = sdf[(sdf["ts_utc"] >= s_naive) & (sdf["ts_utc"] <= e_naive)].copy()
        return window