from typing import Literal, List, Optional
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from dateutil import parser as dtp
import pandas as pd

from fake_db import CSVDataStore, BaseStore

# --------- Schéma & colonnes ----------
LOSS_VALUE_COLS = [f"loss_value_{i}" for i in range(1, 7)]
LOSS_VALID_COLS = [f"loss_valid_id_{i}" for i in range(1, 7)]
BASE_COLS = (
    ["hpp_id", "hpp_name", "ts_utc", "E_prod_kWh", "prod_valid_id"]
    + LOSS_VALUE_COLS
    + LOSS_VALID_COLS
)

class kWhMainPoint(BaseModel):
    ts_utc: str
    E_prod_kWh: float
    prod_valid_id: int
    loss_value_1: float
    loss_value_2: float
    loss_value_3: float
    loss_value_4: float
    loss_value_5: float
    loss_value_6: float
    loss_valid_id_1: int
    loss_valid_id_2: int
    loss_valid_id_3: int
    loss_valid_id_4: int
    loss_valid_id_5: int
    loss_valid_id_6: int

class kWhCmprPoint(BaseModel):
    ts_utc: str
    E_prod_kWh: float
    prod_valid_id: int
    loss_value_agg: float
    loss_valid_id: int
    comparator_id: int


class kWhResponse(BaseModel):
    hpp_id: int
    step: Literal["hourly", "daily", "monthly"]
    data: List[kWhMainPoint]

class kWhCmprResponse(BaseModel):
    hpp_id: int
    step: Literal["hourly", "daily", "monthly"]
    data: List[kWhCmprPoint]

# --------- Utils ----------

def parse_date_utc(s: str) -> datetime:
    """Accept date or datetime; force UTC timezone."""
    try:
        dt = dtp.parse(s)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid date: {s}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def aggregate(df: pd.DataFrame, step: Literal["hourly", "daily", "monthly"]) -> pd.DataFrame:
    if df.empty:
        return df

    if step == "hourly":
        out = df.sort_values("ts_utc").copy()
    else:
        if step == "daily":
            key = df["ts_utc"].dt.floor("D")
        elif step == "monthly":
            key = df["ts_utc"].dt.to_period("M").dt.to_timestamp()
        else:
            raise HTTPException(status_code=400, detail="Invalid time_step")

        agg_map = {"E_prod_kWh": "sum"}
        for c in LOSS_VALUE_COLS:
            agg_map[c] = "sum"

        def valid_reduce(s: pd.Series) -> int:
            return 2 if (s == 2).any() else 1

        agg_map["prod_valid_id"] = valid_reduce
        for c in LOSS_VALID_COLS:
            agg_map[c] = valid_reduce

        out = (
            df.groupby(key, as_index=False)
              .agg(agg_map)
              .rename(columns={"ts_utc": "ts"})
              .sort_values("ts")
              .rename(columns={"ts": "ts_utc"})
        )

    keep = ["ts_utc", "E_prod_kWh", "prod_valid_id"] + LOSS_VALUE_COLS + LOSS_VALID_COLS
    out = out[keep]

    out["E_prod_kWh"] = out["E_prod_kWh"].round(1)
    for c in LOSS_VALUE_COLS:
        out[c] = out[c].round(1)
    return out


def to_payload_rows(df: pd.DataFrame) -> list[dict]:
    rows = df.copy()
    rows["ts_utc"] = rows["ts_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return rows.to_dict(orient="records")


# --------- App Factory ----------

def create_app(csv_path: Optional[str] = None, store: Optional[BaseStore] = None) -> FastAPI:
    app = FastAPI(title="HPP MVP API", version="0.1.0")

    @app.on_event("startup")
    def _startup():
        s = store if store is not None else CSVDataStore(csv_path or "kWh_hourly_with_losses_ids2.csv")
        s.load()
        app.state.store = s

    @app.get("/api/v1/kWh_main", response_model=kWhResponse, summary="Get production + losses for a plant")
    def get_kWh_main(
        hpp_id: int = Query(..., description="Hydropower plant ID"),
        start_date: str = Query(..., description="UTC date/datetime inclusive, e.g. 2025-02-01 or 2025-02-01T00:00:00Z"),
        end_date: str = Query(..., description="UTC date/datetime inclusive"),
        time_step: Literal["hourly", "daily", "monthly"] = Query("hourly"),
    ):
        if not hasattr(app.state, "store"):
            raise HTTPException(status_code=500, detail="Data not initialized")

        start_dt = parse_date_utc(start_date)
        end_dt = parse_date_utc(end_date)
        if end_dt < start_dt:
            raise HTTPException(status_code=400, detail="end_date must be >= start_date")

        window = app.state.store.query(hpp_id, start_dt, end_dt)
        if window.empty:
            return kWhResponse(hpp_id=hpp_id, step=time_step, data=[])

        out = aggregate(window, time_step)
        return kWhResponse(hpp_id=hpp_id, step=time_step, data=to_payload_rows(out))

    @app.get("/api/v1/kWh_cmpr", response_model=kWhCmprResponse, summary="Get comparison of production + losses for a plant")
    def get_kWh_cmpr(
        hpp_id: int = Query(..., description="Hydropower plant ID"),
        start_date: str = Query(..., description="UTC date/datetime inclusive, e.g. 2025-02-01 or 2025-02-01T00:00:00Z"),
        end_date: str = Query(..., description="UTC date/datetime inclusive"),
        time_step: Literal["hourly", "daily", "monthly"] = Query("hourly"),
    ):
        if not hasattr(app.state, "store"):
            raise HTTPException(status_code=500, detail="Data not initialized")

        start_dt = parse_date_utc(start_date)
        end_dt = parse_date_utc(end_date)
        if end_dt < start_dt:
            raise HTTPException(status_code=400, detail="end_date must be >= start_date")

        # Build comparison series for comparator HPPs 2..5
        comps = []
        for comp_id in range(2, 6):
            # Skip echoing the same plant if asked to compare a plant within 2..5 to itself
            if comp_id == hpp_id:
                continue

            comp_window = app.state.store.query(comp_id, start_dt, end_dt)
            if comp_window.empty:
                continue

            comp_out = aggregate(comp_window, time_step)

            # Aggregate losses across categories (1..6) per row
            comp_out = comp_out.copy()
            comp_out["loss_value_agg"] = comp_out[LOSS_VALUE_COLS].sum(axis=1).round(1)
            # Validity: 2 if any category is 2 else 1
            comp_out["loss_valid_id"] = comp_out[LOSS_VALID_COLS].eq(2).any(axis=1).map(lambda x: 2 if x else 1)
            comp_out["comparator_id"] = comp_id

            # Shape to kWhCmprPoint schema
            keep_cols = [
                "ts_utc",
                "E_prod_kWh",
                "prod_valid_id",
                "loss_value_agg",
                "loss_valid_id",
                "comparator_id",
            ]
            comp_keep = comp_out[keep_cols].copy()
            # Format timestamp as Zulu
            comp_keep["ts_utc"] = comp_keep["ts_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            comps.append(comp_keep)

        if not comps:
            return kWhCmprResponse(hpp_id=hpp_id, step=time_step, data=[])

        result_df = pd.concat(comps, ignore_index=True)
        data = result_df.to_dict(orient="records")
        return kWhCmprResponse(hpp_id=hpp_id, step=time_step, data=data)

    return app
