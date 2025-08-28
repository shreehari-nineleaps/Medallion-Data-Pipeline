#!/usr/bin/env python3
"""
Parallel, hybrid forecasting pipeline.

Features:
- Granularity: daily | weekly
- Models: prophet | sarimax | lgbm
- Parallel per-entity forecasting for Prophet and SARIMAX
- Global LightGBM model (faster when many series)
- Bottom-up reconciliation option
- Uses only silver.* tables (silver.supply_orders, silver.warehouses)
- Persists to gold.forecasts (overwrite or append)
"""

import os
import logging
from typing import List, Dict, Optional, Tuple
from datetime import timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from multiprocessing import Pool, cpu_count
from functools import partial

# Models
from prophet import Prophet
from statsmodels.tsa.statespace.sarimax import SARIMAX
import lightgbm as lgb
from sklearn.preprocessing import LabelEncoder

# ---------- Configuration ----------
LOG_LEVEL = logging.INFO
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s:%(message)s")
logger = logging.getLogger("parallel_forecast")

# DB config (override with env variables)
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "supply_chain"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password123"),
}

ENGINE = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# Forecast storage options
GOLD_SCHEMA = "gold"
GOLD_TABLE = "forecasts"
OVERWRITE_GOLD = True  # if True -> delete existing rows for this run before insert; if False -> append

# Defaults
DEFAULT_HORIZON_DAYS = 90
NUM_PROCESSES = max(1, cpu_count() - 1)  # leave one core free
MIN_SERIES_LEN = 14  # min data points required to run heavier models
WEEKLY_FREQ = "W-MON"  # weekly anchored on Monday
DAILY_FREQ = "D"

# ---------- Utility DB functions ----------
def query(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, ENGINE)

def ensure_gold_table():
    create_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};
    CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.{GOLD_TABLE} (
        forecast_id SERIAL PRIMARY KEY,
        created_at TIMESTAMP DEFAULT NOW(),
        ds DATE NOT NULL,
        yhat DOUBLE PRECISION,
        yhat_lower DOUBLE PRECISION,
        yhat_upper DOUBLE PRECISION,
        granularity VARCHAR(16),
        model VARCHAR(32),
        level VARCHAR(32),
        entity_id VARCHAR(255),
        run_id VARCHAR(64)
    );
    """
    with ENGINE.begin() as conn:
        conn.execute(text(create_sql))

def save_forecasts_to_gold(df: pd.DataFrame, run_id: str, overwrite: bool = True):
    """
    df must contain columns: ds, yhat, yhat_lower, yhat_upper, granularity, model, level, entity_id
    """
    ensure_gold_table()
    if df.empty:
        logger.warning("No forecasts to save.")
        return

    with ENGINE.begin() as conn:
        if overwrite:
            # Delete rows for this run_id if existed previously (safer)
            conn.execute(text(f"DELETE FROM {GOLD_SCHEMA}.{GOLD_TABLE} WHERE run_id = :rid"), {"rid": run_id})
        # Bulk insert via pandas
        df_to_write = df.copy()
        df_to_write["created_at"] = pd.Timestamp.now()
        df_to_write["run_id"] = run_id
        # Use to_sql (psycopg2 + SQLAlchemy)
        df_to_write.to_sql(GOLD_TABLE, con=ENGINE, schema=GOLD_SCHEMA, if_exists="append", index=False)
    logger.info(f"Saved {len(df)} rows to {GOLD_SCHEMA}.{GOLD_TABLE} (run_id={run_id})")

# ---------- Data preparation ----------
def fetch_entities(level: str) -> List[str]:
    if level == "product":
        sql = "SELECT DISTINCT product_id FROM silver.supply_orders WHERE product_id IS NOT NULL ORDER BY product_id"
    elif level == "warehouse":
        sql = "SELECT DISTINCT warehouse_id FROM silver.supply_orders WHERE warehouse_id IS NOT NULL ORDER BY warehouse_id"
    elif level == "region":
        sql = "SELECT DISTINCT region FROM silver.warehouses WHERE region IS NOT NULL ORDER BY region"
    else:
        raise ValueError("level must be product|warehouse|region")
    df = query(sql)
    return df.iloc[:, 0].astype(str).tolist()

def fetch_series(level: str, entity_id: str, granularity: str = "daily") -> pd.DataFrame:
    """
    Returns DataFrame with columns ['ds','y'] aggregated at daily/weekly frequency.
    Uses silver.supply_orders and silver.warehouses for region mapping.
    """
    if granularity not in ("daily", "weekly"):
        raise ValueError("granularity must be 'daily' or 'weekly'")

    if level == "product":
        where = f"so.product_id = '{entity_id}'"
    elif level == "warehouse":
        where = f"so.warehouse_id = '{entity_id}'"
    elif level == "region":
        where = f"w.region = '{entity_id}'"
    else:
        raise ValueError("level must be product|warehouse|region")

    if level == "region":
        join = "JOIN silver.warehouses w ON so.warehouse_id = w.warehouse_id"
    else:
        join = "LEFT JOIN silver.warehouses w ON so.warehouse_id = w.warehouse_id"

    date_trunc = "DATE_TRUNC('week', so.order_date)::date" if granularity == "weekly" else "so.order_date::date"
    group_by = "1"  # we use the truncated date select position

    sql = f"""
    SELECT {date_trunc} AS ds, SUM(so.quantity) AS y
    FROM silver.supply_orders so
    {join}
    WHERE {where}
    GROUP BY {group_by}
    ORDER BY ds;
    """
    df = query(sql)
    if df.empty:
        return df
    df["ds"] = pd.to_datetime(df["ds"]).dt.date
    # reindex to continuous date range
    freq = WEEKLY_FREQ if granularity == "weekly" else DAILY_FREQ
    start = pd.Timestamp(df["ds"].min())
    end = pd.Timestamp(df["ds"].max())
    rng = pd.date_range(start=start, end=end, freq=freq)
    full = pd.DataFrame({"ds": rng.date})
    merged = full.merge(df, on="ds", how="left").fillna(0)
    merged["y"] = merged["y"].astype(float)
    return merged

# ---------- Modeling helpers ----------
def fit_prophet_and_forecast(series_df: pd.DataFrame, horizon: int, granularity: str) -> pd.DataFrame:
    """
    Train Prophet and return forecast DataFrame with ds,yhat,yhat_lower,yhat_upper
    """
    if series_df.empty or len(series_df) < MIN_SERIES_LEN:
        return pd.DataFrame()
    df = series_df.rename(columns={"ds": "ds", "y": "y"}).copy()
    df["ds"] = pd.to_datetime(df["ds"])
    # Choose seasonality options
    m = Prophet(daily_seasonality=(granularity == "daily"), yearly_seasonality=True, weekly_seasonality=(granularity=="daily"))
    m.fit(df)
    future = m.make_future_dataframe(periods=horizon, freq="D" if granularity == "daily" else "W")
    fc = m.predict(future)
    out = fc[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    out["ds"] = out["ds"].dt.date
    return out.tail(horizon)  # return only horizon rows

def fit_sarimax_and_forecast(series_df: pd.DataFrame, horizon: int, granularity: str) -> pd.DataFrame:
    """
    Fit a lightweight SARIMAX model (auto-order not implemented; use simple seasonal ARIMA).
    We choose (p,d,q)=(1,1,1) and seasonal (1,1,1,s)
    """
    if series_df.empty or len(series_df) < MIN_SERIES_LEN:
        return pd.DataFrame()
    ts = pd.Series(series_df["y"].values, index=pd.to_datetime(series_df["ds"]))
    # choose seasonal period
    s = 52 if granularity == "weekly" else 7
    try:
        model = SARIMAX(ts, order=(1, 1, 1), seasonal_order=(1, 1, 1, s), enforce_stationarity=False, enforce_invertibility=False)
        res = model.fit(disp=False, maxiter=50)
        pred = res.get_forecast(steps=horizon)
        mean = pred.predicted_mean
        ci = pred.conf_int(alpha=0.05)
        out = pd.DataFrame({
            "ds": pd.date_range(start=ts.index[-1] + (pd.Timedelta(weeks=1) if s==52 else pd.Timedelta(days=1)), periods=horizon, freq=("W" if granularity=="weekly" else "D")),
            "yhat": mean.values,
            "yhat_lower": ci.iloc[:, 0].values,
            "yhat_upper": ci.iloc[:, 1].values
        })
        out["ds"] = out["ds"].dt.date
        return out
    except Exception as e:
        logger.warning(f"SARIMAX failed: {e}")
        return pd.DataFrame()

# ---------- Per-entity worker (for parallel) ----------
def worker_forecast_entity(args: Tuple):
    """Unpack args and run forecasting for one entity with chosen model."""
    level, ent, model_name, horizon, granularity = args
    try:
        series_df = fetch_series(level, ent, granularity=granularity)
        if series_df.empty or len(series_df) < MIN_SERIES_LEN:
            logger.debug(f"[{model_name}] Skip {level}:{ent} (len={len(series_df)})")
            return pd.DataFrame()

        if model_name == "prophet":
            out = fit_prophet_and_forecast(series_df, horizon, granularity)
        elif model_name == "sarimax":
            out = fit_sarimax_and_forecast(series_df, horizon, granularity)
        else:
            logger.warning(f"Unknown model for per-entity worker: {model_name}")
            return pd.DataFrame()

        if out.empty:
            return pd.DataFrame()

        out["granularity"] = granularity
        out["model"] = model_name
        out["level"] = level
        out["entity_id"] = str(ent)
        return out[["ds", "yhat", "yhat_lower", "yhat_upper", "granularity", "model", "level", "entity_id"]]
    except Exception as e:
        logger.exception(f"Worker failed for {level}:{ent}: {e}")
        return pd.DataFrame()

# ---------- Global LightGBM approach ----------
def prepare_panel_dataset(levels: List[str], granularity: str, lags: List[int] = [1,7,14]) -> Tuple[pd.DataFrame, Dict]:
    """
    Build panel dataset with features for LightGBM.
    Returns (train_df, meta) where train_df has columns: ds, entity, y, features...
    meta contains entity list per level.
    """
    records = []
    meta = {}
    for level in levels:
        entities = fetch_entities(level)
        meta[level] = entities
        for ent in entities:
            ser = fetch_series(level, ent, granularity=granularity)
            if ser.empty or len(ser) < max(lags) + 2:
                continue
            ser = ser.rename(columns={"ds": "ds", "y": "y"})
            ser["ds"] = pd.to_datetime(ser["ds"])
            ser = ser.sort_values("ds").reset_index(drop=True)
            for i in range(max(lags), len(ser)):
                row = {"ds": ser.loc[i, "ds"], "entity": f"{level}__{ent}", "y": ser.loc[i, "y"]}
                # add time features
                row["dow"] = ser.loc[i, "ds"].dayofweek
                row["month"] = ser.loc[i, "ds"].month
                row["weekofyear"] = ser.loc[i, "ds"].isocalendar().week
                # lags
                for lag in lags:
                    row[f"lag_{lag}"] = ser.loc[i - lag, "y"]
                # rolling
                row["roll_mean_7"] = ser.loc[max(0, i-6):i, "y"].mean()
                records.append(row)
    train_df = pd.DataFrame.from_records(records)
    return train_df, meta

def train_lgbm_and_predict(levels: List[str], horizon: int, granularity: str, lags: List[int] = [1,7,14]) -> pd.DataFrame:
    """
    Train a single LightGBM model on the panel dataset and predict horizon for all entities.
    We do autoregressive prediction for each entity: predict 1 step, append, predict next, etc.
    """
    train_df, meta = prepare_panel_dataset(levels, granularity, lags)
    if train_df.empty:
        logger.warning("No training data for LGBM.")
        return pd.DataFrame()

    # encode entity
    le = LabelEncoder()
    train_df["entity_code"] = le.fit_transform(train_df["entity"])
    features = [c for c in train_df.columns if c not in ("ds", "entity", "y")]
    X = train_df[features]
    y = train_df["y"]
    lgb_train = lgb.Dataset(X, y)
    params = {"objective": "regression", "metric": "rmse", "verbosity": -1}
    bst = lgb.train(params, lgb_train, num_boost_round=200)

    # Predict for each entity autoregressively
    all_preds = []
    for level in levels:
        entities = meta.get(level, [])
        for ent in entities:
            ent_key = f"{level}__{ent}"
            # fetch original series to initialize lags
            ser = fetch_series(level, ent, granularity=granularity)
            if ser.empty:
                continue
            ser = ser.sort_values("ds").reset_index(drop=True)
            hist = list(ser["y"].values)
            last_date = pd.to_datetime(ser["ds"].iloc[-1])
            # iterative forecast
            preds = []
            for step in range(1, horizon + 1):
                # build features for current step
                ds = (last_date + pd.Timedelta(days=step)) if granularity == "daily" else (last_date + pd.offsets.Week(1) * step)
                feat = {"entity": ent_key,
                        "dow": ds.dayofweek,
                        "month": ds.month,
                        "weekofyear": ds.isocalendar().week if granularity == "daily" else ds.isocalendar().week}
                # lags: use hist
                for lag in lags:
                    feat[f"lag_{lag}"] = hist[-lag] if len(hist) >= lag else 0.0
                feat["roll_mean_7"] = np.mean(hist[-7:]) if len(hist) >= 1 else 0.0
                feat["entity_code"] = le.transform([ent_key])[0] if ent_key in le.classes_ else -1
                X_pred = pd.DataFrame([feat])[features]
                y_pred = bst.predict(X_pred)[0]
                preds.append((ds.date(), max(0.0, float(y_pred))))
                # append predicted value to history for next step (autoregressive)
                hist.append(y_pred)
            # build out rows
            for ds, yhat in preds:
                all_preds.append({
                    "ds": ds, "yhat": yhat, "yhat_lower": None, "yhat_upper": None,
                    "granularity": granularity, "model": "lgbm", "level": level, "entity_id": str(ent)
                })
    return pd.DataFrame(all_preds)

# ---------- Orchestration ----------
def run_parallel_forecasts(levels: List[str], model: str = "prophet", granularity: str = "daily",
                           horizon: int = DEFAULT_HORIZON_DAYS, parallel: bool = True,
                           run_id: Optional[str] = None, overwrite_gold: bool = True,
                           bottom_up_reconcile: bool = False, sample_limit: Optional[int] = None) -> pd.DataFrame:
    """
    top-level function:
    - levels: list of 'product','warehouse','region'
    - model: 'prophet'|'sarimax'|'lgbm'
    - granularity: 'daily'|'weekly'
    """
    if run_id is None:
        run_id = f"run_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}"
    logger.info(f"Starting run {run_id} model={model} granularity={granularity} horizon={horizon} levels={levels}")

    all_results = []

    if model == "lgbm":
        # global model
        preds_df = train_lgbm_and_predict(levels, horizon, granularity)
        if not preds_df.empty:
            all_results.append(preds_df)
    else:
        # per-entity models (can parallelize)
        tasks = []
        for level in levels:
            entities = fetch_entities(level)
            if sample_limit:
                entities = entities[:sample_limit]
            for ent in entities:
                tasks.append((level, ent, model, horizon, granularity))

        logger.info(f"Prepared {len(tasks)} tasks for per-entity modeling.")
        if parallel and len(tasks) > 0:
            logger.info(f"Using {NUM_PROCESSES} processes for parallel forecasting.")
            with Pool(processes=NUM_PROCESSES) as pool:
                for out in pool.imap_unordered(worker_forecast_entity, tasks):
                    if out is None or out.empty:
                        continue
                    all_results.append(out)
        else:
            # sequential
            for t in tasks:
                out = worker_forecast_entity(t)
                if out is None or out.empty:
                    continue
                all_results.append(out)

    if not all_results:
        logger.warning("No forecasts were produced.")
        return pd.DataFrame()

    combined = pd.concat(all_results, ignore_index=True, sort=False)

    # Optional bottom-up reconciliation (simple approach):
    if bottom_up_reconcile:
        logger.info("Running bottom-up reconciliation (product -> warehouse -> region)")
        # Only if product forecasts exist: aggregate up
        try:
            prod = combined[combined["level"] == "product"]
            if not prod.empty:
                # aggregate product -> warehouse: requires mapping product->warehouse via silver.supply_orders
                # We'll compute product*date sums and then map using historical proportions (approx)
                # Simpler approach: if both product and warehouse forecasts exist, replace warehouse by sum(product)
                aggregated_wh = prod.groupby(["ds", "entity_id"], dropna=False)["yhat"].sum().reset_index()
                # This is a placeholder — a proper reconciliation would need product->warehouse mapping
                # Not implementing full mapping automatically here due to missing direct mapping.
                logger.info("Note: bottom-up reconciliation placeholder — implement mapping for exact reconciliation.")
        except Exception:
            logger.exception("Reconciliation failed; continuing without it.")

    # attach metadata columns if missing (for lgbm we created them)
    if "granularity" not in combined.columns:
        combined["granularity"] = granularity
    if "model" not in combined.columns:
        combined["model"] = model
    if "level" not in combined.columns:
        combined["level"] = combined.get("level", "unknown")
    if "entity_id" not in combined.columns:
        combined["entity_id"] = combined.get("entity_id", "ALL")

    # ensure columns in right format
    combined = combined.rename(columns={"ds": "ds", "yhat": "yhat"})
    combined["ds"] = pd.to_datetime(combined["ds"]).dt.date
    # set null bounds to mean +/- 20% if absent
    if "yhat_lower" not in combined.columns or combined["yhat_lower"].isnull().all():
        combined["yhat_lower"] = (combined["yhat"] * 0.8).astype(float)
    if "yhat_upper" not in combined.columns or combined["yhat_upper"].isnull().all():
        combined["yhat_upper"] = (combined["yhat"] * 1.2).astype(float)

    # Save to gold
    save_forecasts_to_gold(combined[["ds", "yhat", "yhat_lower", "yhat_upper", "granularity", "model", "level", "entity_id"]], run_id, overwrite=overwrite_gold)

    logger.info(f"Run {run_id} completed. Total forecast rows: {len(combined)}")
    return combined

# ---------- Example / CLI ----------
if __name__ == "__main__":
    # parameters you can change
    levels = ["product", "warehouse", "region"]
    model = "lgbm"         # prophet | sarimax | lgbm
    granularity = "weekly" # daily | weekly  (weekly much faster)
    horizon = 12 if granularity == "weekly" else DEFAULT_HORIZON_DAYS  # 12 weeks (~3 months) or 90 days
    parallel = True
    run_id = None
    overwrite_gold = True
    bottom_up_reconcile = False
    sample_limit = None  # can set small number for quick testing e.g. 50

    result_df = run_parallel_forecasts(levels=levels, model=model, granularity=granularity,
                                       horizon=horizon, parallel=parallel, run_id=run_id,
                                       overwrite_gold=overwrite_gold, bottom_up_reconcile=bottom_up_reconcile,
                                       sample_limit=sample_limit)

    if not result_df.empty:
        logger.info(result_df.head(20).to_string(index=False))
    else:
        logger.warning("No results generated.")
