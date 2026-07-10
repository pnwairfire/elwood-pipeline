"""
Detect low-cost sensor outliers in the FASM AQ feed using information-theoretic
metrics from the elwood-spatial package.
"""

try:
    from prefect import task, flow
except ImportError:
    def task(fn=None, **kwargs):
        if fn is None:
            return lambda f: f
        return fn
    def flow(fn=None, **kwargs):
        if fn is None:
            return lambda f: f
        return fn

import io
import json
import logging
from datetime import UTC, datetime

import geopandas as gpd
import pandas as pd
from elwood_spatial.air_quality import AQI_MODIFIED_BINS
from elwood_spatial.detect import PARAMS_OPERATIONAL, detect_outliers
from elwood_spatial.network import filter_network

from elwood_pipeline.s3 import fasm_layers_bucket, init_epa_s3

logger = logging.getLogger(__name__)

AQ_KEY_PREFIX = "elwood/hourly_aq"
NETWORK_KEY_PREFIX = "elwood/hourly_weights"

SENSOR_UNIT_TYPES = {"PurpleAir", "SensOR", "SensWA", "Clarity"}
MIN_HOURS_PRESENT = 3
CV_THRESHOLD = 7.5
HIGH_AQI_THRESHOLD = 200


def _hour_ts() -> int:
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    return int(now.timestamp())


def _s3_get_bytes(s3, bucket: str, key: str) -> bytes:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


@task
def collect_aq_df(ts: int):
    s3 = init_epa_s3()
    bucket = fasm_layers_bucket()
    frames = []
    for offset in (0, 3600, 7200):
        key = f"{AQ_KEY_PREFIX}/{ts - offset}.parquet"
        try:
            data = _s3_get_bytes(s3, bucket, key)
            frame = gpd.read_parquet(io.BytesIO(data))
            frame["ts"] = ts - offset
            frames.append(frame)
        except Exception as exc:
            logger.warning(f"Skipping s3://{bucket}/{key}: {exc}")

    if not frames:
        raise RuntimeError("No hourly_aq parquet files could be loaded")

    merged = pd.concat(frames, ignore_index=True).sort_values("ts")
    counts = merged["unit_id"].value_counts()
    units_to_drop = set(counts[counts < MIN_HOURS_PRESENT].index)

    kept = merged[~merged["unit_id"].isin(units_to_drop)]
    aq_df = (
        kept.groupby("unit_id")
        .agg(
            {
                "aqi": "mean",
                "unit_type": "last",
                "cat": "last",
            }
        )
        .reset_index()
    )

    logger.info(
        f"AGGREGATED {len(aq_df)} units ({len(units_to_drop)} dropped "
        f"for <{MIN_HOURS_PRESENT}h coverage)"
    )
    return aq_df, units_to_drop


@task
def collect_network(ts: int, aq_df: pd.DataFrame) -> dict:
    s3 = init_epa_s3()
    data = _s3_get_bytes(s3, fasm_layers_bucket(), f"{NETWORK_KEY_PREFIX}/{ts}.json")
    network_raw = json.loads(data)

    active_ids = set(aq_df["unit_id"].astype(str).tolist())
    filtered = filter_network(network_raw, active_ids=active_ids, min_neighbors=1)
    logger.info(f"NETWORK filtered to {len(filtered)} units with active neighbors")
    return filtered


@task
def find_outliers(aq_df: pd.DataFrame, network: dict) -> list[str]:
    values = {str(r.unit_id): r.aqi for r in aq_df.itertuples(index=False)}

    sensor_ids = set(
        aq_df[aq_df["unit_type"].isin(SENSOR_UNIT_TYPES)]["unit_id"].astype(str)
    )
    sensor_network = {k: v for k, v in network.items() if k in sensor_ids}

    flags = detect_outliers(
        values=values,
        bins=AQI_MODIFIED_BINS,
        network=sensor_network,
        params=PARAMS_OPERATIONAL,
    )
    outliers = sorted({uid for uid, is_out in flags.items() if is_out})
    logger.info(f"DETECTED {len(outliers)} outliers from {len(sensor_network)} sensors")
    return outliers


def _measure_cv(series: pd.Series) -> float:
    mean = series.mean()
    if mean == 0:
        return 0.0
    return (series.std() / mean) * 100


@task
def cv_amendment(aq_df: pd.DataFrame, outliers: list[str]) -> list[str]:
    high = aq_df[
        (aq_df["aqi"] >= HIGH_AQI_THRESHOLD)
        & (aq_df["unit_type"].isin(SENSOR_UNIT_TYPES))
    ][["unit_id", "unit_type"]].to_dict(orient="records")

    add_set, drop_set = set(), set()
    for unit in high:
        unit_id = str(unit["unit_id"])
        unit_type = unit["unit_type"]
        try:
            if unit_type == "PurpleAir":
                url = f"https://airfire-data-exports.s3.us-west-2.amazonaws.com/maps/purple_air/v4/timeseries/weekly/{unit_id}.csv"
                data = pd.read_csv(url).dropna(subset=["epa_nowcast"])
                data["local_ts"] = pd.to_datetime(data["local_ts"])
                cutoff = data["local_ts"].iloc[-1] - pd.Timedelta(hours=24)
                recent = data[data["local_ts"] >= cutoff]
                if len(recent) < 12:
                    continue
                cv = _measure_cv(recent["epa_nowcast"])
            elif unit_type in ("SensOR", "SensWA", "Clarity"):
                if unit_type == "Clarity":
                    url = f"https://airfire-data-exports.s3.us-west-2.amazonaws.com/sensors/v3/PM2.5/latest/csv/{unit_id}.csv"
                else:
                    url = f"https://s3-us-west-2.amazonaws.com/airfire-data-exports/monitoring/v2/latest/csv/{unit_id}.csv"
                data = pd.read_csv(url).dropna(subset=["nowcast"])
                if len(data) <= 12:
                    continue
                cv = _measure_cv(data["nowcast"].tail(24))
            else:
                continue
        except Exception as exc:
            logger.warning(f"CV check failed for {unit_id}: {exc}")
            continue

        if cv <= CV_THRESHOLD:
            add_set.add(unit_id)
        else:
            drop_set.add(unit_id)

    result = [u for u in outliers if u not in drop_set] + [
        u for u in add_set if u not in outliers
    ]
    logger.info(
        f"CV amendment: +{len(add_set)} -{len(drop_set)} → {len(result)} outliers"
    )
    return result


@task
def write_current_to_s3(outliers: list[str]):
    s3 = init_epa_s3()
    s3.put_object(
        Bucket=fasm_layers_bucket(),
        Key="elwood/outliers/current.json",
        Body=json.dumps(outliers).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"WROTE current.json with {len(outliers)} outliers")


@flow
def run():
    ts = _hour_ts()
    logger.info(f"Running elwood outliers for ts={ts}")

    aq_df, _ = collect_aq_df(ts)
    network = collect_network(ts, aq_df)
    base_outliers = find_outliers(aq_df, network)
    amended = cv_amendment(aq_df, base_outliers)
    write_current_to_s3(amended)
    return f"outliers detected for ts={ts}: {len(amended)}"
