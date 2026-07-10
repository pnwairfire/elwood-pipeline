"""
Manage the unit_status state machine for elwood outliers.
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

import ast
import io
import json
import logging

import pandas as pd
from botocore.exceptions import ClientError
from psycopg2.extras import execute_values

from elwood_pipeline.db import get_ts_db_conn
from elwood_pipeline.s3 import fasm_layers_bucket, init_epa_s3

logger = logging.getLogger(__name__)

CURRENT_KEY = "elwood/outliers/current.json"
UNIT_STATUS_KEY = "elwood/outliers/unit_status.csv"
OVERRIDES_KEY = "elwood/outliers/exclude_from_outliers.csv"

HISTORY_WINDOW_HOURS = 8
WINDOW_HOURS_FOR_PROMOTION = 4
MIN_FLAGGED_HOURS_FOR_4 = 3
MIN_HOURS_FOR_5 = 6
MIN_HOURS_FOR_6 = 24

GOOD_HOURS_TO_CLEAR = {3: 2, 4: 2, 5: 3, 6: 3}


def _s3_get_bytes(s3, bucket: str, key: str) -> bytes:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


@task
def get_current_outliers() -> list[str] | None:
    s3 = init_epa_s3()
    try:
        raw = json.loads(_s3_get_bytes(s3, fasm_layers_bucket(), CURRENT_KEY))
    except ClientError as exc:
        logger.warning(f"current.json not available yet ({exc}); skipping run")
        return None
    outliers = [str(u) for u in raw]
    logger.info(f"LOADED {len(outliers)} current outliers")
    return outliers


@task
def get_unit_status() -> pd.DataFrame:
    s3 = init_epa_s3()
    try:
        data = _s3_get_bytes(s3, fasm_layers_bucket(), UNIT_STATUS_KEY)
        df = pd.read_csv(io.BytesIO(data), parse_dates=["created_at", "last_updated"])
        df["unit_id"] = df["unit_id"].astype(str)
        df["hours_since_addition"] = df["hours_since_addition"].astype(float)
        df["num_good_hours"] = df["num_good_hours"].astype(float)
        df["unit_status"] = df["unit_status"].astype(int)
        df["history"] = df["history"].apply(ast.literal_eval)
        logger.info(f"LOADED existing unit_status.csv ({len(df)} rows)")
        return df
    except ClientError as exc:
        logger.warning(f"No existing unit_status.csv ({exc}); starting fresh")
        return pd.DataFrame(
            columns=[
                "unit_id",
                "hours_since_addition",
                "num_good_hours",
                "unit_status",
                "created_at",
                "last_updated",
                "history",
            ]
        )


@task
def get_overrides() -> set[str]:
    s3 = init_epa_s3()
    try:
        data = _s3_get_bytes(s3, fasm_layers_bucket(), OVERRIDES_KEY)
        df = pd.read_csv(io.BytesIO(data))
        ids = set(df["unit_id"].astype(str).tolist())
    except ClientError as exc:
        logger.warning(f"Could not load overrides ({exc}); assuming none")
        ids = set()
    logger.info(f"{len(ids)} override unit_ids will be excluded from postgres write")
    return ids


def _advance_state(status_df: pd.DataFrame, outlier_ids: list[str]) -> pd.DataFrame:
    now = pd.Timestamp.now(tz="UTC")
    outliers_set = set(outlier_ids)

    # --- Units currently flagged: increment, possibly promote ---
    for unit_id in outlier_ids:
        if unit_id not in status_df["unit_id"].values:
            status_df = pd.concat(
                [
                    status_df,
                    pd.DataFrame(
                        [
                            {
                                "unit_id": unit_id,
                                "hours_since_addition": 0.0,
                                "num_good_hours": 0.0,
                                "unit_status": 3,
                                "created_at": now,
                                "last_updated": now,
                                "history": [0.5],
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )
            continue

        idx = status_df.index[status_df["unit_id"] == unit_id][0]
        elapsed = (now - status_df.at[idx, "last_updated"]).total_seconds() / 3600

        status_df.at[idx, "hours_since_addition"] += elapsed
        status_df.at[idx, "num_good_hours"] = 0
        status_df.at[idx, "last_updated"] = now

        history = status_df.at[idx, "history"]
        history.append(round(elapsed, 2))
        while sum(abs(h) for h in history) > HISTORY_WINDOW_HOURS:
            history.pop(0)
        status_df.at[idx, "history"] = history

        total, flagged = 0.0, 0.0
        for h in reversed(history):
            total += abs(h)
            if h > 0:
                flagged += h
            if total >= WINDOW_HOURS_FOR_PROMOTION:
                break

        current = status_df.at[idx, "unit_status"]
        if current == 3 and flagged >= MIN_FLAGGED_HOURS_FOR_4:
            status_df.at[idx, "unit_status"] = 4
        elif current == 4 and sum(history) >= MIN_HOURS_FOR_5:
            status_df.at[idx, "unit_status"] = 5
        elif (
            current == 5
            and status_df.at[idx, "hours_since_addition"] >= MIN_HOURS_FOR_6
        ):
            status_df.at[idx, "unit_status"] = 6

    # --- Units not currently flagged: accumulate good hours, maybe drop ---
    to_drop = []
    for idx, row in status_df.iterrows():
        if row["unit_id"] in outliers_set:
            continue

        elapsed = (now - row["last_updated"]).total_seconds() / 3600
        status_df.at[idx, "num_good_hours"] += elapsed
        status_df.at[idx, "last_updated"] = now

        history = row["history"]
        history.append(round(-elapsed, 2))
        while sum(abs(h) for h in history) > HISTORY_WINDOW_HOURS:
            history.pop(0)
        status_df.at[idx, "history"] = history

        good = status_df.at[idx, "num_good_hours"]
        if good >= GOOD_HOURS_TO_CLEAR.get(row["unit_status"], 999):
            to_drop.append(idx)

    if to_drop:
        status_df = status_df.drop(to_drop).reset_index(drop=True)

    return status_df


@task
def advance_state(status_df: pd.DataFrame, outliers: list[str]) -> pd.DataFrame:
    updated = _advance_state(status_df, outliers)
    for status, count in updated["unit_status"].value_counts().items():
        logger.info(f"  status {status}: {count} units")
    return updated


@task
def write_unit_status_to_s3(df: pd.DataFrame):
    s3 = init_epa_s3()
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    s3.put_object(
        Bucket=fasm_layers_bucket(),
        Key=UNIT_STATUS_KEY,
        Body=buf.getvalue(),
        ContentType="text/csv",
    )
    logger.info(f"WROTE unit_status.csv ({len(df)} rows)")


@task
def write_elwood_outliers_to_pg(df: pd.DataFrame, overrides: set[str]):
    filtered = df[~df["unit_id"].isin(overrides)]
    records = filtered[
        ["unit_id", "unit_status", "created_at", "last_updated"]
    ].to_dict(orient="records")

    conn = get_ts_db_conn()
    try:
        with conn.cursor() as c:
            c.execute("TRUNCATE pwfsl_map.elwood_outliers;")
            if records:
                execute_values(
                    cur=c,
                    sql="""
                        INSERT INTO pwfsl_map.elwood_outliers
                        (unit_id, unit_status, created_at, last_updated)
                        VALUES %s;
                    """,
                    argslist=records,
                    template=(
                        "(%(unit_id)s, %(unit_status)s, "
                        "%(created_at)s, %(last_updated)s)"
                    ),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info(
        f"LOADED {len(records)} rows to pwfsl_map.elwood_outliers (after overrides)"
    )


@flow
def run():
    outliers = get_current_outliers()
    if outliers is None:
        return "skipped - no current outliers"
    status_in = get_unit_status()
    overrides = get_overrides()
    status_out = advance_state(status_in, outliers)
    write_unit_status_to_s3(status_out)
    write_elwood_outliers_to_pg(status_out, overrides)
    return f"advanced state machine: {len(status_out)} tracked units"
