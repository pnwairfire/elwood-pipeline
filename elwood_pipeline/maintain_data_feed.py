"""
Maintain the hourly AQ data feed that powers elwood outlier detection.

Pulls active AQ units from the tileserver DB, computes a distance-banded
spatial neighbor network, and publishes two artifacts to S3 (EPA bucket):

  s3://<epa-bucket>/elwood/hourly_aq/<ts>.parquet      (GeoParquet, EPSG:4326)
  s3://<epa-bucket>/elwood/hourly_weights/<ts>.json    (network dict)

<ts> is the current hour floored to the top of the UTC hour. Running this
flow repeatedly within an hour overwrites the same <ts> keys, which is the
intended behavior — downstream flows read the latest value.
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
from elwood_spatial.air_quality import AQI_MODIFIED_BINS
from elwood_spatial.network import build_network
from sqlalchemy import text

from elwood_pipeline.db import get_ts_engine
from elwood_pipeline.s3 import fasm_layers_bucket, init_epa_s3
from elwood_pipeline.sql_util import read_sql

logger = logging.getLogger(__name__)

NETWORK_THRESHOLD_M = 8000
MAX_NEIGHBORS = 15
PROJECTED_CRS = 26915


def _hour_ts() -> str:
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    return str(int(now.timestamp()))


def _assign_cat(aqi):
    idx = AQI_MODIFIED_BINS.bin_index(aqi)
    if idx < 0 or AQI_MODIFIED_BINS.labels is None:
        return None
    return AQI_MODIFIED_BINS.labels[idx]


@task
def pull_aq_data() -> gpd.GeoDataFrame:
    query_sql = read_sql("pull_aq_units.sql")
    engine = get_ts_engine()
    with engine.begin() as conn:
        gdf = gpd.GeoDataFrame.from_postgis(sql=text(query_sql), con=conn)
    gdf.crs = "EPSG:4326"
    gdf["cat"] = gdf["aqi"].apply(_assign_cat)
    logger.info(f"EXTRACTED {len(gdf)} AQ units from tileserver DB")
    return gdf


@task
def upload_aq_feed(gdf: gpd.GeoDataFrame) -> str:
    ts = _hour_ts()
    key = f"elwood/hourly_aq/{ts}.parquet"

    buf = io.BytesIO()
    gdf.to_parquet(buf, compression="snappy", index=False)

    s3 = init_epa_s3()
    s3.put_object(
        Bucket=fasm_layers_bucket(),
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/vnd.apache.parquet",
    )
    logger.info(f"UPLOADED {key} ({len(gdf)} features, {buf.tell():,} bytes)")
    return ts


@task
def upload_weights(gdf: gpd.GeoDataFrame, ts: str) -> dict:
    network = build_network(
        gdf=gdf,
        threshold=NETWORK_THRESHOLD_M,
        id_column="unit_id",
        max_neighbors=MAX_NEIGHBORS,
        min_neighbors=0,
        projected_crs=PROJECTED_CRS,
    )

    key = f"elwood/hourly_weights/{ts}.json"
    s3 = init_epa_s3()
    s3.put_object(
        Bucket=fasm_layers_bucket(),
        Key=key,
        Body=json.dumps(network).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"UPLOADED {key} ({len(network)} units)")
    return network


@flow
def run():
    gdf = pull_aq_data()
    ts = upload_aq_feed(gdf)
    upload_weights(gdf, ts)
    logger.info(f"Data feed refreshed for ts={ts}")
    return f"refreshed data feed for ts={ts}"
