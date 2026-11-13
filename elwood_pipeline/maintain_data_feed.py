"""
Maintain the hourly AQ data feed that powers elwood outlier detection.

Pulls active AQ units from the tileserver DB, computes a distance-banded
spatial neighbor network, and publishes artifacts to S3 (EPA bucket):

  s3://<epa-bucket>/<prefix>/hourly_aq/<ts>.parquet      (GeoParquet, EPSG:4326)
  s3://<epa-bucket>/<prefix>/hourly_aq/<ts>.geojson      (GeoJSON, EPSG:4326)
  s3://<epa-bucket>/<prefix>/hourly_weights/<ts>.json    (network dict)

<ts> is the current hour floored to the top of the UTC hour. Running this
flow repeatedly within an hour overwrites the same <ts> keys, which is the
intended behavior — downstream flows read the latest value.
"""

import io
import json
import logging
from datetime import UTC, datetime, timedelta

import geopandas as gpd
from elwood_spatial.air_quality import AQI_MODIFIED_BINS
from sqlalchemy import text

from elwood_pipeline.db import get_ts_engine
from elwood_pipeline.network import build_spherical_network
from elwood_pipeline.s3 import (
    elwood_key,
    fasm_layers_bucket,
    init_epa_s3,
    put_s3_object,
)
from elwood_pipeline.sql_util import read_sql

logger = logging.getLogger(__name__)

NETWORK_THRESHOLD_M = 8000
MAX_NEIGHBORS = 15


def _hour_ts() -> str:
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    return str(int(now.timestamp()))


def _assign_cat(aqi):
    idx = AQI_MODIFIED_BINS.bin_index(aqi)
    if idx < 0 or AQI_MODIFIED_BINS.labels is None:
        return None
    return AQI_MODIFIED_BINS.labels[idx]


def pull_aq_data() -> gpd.GeoDataFrame:
    query_sql = read_sql("pull_aq_units.sql")
    engine = get_ts_engine()
    with engine.begin() as conn:
        gdf = gpd.GeoDataFrame.from_postgis(sql=text(query_sql), con=conn)
    gdf.crs = "EPSG:4326"
    gdf["cat"] = gdf["aqi"].apply(_assign_cat)
    logger.info(f"EXTRACTED {len(gdf)} AQ units from tileserver DB")
    return gdf


def upload_aq_feed(gdf: gpd.GeoDataFrame) -> str:
    ts = _hour_ts()
    parquet_key = elwood_key(f"hourly_aq/{ts}.parquet")
    geojson_key = elwood_key(f"hourly_aq/{ts}.geojson")

    buf = io.BytesIO()
    gdf.to_parquet(buf, compression="snappy", index=False)
    geojson_bytes = gdf.to_json().encode("utf-8")

    s3 = init_epa_s3()
    bucket = fasm_layers_bucket()
    put_s3_object(
        s3,
        bucket=bucket,
        key=parquet_key,
        body=buf.getvalue(),
        content_type="application/vnd.apache.parquet",
    )
    put_s3_object(
        s3,
        bucket=bucket,
        key=geojson_key,
        body=geojson_bytes,
        content_type="application/geo+json",
    )
    logger.info(
        f"UPLOADED {parquet_key} ({buf.tell():,} bytes) and "
        f"{geojson_key} ({len(geojson_bytes):,} bytes) for {len(gdf)} features"
    )
    return ts


def upload_weights(gdf: gpd.GeoDataFrame, ts: str) -> dict:
    network = build_spherical_network(
        gdf=gdf,
        threshold_m=NETWORK_THRESHOLD_M,
        id_column="unit_id",
        max_neighbors=MAX_NEIGHBORS,
        min_neighbors=0,
    )

    key = elwood_key(f"hourly_weights/{ts}.json")
    s3 = init_epa_s3()
    put_s3_object(
        s3,
        bucket=fasm_layers_bucket(),
        key=key,
        body=json.dumps(network, allow_nan=False).encode("utf-8"),
        content_type="application/json",
    )
    logger.info(f"UPLOADED {key} ({len(network)} units)")
    return network


def prune_old_hourly_files(
    retention_hours: int = 48, now: datetime | None = None
) -> int:
    current_time = now or datetime.now(UTC)
    cutoff = current_time - timedelta(hours=retention_hours)
    s3 = init_epa_s3()
    bucket = fasm_layers_bucket()
    deleted_total = 0

    for subdir in ("hourly_aq/", "hourly_weights/"):
        prefix = elwood_key(subdir)
        continuation_token = None
        to_delete = []
        while True:
            list_kwargs = {"Bucket": bucket, "Prefix": prefix}
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token
            resp = s3.list_objects_v2(**list_kwargs)
            for obj in resp.get("Contents", []):
                last_modified = obj.get("LastModified")
                if last_modified is not None and last_modified < cutoff:
                    to_delete.append({"Key": obj["Key"]})
            if resp.get("IsTruncated") and resp.get("NextContinuationToken"):
                continuation_token = resp["NextContinuationToken"]
            else:
                break

        for i in range(0, len(to_delete), 1000):
            batch = to_delete[i : i + 1000]
            s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
            deleted_total += len(batch)

    if deleted_total:
        logger.info(
            f"PRUNED {deleted_total} hourly Elwood objects "
            f"older than {retention_hours}h"
        )
    return deleted_total


def elwood_maintain_data_feed():
    gdf = pull_aq_data()
    ts = upload_aq_feed(gdf)
    upload_weights(gdf, ts)
    prune_old_hourly_files()
    logger.info(f"Data feed refreshed for ts={ts}")
    return f"refreshed data feed for ts={ts}"


def run():
    return elwood_maintain_data_feed()
