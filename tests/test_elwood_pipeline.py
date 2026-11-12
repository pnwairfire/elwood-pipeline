# ruff: noqa: S101
import io
import json
import math
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from elwood_pipeline import (
    config,
    elwood_outliers,
    maintain_data_feed,
    s3,
    state_management,
)
from elwood_pipeline.network import build_spherical_network


def test_elwood_key_respects_elwood_s3_prefix(monkeypatch):
    monkeypatch.setattr(config, "ELWOOD_S3_PREFIX", "shadow-dev/elwood")
    assert (
        s3.elwood_key("outliers/current.json")
        == "shadow-dev/elwood/outliers/current.json"
    )

    monkeypatch.setattr(config, "ELWOOD_S3_PREFIX", "elwood")
    assert s3.elwood_key("outliers/current.json") == "elwood/outliers/current.json"


def test_put_s3_object_omits_acl_when_disabled(monkeypatch):
    mock_s3 = MagicMock()
    monkeypatch.setattr(config, "DISABLE_S3_ACL", True)
    s3.put_s3_object(
        mock_s3,
        "airfire-data-exports",
        "shadow-dev/elwood/outliers/current.json",
        b"[]",
        "application/json",
    )
    mock_s3.put_object.assert_called_once_with(
        Bucket="airfire-data-exports",
        Key="shadow-dev/elwood/outliers/current.json",
        Body=b"[]",
        ContentType="application/json",
    )


def test_upload_aq_feed_dual_writes_parquet_and_geojson_with_public_acl(monkeypatch):
    mock_s3 = MagicMock()
    monkeypatch.setattr(maintain_data_feed, "init_epa_s3", lambda: mock_s3)
    monkeypatch.setattr(
        maintain_data_feed, "fasm_layers_bucket", lambda: "airfire-data-exports"
    )
    monkeypatch.setattr(config, "ELWOOD_S3_PREFIX", "elwood")
    monkeypatch.setattr(config, "DISABLE_S3_ACL", False)
    monkeypatch.setattr(maintain_data_feed, "_hour_ts", lambda: "1762452000")

    gdf = gpd.GeoDataFrame(
        {
            "unit_id": ["158009"],
            "aqi": [42],
            "unit_type": ["PurpleAir"],
            "cat": ["Good"],
        },
        geometry=[Point(-122.33, 47.61)],
        crs="EPSG:4326",
    )
    ts = maintain_data_feed.upload_aq_feed(gdf)
    assert ts == "1762452000"
    assert mock_s3.put_object.call_count == 2

    keys_written = {
        c.kwargs["Key"]: c.kwargs for c in mock_s3.put_object.call_args_list
    }
    assert "elwood/hourly_aq/1762452000.parquet" in keys_written
    assert "elwood/hourly_aq/1762452000.geojson" in keys_written
    assert keys_written["elwood/hourly_aq/1762452000.parquet"]["ACL"] == "public-read"
    assert keys_written["elwood/hourly_aq/1762452000.geojson"]["ACL"] == "public-read"
    assert (
        keys_written["elwood/hourly_aq/1762452000.geojson"]["ContentType"]
        == "application/geo+json"
    )


def test_prune_old_hourly_files_deletes_only_keys_older_than_cutoff(monkeypatch):
    now = datetime(2026, 11, 6, 20, 0, tzinfo=UTC)
    old_ts = now - timedelta(hours=50)
    recent_ts = now - timedelta(hours=2)

    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "elwood/hourly_aq/old.geojson", "LastModified": old_ts},
            {"Key": "elwood/hourly_aq/recent.geojson", "LastModified": recent_ts},
        ]
    }
    monkeypatch.setattr(maintain_data_feed, "init_epa_s3", lambda: mock_s3)
    monkeypatch.setattr(
        maintain_data_feed, "fasm_layers_bucket", lambda: "airfire-data-exports"
    )
    monkeypatch.setattr(config, "ELWOOD_S3_PREFIX", "elwood")

    deleted = maintain_data_feed.prune_old_hourly_files(retention_hours=48, now=now)
    assert deleted == 2  # 1 from hourly_aq, 1 from hourly_weights
    assert mock_s3.delete_objects.call_count == 2


def test_outliers_and_state_management_use_elwood_key_and_public_acl(monkeypatch):
    mock_s3 = MagicMock()
    monkeypatch.setattr(elwood_outliers, "init_epa_s3", lambda: mock_s3)
    monkeypatch.setattr(
        elwood_outliers, "fasm_layers_bucket", lambda: "airfire-data-exports"
    )
    monkeypatch.setattr(state_management, "init_epa_s3", lambda: mock_s3)
    monkeypatch.setattr(
        state_management, "fasm_layers_bucket", lambda: "airfire-data-exports"
    )
    monkeypatch.setattr(config, "ELWOOD_S3_PREFIX", "shadow-dev/elwood")
    monkeypatch.setattr(config, "DISABLE_S3_ACL", False)

    elwood_outliers.write_current_to_s3(["u1", "u2"])
    mock_s3.put_object.assert_called_with(
        Bucket="airfire-data-exports",
        Key="shadow-dev/elwood/outliers/current.json",
        Body=b'["u1", "u2"]',
        ContentType="application/json",
        ACL="public-read",
    )

    df = pd.DataFrame(
        [
            {
                "unit_id": "u1",
                "hours_since_addition": 0.0,
                "num_good_hours": 0.0,
                "unit_status": 3,
                "created_at": "2026-11-06T20:00:00Z",
                "last_updated": "2026-11-06T20:00:00Z",
                "history": "[0.5]",
            }
        ]
    )
    state_management.write_unit_status_to_s3(df)
    last_call = mock_s3.put_object.call_args.kwargs
    assert last_call["Key"] == "shadow-dev/elwood/outliers/unit_status.csv"
    assert last_call["ACL"] == "public-read"
    assert last_call["ContentType"] == "text/csv"

    # Overrides always read canonical elwood/outliers/exclude_from_outliers.csv
    mock_s3.get_object.return_value = {"Body": io.BytesIO(b"unit_id\n158009\n")}
    overrides = state_management.get_overrides()
    assert overrides == {"158009"}
    mock_s3.get_object.assert_called_with(
        Bucket="airfire-data-exports",
        Key="elwood/outliers/exclude_from_outliers.csv",
    )


@pytest.mark.parametrize(
    ("region", "lon", "lat"),
    [
        ("Honolulu HI", -157.858, 21.307),
        ("San Juan PR", -66.105, 18.465),
        ("St Johns NL", -52.713, 47.561),
        ("Mexico City MX", -99.133, 19.432),
        ("Fairbanks AK", -147.716, 64.837),
        ("Minneapolis MN", -93.265, 44.978),
    ],
)
def test_build_spherical_network_enforces_true_8km_globally(region, lon, lat):
    dlat_5km = math.degrees(5000.0 / 6_371_000.0)
    dlat_9km = math.degrees(9000.0 / 6_371_000.0)
    dlon_6km = math.degrees(6000.0 / (6_371_000.0 * math.cos(math.radians(lat))))

    gdf = gpd.GeoDataFrame(
        {"unit_id": ["center", "north_5km", "east_6km", "north_9km"]},
        geometry=[
            Point(lon, lat),
            Point(lon, lat + dlat_5km),
            Point(lon + dlon_6km, lat),
            Point(lon, lat + dlat_9km),
        ],
        crs="EPSG:4326",
    )
    net = build_spherical_network(
        gdf, threshold_m=8000.0, id_column="unit_id", max_neighbors=15, min_neighbors=0
    )
    assert net["center"]["neighbors"] == ["north_5km", "east_6km"], (
        f"Failed in {region}"
    )
    assert net["north_9km"]["neighbors"] == ["north_5km"]


def test_build_spherical_network_handles_empty_and_isolated_units():
    empty = gpd.GeoDataFrame({"unit_id": []}, geometry=[], crs="EPSG:4326")
    assert build_spherical_network(empty, threshold_m=8000.0, id_column="unit_id") == {}

    single = gpd.GeoDataFrame(
        {"unit_id": ["only"]},
        geometry=[Point(-155.084, 19.724)],
        crs="EPSG:4326",
    )
    assert build_spherical_network(
        single, threshold_m=8000.0, id_column="unit_id", min_neighbors=0
    ) == {"only": {"neighbors": [], "weights": []}}
    assert (
        build_spherical_network(
            single, threshold_m=8000.0, id_column="unit_id", min_neighbors=1
        )
        == {}
    )

    isolated = gpd.GeoDataFrame(
        {"unit_id": ["hilo", "san_juan"]},
        geometry=[Point(-155.084, 19.724), Point(-66.105, 18.465)],
        crs="EPSG:4326",
    )
    net = build_spherical_network(
        isolated, threshold_m=8000.0, id_column="unit_id", min_neighbors=0
    )
    assert net == {
        "hilo": {"neighbors": [], "weights": []},
        "san_juan": {"neighbors": [], "weights": []},
    }


def test_build_spherical_network_colocated_and_upload_weights_produce_valid_json(
    monkeypatch,
):
    mock_s3 = MagicMock()
    monkeypatch.setattr(maintain_data_feed, "init_epa_s3", lambda: mock_s3)
    monkeypatch.setattr(
        maintain_data_feed, "fasm_layers_bucket", lambda: "airfire-data-exports"
    )
    monkeypatch.setattr(config, "ELWOOD_S3_PREFIX", "elwood")
    monkeypatch.setattr(config, "DISABLE_S3_ACL", False)

    colocated = gpd.GeoDataFrame(
        {"unit_id": ["a", "b", "c"]},
        geometry=[
            Point(-157.858, 21.307),
            Point(-157.858, 21.307),
            Point(-157.858, 21.325),  # ~2 km north in Honolulu
        ],
        crs="EPSG:4326",
    )
    net = maintain_data_feed.upload_weights(colocated, "1762452000")
    assert net["a"]["neighbors"] == ["b", "c"]
    assert net["a"]["weights"][0] > net["a"]["weights"][1] > 0.0
    assert all(math.isfinite(w) for w in net["a"]["weights"])

    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Key"] == "elwood/hourly_weights/1762452000.json"
    parsed = json.loads(call_kwargs["Body"].decode("utf-8"))
    assert parsed == net
