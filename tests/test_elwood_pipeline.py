# ruff: noqa: S101
# ruff: noqa: S101
import io
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from elwood_pipeline import (
    config,
    elwood_outliers,
    maintain_data_feed,
    s3,
    state_management,
)


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
