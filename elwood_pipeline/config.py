"""Externalized configuration for Elwood data targets."""

import os
import re

from dotenv import load_dotenv

load_dotenv()

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _env(key, default):
    return os.getenv(key, default)


def _ident(key, default):
    val = os.getenv(key, default)
    if not _IDENT.match(val):
        raise ValueError(
            f"Config {key}={val!r} is not a valid SQL identifier "
            f"(expected letters, digits, underscore; no dots or spaces)."
        )
    return val


def qualified(table: str) -> str:
    return f"{DEST_SCHEMA}.{table}"


# --- Schemas ---
DEST_SCHEMA = _ident("DEST_SCHEMA", "pwfsl_map")

# --- Destination tables ---
ELWOOD_OUTLIERS_TABLE = _ident("ELWOOD_OUTLIERS_TABLE", "elwood_outliers")

# --- S3 routing & ACL ---
ELWOOD_S3_PREFIX = _env("ELWOOD_S3_PREFIX", "elwood").strip("/")
DEFAULT_PURPLEAIR_WEEKLY_S3_PREFIX = "maps/purple_air/v4/timeseries/weekly"
PURPLEAIR_WEEKLY_S3_PREFIX = _env(
    "PURPLEAIR_WEEKLY_S3_PREFIX", DEFAULT_PURPLEAIR_WEEKLY_S3_PREFIX
).strip("/")
DISABLE_S3_ACL = _env("DISABLE_S3_ACL", "false").lower() in ("1", "true", "yes")
