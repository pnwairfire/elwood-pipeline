"""Externalized configuration for Elwood data targets.
"""
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
