"""PostgreSQL connection/engine helpers for the Elwood pipeline.
"""
import os
from urllib.parse import quote_plus

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine

from elwood_pipeline import config

load_dotenv()


REQUIRED_KEYS = {"host", "user", "password", "database"}


def _get_config(env_vars):
    if not isinstance(env_vars, dict):
        raise ValueError(
            f"env_vars must be a dict mapping keys to env var names. "
            f"Required keys: {REQUIRED_KEYS}. Optional: port"
        )

    missing_keys = REQUIRED_KEYS - env_vars.keys()
    if missing_keys:
        raise ValueError(
            f"env_vars missing required keys: {missing_keys}."
        )

    missing_env = [
        v for k, v in env_vars.items()
        if k in REQUIRED_KEYS and not os.getenv(v)
    ]
    if missing_env:
        raise ValueError(f"Missing env var(s): {', '.join(missing_env)}")

    return {
        "host": os.getenv(env_vars["host"]),
        "port": os.getenv(env_vars.get("port"), "5432"),
        "user": os.getenv(env_vars["user"]),
        "password": os.getenv(env_vars["password"]),
        "database": os.getenv(env_vars["database"]),
    }


def get_uri(env_vars, sslmode="require"):
    cfg = _get_config(env_vars)
    pw = quote_plus(cfg["password"])
    uri = f"postgresql://{cfg['user']}:{pw}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return f"{uri}?sslmode={sslmode}" if sslmode else uri


def get_conn(env_vars, options=None, connect_timeout=20):
    cfg = _get_config(env_vars)
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        connect_timeout=connect_timeout,
        options=options,
    )


def get_engine(env_vars, options=None):
    uri = get_uri(env_vars, sslmode=None)
    connect_args = {"options": options} if options else {}
    return create_engine(uri, connect_args=connect_args)


TS_DB = {
    "host": "TS_DB_HOST",
    "port": "TS_DB_PORT",
    "user": "TS_DB_USER",
    "password": "TS_DB_PW",
    "database": "TS_DB_DATABASE",
}

_TS_SEARCH_PATH = f"-c search_path={config.DEST_SCHEMA},public"


def get_ts_db_conn():
    return get_conn(TS_DB, options=_TS_SEARCH_PATH)


def get_ts_engine():
    return get_engine(TS_DB, options=_TS_SEARCH_PATH)
