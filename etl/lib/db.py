"""
A toy DB interface
"""
import os
import textwrap
from typing import Dict, Optional

import sqlalchemy as sa

import lib.env as env_lib

_db_engines: Dict[str, sa.engine.Engine] = {}


def execute(
    env: env_lib.EnvType, sql: str, sql_params: Optional[Dict] = None
) -> sa.engine.CursorResult:
    """Execute a single SQL statement against the target environment using
    connection.execute(text()).

    For parameter usage see: https://docs.sqlalchemy.org/en/14/core/tutorial.html#using-textual-sql

    Manages the connection for you.

    WARNING: Not suitable for fetching really large amounts of data as it loads the
    entire results into memory
    """
    try:
        with get_engine(env).connect() as connection:
            # Start a new transaction explicitly
            with connection.begin():
                result = connection.execute(sa.text(textwrap.dedent(sql)), sql_params)  # type: ignore
                # Commit the transaction explicitly
                connection.commit()
                return result
    except Exception as e:
        print(f"An error occurred: {e}")
        raise


def get_engine(env: env_lib.EnvType) -> sa.engine.Engine:
    """
    WARNING: Lower-level functionality. You are responsible for managing the DB
    connections responsibly yourself. Unless you know you need this, use `execute`
    instead!

    Get a SQL engine (see https://docs.sqlalchemy.org/en/14/core/engines.html) for
    target environment. Useful for multi-query transactions on a single connection.
    """
    if env.value not in _db_engines:
        _db_engines[env.value] = sa.engine.create_engine(
            "postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db_name}".format(
                user=os.environ["POSTGRES_CACHE_USER"],
                pwd=os.environ["POSTGRES_CACHE_PASSWORD"],
                host=os.environ["POSTGRES_CACHE_HOST"],
                port=os.environ["POSTGRES_CACHE_PORT"],
                db_name=os.environ["POSTGRES_CACHE_DATABASE"],
            ),
            pool_pre_ping=True,
            echo=True,  # Add this line to enable logging
        )

    return _db_engines[env.value]
