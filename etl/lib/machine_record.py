import json
import os

import pandas as pd

import lib.db as db
import lib.env as env_lib


def move_file_record_to_storage(cache_dir: str, file_record: dict) -> None:
    """
    Move the data frames for one file to disk.
    For now this is local disk storage, but this layer could also abstract object storage
    :param cache_dir: The director to which to write the record
    :param file_record: The data from harvesting the file
    """
    file_dir = os.path.join(cache_dir, file_record["source_file"])
    os.makedirs(file_dir, exist_ok=True)
    for run_uuid, run_data in file_record["run_to_result"].items():
        run_file_name = f"run_{run_uuid}.parquet"
        run_file_path = os.path.join(file_dir, run_file_name)
        run_data["file_name"] = run_file_name
        df = run_data.pop("dataframe")
        if df is not None:
            df.to_parquet(run_file_path, engine="fastparquet")
    file_record_path = os.path.join(file_dir, "record.json")
    with open(file_record_path, "w") as f:
        json.dump(file_record, f, indent="  ")


def read_file_record_from_storage(cache_dir: str, source_file: str) -> dict:
    """
    Load the harvest record for one file from disk
    :param cache_dir: The directory where the file records are located
    :param source_file: The name of the parquet file for which to read the record
    :return:
    """
    # We are agnostic about including the path or extension
    source_file = os.path.splitext(os.path.basename(source_file))[0]
    file_dir = os.path.join(cache_dir, source_file)
    file_record_path = os.path.join(file_dir, "record.json")
    with open(file_record_path) as f:
        file_record = json.load(f)
    for run_data in file_record["run_to_result"].values():
        run_file_path = os.path.join(file_dir, run_data.pop("file_name"))
        run_data["dataframe"] = pd.read_parquet(run_file_path)
    return file_record


def save_file_record_to_db(file_record: dict) -> None:
    """
    Insert a new record into cache.machine_data_file table.
    """
    query = """
        insert into cache.machine_data_file (has_error, run_error_count, run_success_count, source)
        values (:has_error, :run_error_count, :run_success_count, :source)
    """
    db.execute(
        env_lib.EnvType.local,
        query,
        sql_params={
            "has_error": file_record["error"] is not None,
            "run_error_count": file_record["file_stats"]["run_error_count"],
            "run_success_count": file_record["file_stats"]["run_success_count"],
            "source": file_record["source_file"],  # TODO: Something better
        },
    )


def file_record_exists_in_db(file_record: dict) -> bool:
    """
    Insert a new record into cache.machine_data_file table.
    """
    query = """
        select created_at
        from
            cache.machine_data_file as mdf
        where
            mdf.source = :source
    """
    results = (
        db.execute(
            env_lib.EnvType.local,
            query,
            sql_params={
                "source": file_record["source_file"],
            },
        )
        .scalars()
        .all()
    )
    return len(results) > 0
