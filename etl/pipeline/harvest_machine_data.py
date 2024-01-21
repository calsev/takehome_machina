import argparse
import logging
import multiprocessing
import os
import traceback
import uuid
from typing import List, Optional

import pandas as pd

import lib.file as file_lib
import lib.machine_record as rec
import pipeline.harvest_machine_run as run

DATA_PATH_DEFAULT = "../data"
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO")
NUM_RUN_WORKERS_DEFAULT = int(
    os.environ.get("NUM_RUN_WORKERS", multiprocessing.cpu_count())
)
CACHE_PATH_DEFAULT = "../cache"
run_record = dict[str, pd.DataFrame]


logging.basicConfig(level=logging.getLevelName(LOGGING_LEVEL))
logger = logging.getLogger(__name__)


def parse_args(args_in: Optional[List[str]] = None) -> argparse.Namespace:
    args = argparse.ArgumentParser()
    args.add_argument(
        "--data-dir",
        type=str,
        default=DATA_PATH_DEFAULT,
    )
    args.add_argument(
        "--cache-dir",
        type=str,
        default=CACHE_PATH_DEFAULT,
    )
    args.add_argument(
        "--profile",
        dest="profile_mode",
        action="store_true",
        default=False,
    )

    parsed_args = args.parse_args(args_in)
    return parsed_args


def convert_and_drop_time_and_value(df: pd.DataFrame) -> None:
    # rstrip + tz-ignorant to_datetime 3x faster - to_datetime was half the runtime
    df["time"] = pd.to_datetime(
        df["time"].str.rstrip("Z"), errors="coerce", format="%Y-%m-%dT%H:%M:%S.%f"
    )
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df.dropna(subset=["time", "value"], inplace=True)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    len_df = len(df)
    logger.info(f"Before cleaning, the dataframe has {len_df} rows")

    convert_and_drop_time_and_value(df)

    # Precompute the 'startswith' condition because the string operation is expensive
    starts_with_f = df["field"].str.startswith("f")
    df = df[
        pd.to_numeric(df["run_uuid"], errors="coerce").notnull()  # type: ignore
        & df["field"].isin({"x", "y", "z", "fx", "fy", "fz"})  # type: ignore
        & df["robot_id"].isin([1, 2])
        & (
            starts_with_f & (df["sensor_type"] == "load_cell")
            | ~starts_with_f & (df["sensor_type"] == "encoder")
        )
    ]
    df["field"] = df["field"].astype("category")
    df["robot_id"] = df["robot_id"].astype("category")
    df.drop(["sensor_type"], axis=1, inplace=True)  # This is now redundant
    logger.info(f"Cleaned {len_df - len(df)} rows")
    return df


def separate_runs(df: pd.DataFrame) -> run_record:
    unique_run_uuids = df["run_uuid"].unique()
    logger.info(f"\n{len(unique_run_uuids)} runs")
    run_to_df = {}
    for run_uuid in unique_run_uuids:
        # Create a copy of the filtered DataFrame before modifying
        filtered_df = df[df["run_uuid"] == run_uuid].copy()
        filtered_df.drop(columns=["run_uuid"], inplace=True)
        # These are actually GIDs, but leave the door open
        run_to_df[str(uuid.UUID(int=int(run_uuid)))] = filtered_df
    return run_to_df


def print_runs(run_to_df: run_record) -> None:
    for df in run_to_df.values():
        logger.info(df)


def harvest_one_file_dataframe(df: pd.DataFrame, profile_mode: bool) -> dict:
    # We may want to cache the cleaned dataframes eventually
    df = clean_dataframe(df)
    run_to_df = separate_runs(df)
    if profile_mode:
        run_results = list(map(run.process_one_run, list(run_to_df.items())))
    else:
        with multiprocessing.Pool(
            processes=(min(NUM_RUN_WORKERS_DEFAULT, len(run_to_df)))
        ) as run_pool:
            run_results = run_pool.map(run.process_one_run, list(run_to_df.items()))
    return {run_data["run_uuid"]: run_data for run_data in run_results}


def get_file_stats_and_print_errors(id_to_result: dict, tag: str) -> dict:
    run_error_count = 0
    run_success_count = 0
    for _id, result in id_to_result.items():
        if result.get("error"):
            run_error_count += 1
            logger.error(f"Failed to process {tag} {_id}:\n{result.get('error')}")
        else:
            run_success_count += 1
    return {
        "run_error_count": run_error_count,
        "run_success_count": run_success_count,
        "run_total_count": run_error_count + run_success_count,
    }


def make_file_result(
    file_path: str,
    run_to_result: Optional[dict] = None,
    error: Optional[str] = None,
    file_stats: Optional[dict] = None,
) -> dict:
    return {
        "error": error,
        "file_stats": file_stats,
        "run_to_result": run_to_result,
        "source_file": os.path.splitext(os.path.basename(file_path))[0],
    }


def harvest_one_sensor_file(file_path: str, cache_dir: str, profile_mode: bool) -> dict:
    try:
        logger.info(f"\n\nProcessing data file {file_path}\n")
        df = pd.read_parquet(file_path)
        run_to_result = harvest_one_file_dataframe(df, profile_mode)
        file_stats = get_file_stats_and_print_errors(run_to_result, "run")
        file_result = make_file_result(
            file_path, run_to_result=run_to_result, file_stats=file_stats
        )
        if rec.file_record_exists_in_db(file_result):
            raise ValueError(f"File {file_path} already exists in the database")
        rec.move_file_record_to_storage(cache_dir, file_result)
        rec.save_file_record_to_db(file_result)
        return file_result
    except Exception:
        return make_file_result(file_path, error=traceback.format_exc())


def harvest_all_sensor_files_in_directory(
    data_dir: str, cache_dir: str, profile_mode: bool = False
) -> None:
    """
    Harvest all files in data_dir, creating a data log in the out_dir
    :param data_dir: The path to the directory containing only the parquet files
    :param cache_dir: The path to the directory in which to create the run record
    :param profile_mode: If true, run serially in main thread
    """
    file_results = list(
        map(
            lambda ddir: harvest_one_sensor_file(ddir, cache_dir, profile_mode),
            file_lib.dir_iter(data_dir),
        )
    )
    file_results = {result["source_file"]: result for result in file_results}
    get_file_stats_and_print_errors(file_results, "file")


def main(args_in: Optional[List[str]] = None) -> None:
    args = parse_args(args_in)
    harvest_all_sensor_files_in_directory(**vars(args))


if __name__ == "__main__":
    main()
