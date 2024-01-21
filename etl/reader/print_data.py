"""
This is more-or-less a sample script to demonstrate usage of the run_record module
"""
import argparse
from typing import List, Optional

import lib.machine_record as rec

CACHE_PATH_DEFAULT = "../cache"


def parse_args(args_in: Optional[List[str]] = None) -> argparse.Namespace:
    args = argparse.ArgumentParser()
    args.add_argument(
        "--cache-dir",
        type=str,
        default=CACHE_PATH_DEFAULT,
    )
    args.add_argument(
        "--file",
        dest="source_file",
        type=str,
    )

    parsed_args = args.parse_args(args_in)
    return parsed_args


def print_run_record(cache_dir: str, source_file: str) -> None:
    """
    View statistics for harvested machine data
    :param cache_dir: The path to the directory in which to create the run record
    :param source_file: The name of the source parquet file
    """
    file_result = rec.read_file_record_from_storage(cache_dir, source_file)
    for stat, val in file_result["file_stats"].items():
        print(f"{stat} = {val}")
    for run_result in file_result["run_to_result"].values():
        print(f"\nstats for run {run_result['run_uuid']}:")
        for stat, val in run_result["run_stats"].items():
            print(f"{stat} = {val}")
        if run_result["error"]:
            print(f"Run {run_result['run_uuid']} had error:\n{run_result['error']}")
        else:
            print(run_result["dataframe"])


def main(args_in: Optional[List[str]] = None) -> None:
    args = parse_args(args_in)
    print_run_record(**vars(args))


if __name__ == "__main__":
    main()
