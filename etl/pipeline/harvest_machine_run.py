import logging
import traceback
from typing import Optional

import numpy as np
import pandas as pd

import lib.env as env_lib

env_lib.load_env()

logger = logging.getLogger(__name__)
# This takes insanely long but may be useful for sampling a new data set
VALIDATE_DUPLICATES = False


def clean_run(df: pd.DataFrame) -> pd.DataFrame:
    df.sort_values("time", inplace=True)
    if VALIDATE_DUPLICATES:
        for _, group in df.groupby(["time", "field", "robot_id"]):
            if group["value"].nunique() > 1:
                raise ValueError(
                    f"Redundant samples with different values found: {group}"
                )

    # If no error was raised, it means all redundant samples have the same value
    df = df.drop_duplicates(subset=["time", "field", "robot_id"])

    # TODO: Outliers
    return df


def combine_columns(df: pd.DataFrame) -> None:
    df["channel"] = df["field"].astype(str) + "_" + df["robot_id"].astype(str)
    df.drop(columns=["field", "robot_id"], inplace=True)


def append_derivative(
    df: pd.DataFrame, time_delta: pd.DataFrame, new_channel: str, df_list: list
) -> pd.DataFrame:
    df_derivative = df.copy()
    df_derivative["channel"] = new_channel
    df_derivative["value"] = df_derivative["value"].diff() / time_delta
    df_derivative.dropna(subset=["value"], inplace=True)
    df_list.append(df_derivative)
    return df_derivative


def add_delta_and_derivative_for_components(df: pd.DataFrame) -> pd.DataFrame:
    dataframes_to_append = []

    # Iterate over each robot and axis to calculate velocity and acceleration
    for robot in [1, 2]:
        for axis in ["x", "y", "z"]:
            pos_channel = f"{axis}_{robot}"

            # Select rows for the current position channel directly as a slice
            df_pos = df[df["channel"] == pos_channel]
            time_delta = df_pos["time"].diff().dt.total_seconds()  # type: ignore

            df_vel = append_derivative(
                df_pos, time_delta, f"v{pos_channel}", dataframes_to_append  # type: ignore
            )
            append_derivative(
                df_vel, time_delta, f"a{pos_channel}", dataframes_to_append
            )

    # Concatenate original dataframe with the velocity and acceleration dataframes
    df = pd.concat([df, *dataframes_to_append])

    # Sort the dataframe by time
    df.sort_values("time", inplace=True)

    return df


def pivot_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.pivot(index="time", columns="channel", values="value")
    df.reset_index(inplace=True)
    # Here we are doing brute force store-hold.
    # We could resample to a standard rate (the derivatives and integrals are already calculated)
    df.ffill(inplace=True)
    # We most likely do not care about the sampling ramp-up period, but maybe revisit
    df.dropna(inplace=True)
    return df


def add_scalar_for_components(
    run_uuid: str, new_channel: str, needed_channels: list[str], df: pd.DataFrame
) -> None:
    has_channels = [channel for channel in needed_channels if channel in df.columns]
    if len(has_channels) == 3:
        # If all channels exist, calculate the scalar value
        df[new_channel] = np.sqrt(
            df[has_channels[0]] ** 2
            + df[has_channels[1]] ** 2
            + df[has_channels[2]] ** 2
        )
    else:
        logger.warning(
            f"Run {run_uuid} missing channels for {new_channel}: {needed_channels} vs {has_channels}"
        )


def add_scalars(run_uuid: str, df: pd.DataFrame) -> None:
    for robot in [1, 2]:
        for measure in ["v", "a", "f"]:
            needed_channels = [f"{measure}{axis}_{robot}" for axis in ["x", "y", "z"]]
            add_scalar_for_components(
                run_uuid, f"{measure}{robot}", needed_channels, df
            )
        needed_channels = [f"{axis}_{robot}" for axis in ["x", "y", "z"]]
        add_scalar_for_components(run_uuid, f"p_{robot}", needed_channels, df)


def calculate_total_distances(df: pd.DataFrame) -> dict:
    total_distance = {}
    for robot in [1, 2]:
        velocity_channel = f"v{robot}"
        if velocity_channel in df.columns:
            time_deltas = df["time"].diff().dt.total_seconds().fillna(0)
            distance = df[velocity_channel] * time_deltas
            total_distance[robot] = distance.sum()
    return total_distance


def add_run_stats(run_result: dict) -> None:
    df = run_result["dataframe"]
    run_result["run_stats"] = {
        "available_channels": list(df.columns),
        "start_time": df["time"].iloc[0].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "stop_time": df["time"].iloc[-1].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "total_distance_mm": calculate_total_distances(df),
        "total_runtime_seconds": (
            df["time"].iloc[-1] - df["time"].iloc[0]
        ).total_seconds(),
    }


def make_run_result(
    run_uuid: str, df: Optional[pd.DataFrame] = None, error: Optional[str] = None
) -> dict:
    run_result = {
        "dataframe": df,
        "error": error,
        "run_uuid": run_uuid,
    }
    add_run_stats(run_result)
    return run_result


def process_one_run(item: tuple[str, pd.DataFrame]) -> dict:
    """
    Process the dataframe for one run:
      * Convert to wide format
      * Add synthesized channels for velocity and acceleration
      * Add scalars for velocity, acceleration, and force
    :param item: A tuple containing the `run_uuid` and a dataframe containing the data
    :return: A tuple containing the `run_uuid` and dict containing either an `error` or transformed `data`
    """
    run_uuid, df = item
    try:
        logger.info(f"\n\nProcessing run {run_uuid}\n")
        df = clean_run(df)
        combine_columns(df)
        df = add_delta_and_derivative_for_components(df)
        df = pivot_table(df)
        add_scalars(run_uuid, df)
        return make_run_result(run_uuid, df=df)
    except Exception:
        return make_run_result(run_uuid, error=traceback.format_exc())
