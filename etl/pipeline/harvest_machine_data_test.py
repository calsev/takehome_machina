import uuid

import pandas as pd
import pytest

import lib.data as dat_lib
import pipeline.harvest_machine_data as dat


@pytest.fixture
def sample_file_data():
    valid_data = {
        "time": "2022-11-23T20:40:00.005Z",
        "value": 123.456,
        "run_uuid": 1234567890123456789,
        "field": "x",
        "robot_id": 1,
        "sensor_type": "encoder",
    }
    data_list = [
        valid_data,
        {**valid_data, **{"time": "invalid_time"}},
        {**valid_data, **{"value": None}},
        {**valid_data, **{"run_uuid": None}},
        {**valid_data, **{"field": "invalid_field"}},
        {**valid_data, **{"robot_id": 3}},
        {**valid_data, **{"sensor_type": "invalid_sensor"}},
    ]
    return pd.DataFrame(dat_lib.array_of_struct_to_struct_of_array(data_list))


def test_clean_dataframe(sample_file_data, mocker):
    mocker.patch("pipeline.harvest_machine_data.logger")

    result_df = dat.clean_dataframe(sample_file_data)

    assert len(result_df) == 1, "Incorrect number of rows after cleaning"
    assert result_df["time"].dtype == "<M8[ns]", "Time column not in datetime format"
    assert (
        "invalid_field" not in result_df["field"].values
    ), "Invalid field value present after cleaning"
    assert (
        3 not in result_df["robot_id"].values
    ), "Invalid robot_id present after cleaning"
    assert (
        result_df["field"].dtype.name == "category"
    ), "Field column not converted to category"
    assert (
        result_df["robot_id"].dtype.name == "category"
    ), "Robot_id column not converted to category"
    assert "sensor_type" not in result_df.columns, "Sensor_type column not dropped"


@pytest.fixture
def sample_run_data():
    # Sample data simulating two different runs
    data = {
        "time": [
            "2022-11-23T20:40:00.005Z",
            "2022-11-23T20:40:10.005Z",
            "2022-11-23T20:50:00.005Z",
        ],
        "value": [123.456, 789.012, 456.789],
        "run_uuid": [
            1234567890123456789,
            1234567890123456789,
            9876543210987654321,
        ],  # Two different run_uuids
        "field": ["x", "y", "z"],
        "robot_id": [1, 1, 2],
        "sensor_type": ["encoder", "encoder", "load_cell"],
    }
    return pd.DataFrame(data)


def test_separate_runs(sample_run_data, mocker):
    mocker.patch("pipeline.harvest_machine_data.logger")

    run_to_df = dat.separate_runs(sample_run_data)

    assert len(run_to_df) == 2, "Incorrect number of separate runs"
    for run_uuid, df in run_to_df.items():
        assert (
            isinstance(run_uuid, str) and len(run_uuid) == 36
        ), "Run UUID is not in the correct format"
        assert "run_uuid" not in df.columns, "run_uuid column not dropped"
        expected_uuid = uuid.UUID(run_uuid)
        expected_run_data = sample_run_data[
            sample_run_data["run_uuid"] == int(expected_uuid)
        ]
        pd.testing.assert_frame_equal(
            df.reset_index(drop=True),
            expected_run_data.drop(columns=["run_uuid"]).reset_index(drop=True),
            check_dtype=False,
        )
