import pandas as pd
import pytest

import pipeline.harvest_machine_run as harvest


@pytest.fixture
def sample_data():
    # Sample data
    data = {
        "time": pd.to_datetime(
            ["2022-01-01 10:00:00", "2022-01-01 10:00:01", "2022-01-01 10:00:02"]
        ),
        "value": [1, 2, 3],
        "field": ["x", "x", "x"],
        "robot_id": [1, 1, 1],
    }
    df = pd.DataFrame(data)
    return df


def test_clean_run(sample_data):
    # Add duplicate data for testing
    sample_data = pd.concat([sample_data, sample_data], ignore_index=True)

    cleaned_df = harvest.clean_run(sample_data)

    assert len(cleaned_df) == 3, "Duplicate rows were not removed correctly"

    # Test that the DataFrame is sorted by time
    assert cleaned_df["time"].is_monotonic_increasing, "DataFrame is not sorted by time"


def test_calculate_total_distances(sample_data):
    # Adding velocity channels for the sample data
    sample_data["v1"] = [0.5, 0.5, 0.5]  # constant velocity
    sample_data["v2"] = [1, 1, 1]  # constant velocity

    total_distances = harvest.calculate_total_distances(sample_data)

    # Check if the total distance is calculated correctly
    assert total_distances[1] == pytest.approx(1.0)
    assert total_distances[2] == pytest.approx(2.0)


def test_add_run_stats(sample_data):
    run_result = {"dataframe": sample_data}
    harvest.add_run_stats(run_result)

    # Check if stats are added correctly
    assert "start_time" in run_result["run_stats"]
    assert "stop_time" in run_result["run_stats"]
    assert "total_distance_mm" in run_result["run_stats"]
    assert "total_runtime_seconds" in run_result["run_stats"]
