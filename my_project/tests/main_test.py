from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from my_project.main import get_spark, get_taxis


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    return spark


def test_get_taxis_returns_dataframe(spark):
    """Test that get_taxis returns a DataFrame when table exists."""
    # Create mock taxi data using the real spark session
    mock_data = [
        (1, "2023-01-01", "2023-01-01", 1.5, 10.0),
        (2, "2023-01-02", "2023-01-02", 2.0, 15.0),
        (3, "2023-01-03", "2023-01-03", 3.5, 20.0),
        (4, "2023-01-04", "2023-01-04", 1.0, 8.0),
        (5, "2023-01-05", "2023-01-05", 2.5, 12.0),
        (6, "2023-01-06", "2023-01-06", 4.0, 25.0),
    ]
    columns = [
        "trip_id",
        "pickup_datetime",
        "dropoff_datetime",
        "trip_distance",
        "fare_amount",
    ]
    expected_df = spark.createDataFrame(mock_data, columns)

    # Create a mock SparkSession with mocked read.table()
    mock_spark = MagicMock(spec=SparkSession)
    mock_spark.read.table.return_value = expected_df

    # Call the function with the mock
    result = get_taxis(mock_spark)

    assert result.count() > 5
    mock_spark.read.table.assert_called_once_with("samples.nyctaxi.trips")


def test_get_spark_returns_session():
    """Test that get_spark returns a SparkSession."""
    session = get_spark()
    assert session is not None
    assert isinstance(session, SparkSession)
