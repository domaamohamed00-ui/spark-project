import pytest
from pyspark.sql import SparkSession

from pyspark_job import clean_data


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_valid_records_are_kept(spark):
    df = spark.createDataFrame([("Ali", 100.0), ("Sara", 50.0)], ["name", "amount"])
    result = clean_data(df)
    assert result.count() == 2


def test_amount_less_or_equal_zero_removed(spark):
    df = spark.createDataFrame(
        [("Ali", 100.0), ("Sara", 0.0), ("Omar", -5.0)], ["name", "amount"]
    )
    result = clean_data(df)
    names = [r["name"] for r in result.collect()]
    assert names == ["Ali"]


def test_null_names_removed(spark):
    df = spark.createDataFrame(
        [("Ali", 100.0), (None, 50.0)], "name string, amount double"
    )
    result = clean_data(df)
    assert result.count() == 1
    assert result.collect()[0]["name"] == "Ali"


def test_amount_with_tax_calculated_correctly(spark):
    df = spark.createDataFrame([("Ali", 100.0)], ["name", "amount"])
    result = clean_data(df).collect()[0]
    assert result["amount_with_tax"] == pytest.approx(120.0)