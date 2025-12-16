from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession


def get_taxis(spark: SparkSession) -> DataFrame:
    return spark.read.table("samples.nyctaxi.trips")


# Create a new Databricks Connect session using serverless.
def get_spark() -> SparkSession:
    return DatabricksSession.builder.serverless(True).getOrCreate()

def main():
    get_taxis(get_spark()).show(5)


if __name__ == "__main__":
    main()
