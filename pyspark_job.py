# spark data cleaning job
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_data(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("amount") > 0)          # شيل الصفوف اللي amount <= 0
          .filter(F.col("name").isNotNull())    # شيل الصفوف اللي name فيها NULL
          .withColumn("amount_with_tax", F.col("amount") * 1.20)
    )