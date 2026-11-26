import sys

from delta.tables import DeltaTable

from pyspark.sql import functions as F

from masar.config import SILVER

from masar.spark import get_spark

 

def _version(spark) -> int:

        return DeltaTable.forPath(spark, SILVER).history(1).select("version").first()[0]

 

def upsert_corrections(spark, path: str) -> None:

        src = (spark.read.option("header", True).csv(path)

               .select("trip_id", F.col("fare_sar").cast("double").alias("fare_sar")))

        before = _version(spark)

        (DeltaTable.forPath(spark, SILVER).alias("t")

            .merge(src.alias("s"), "t.trip_id = s.trip_id")                   # the key: one row per trip

            .whenMatchedUpdate(condition="t.fare_sar <> s.fare_sar",          # only real changes

                               set={"fare_sar": "s.fare_sar"})                # the CSV has 2 columns

            .execute())

        after = _version(spark)

        print("no changes: nothing committed" if after == before else f"MERGE committed as v{after}")

 

if __name__ == "__main__":

        spark = get_spark("merge-ops")

        if sys.argv[1] == "corrections":

            upsert_corrections(spark, "data/fixtures/corrections.csv")

 