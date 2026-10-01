"""BRONZE: land raw CSVs as Delta, untouched, plus lineage metadata.

- every column kept as STRING (no schema loss, no failed loads)
- adds _ingest_ts, _source_file, _batch_date
- idempotent: re-running a date replaces only that date's partition
"""
import argparse

from pyspark.sql import functions as F

from common import ENTITIES, get_spark, lake_path


def ingest(spark, entity: str, ds: str) -> None:
    src = f"{lake_path('raw', entity)}/ds={ds}/*.csv"
    df = (
        spark.read.option("header", True).option("mode", "PERMISSIVE").csv(src)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_date", F.lit(ds))
    )
    n = df.count()
    (
        df.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"_batch_date = '{ds}'")
        .partitionBy("_batch_date")
        .save(lake_path("bronze", entity))
    )
    print(f"[bronze] {entity}: {n} rows for {ds}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--entity", choices=ENTITIES + ["all"], default="all")
    p.add_argument("--ds", required=True)
    a = p.parse_args()
    spark = get_spark(f"bronze_{a.entity}")
    for e in (ENTITIES if a.entity == "all" else [a.entity]):
        ingest(spark, e, a.ds)
    spark.stop()
