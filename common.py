"""Shared helpers for the Spark jobs (Bronze + Silver)."""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

LAKE_ROOT = os.getenv("LAKE_ROOT", "/opt/lakehouse")
ENTITIES = ["customers", "products", "orders", "order_items"]


def get_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def lake_path(layer: str, name: str) -> str:
    return f"{LAKE_ROOT}/{layer}/{name}"
