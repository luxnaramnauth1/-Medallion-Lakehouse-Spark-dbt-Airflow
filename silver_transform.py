"""SILVER: cleaned, typed, de-duplicated, validated Delta tables.

- casts to proper types, trims/normalises strings
- de-duplicates on the business key (latest record wins)
- rows failing data-quality rules go to  silver/_quarantine/<entity>  with reasons
- MERGE (upsert) into the Silver table -> idempotent and safe to re-run
"""
import argparse

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common import ENTITIES, get_spark, lake_path

META = ["_ingest_ts", "_batch_date", "_source_file"]


def clean_str(c: str):
    return F.when(F.trim(F.col(c)) != "", F.trim(F.col(c)))


def dedupe(df: DataFrame, key: list, order_col: str) -> DataFrame:
    w = Window.partitionBy(*key).orderBy(F.col(order_col).desc_nulls_last(), F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def split_valid(df: DataFrame, rules: list):
    """rules = [(name, boolean Column)]. Returns (valid_df, quarantined_df)."""
    errors = F.array_compact(
        F.array(*[F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules])
    )
    df = df.withColumn("_dq_errors", errors)
    valid = df.filter(F.size("_dq_errors") == 0).drop("_dq_errors")
    bad = df.filter(F.size("_dq_errors") > 0).withColumn("_dq_errors", F.concat_ws(",", "_dq_errors"))
    return valid, bad


def upsert(spark, df: DataFrame, target: str, key: list, order_col: str) -> None:
    if DeltaTable.isDeltaTable(spark, target):
        cond = " AND ".join(f"t.{k} = s.{k}" for k in key)
        (
            DeltaTable.forPath(spark, target).alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll(condition=f"s.{order_col} >= t.{order_col}")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").save(target)


# ---- entity definitions -----------------------------------------------------
def customers(b: DataFrame):
    df = b.select(
        clean_str("customer_id").alias("customer_id"),
        F.initcap(clean_str("first_name")).alias("first_name"),
        clean_str("last_name").alias("last_name"),
        F.lower(clean_str("email")).alias("email_raw"),
        F.upper(clean_str("country")).alias("country"),
        F.to_date("signup_date").alias("signup_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
        *META,
    )
    ok = F.col("email_raw").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    df = (df.withColumn("is_email_valid", F.coalesce(ok, F.lit(False)))
            .withColumn("email", F.when(F.col("is_email_valid"), F.col("email_raw")))
            .drop("email_raw"))
    rules = [("customer_id_missing", F.col("customer_id").isNotNull()),
             ("updated_at_invalid", F.col("updated_at").isNotNull())]
    return df, rules, ["customer_id"], "updated_at"


def products(b: DataFrame):
    df = b.select(
        clean_str("product_id").alias("product_id"),
        clean_str("name").alias("name"),
        F.initcap(clean_str("category")).alias("category"),
        F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        *META,
    )
    rules = [("product_id_missing", F.col("product_id").isNotNull()),
             ("price_not_positive", F.col("unit_price") > 0)]
    return df, rules, ["product_id"], "_ingest_ts"


def orders(b: DataFrame):
    df = b.select(
        clean_str("order_id").alias("order_id"),
        clean_str("customer_id").alias("customer_id"),
        F.to_timestamp("order_ts").alias("order_ts"),
        F.lower(clean_str("status")).alias("status"),
        F.upper(clean_str("currency")).alias("currency"),
        *META,
    )
    rules = [("order_id_missing", F.col("order_id").isNotNull()),
             ("customer_id_missing", F.col("customer_id").isNotNull()),
             ("order_ts_invalid", F.col("order_ts").isNotNull()),
             ("status_unknown", F.col("status").isin("placed", "shipped", "completed", "cancelled", "returned"))]
    return df, rules, ["order_id"], "_ingest_ts"


def order_items(b: DataFrame):
    df = b.select(
        clean_str("order_item_id").alias("order_item_id"),
        clean_str("order_id").alias("order_id"),
        clean_str("product_id").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        F.coalesce(F.col("discount_pct").cast("double"), F.lit(0.0)).alias("discount_pct"),
        *META,
    )
    rules = [("order_item_id_missing", F.col("order_item_id").isNotNull()),
             ("order_id_missing", F.col("order_id").isNotNull()),
             ("product_id_missing", F.col("product_id").isNotNull()),
             ("quantity_not_positive", F.col("quantity") > 0),
             ("unit_price_invalid", F.col("unit_price") > 0),
             ("discount_out_of_range", F.col("discount_pct").between(0, 100))]
    return df, rules, ["order_item_id"], "_ingest_ts"


BUILDERS = {"customers": customers, "products": products, "orders": orders, "order_items": order_items}


def transform(spark, entity: str, ds: str) -> None:
    bronze = spark.read.format("delta").load(lake_path("bronze", entity)).filter(F.col("_batch_date") == ds)
    typed, rules, key, order_col = BUILDERS[entity](bronze)
    valid, bad = split_valid(typed, rules)
    valid = dedupe(valid, key, order_col)
    n_valid, n_bad = valid.count(), bad.count()

    upsert(spark, valid, lake_path("silver", entity), key, order_col)
    (bad.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"_batch_date = '{ds}'")
        .partitionBy("_batch_date")
        .save(lake_path("silver", f"_quarantine/{entity}")))
    print(f"[silver] {entity}: {n_valid} valid (post-dedupe), {n_bad} quarantined for {ds}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--entity", choices=ENTITIES + ["all"], default="all")
    p.add_argument("--ds", required=True)
    a = p.parse_args()
    spark = get_spark(f"silver_{a.entity}")
    for e in (ENTITIES if a.entity == "all" else [a.entity]):
        transform(spark, e, a.ds)
    spark.stop()
