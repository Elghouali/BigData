"""Spark Bronze, Silver, and Gold reference pipeline."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

FIELDS = ["order_id", "order_date", "customer_id", "region", "category", "quantity", "unit_price", "status"]
RAW_SCHEMA = StructType([StructField(name, StringType(), True) for name in FIELDS])


def create_spark() -> SparkSession:
    return (SparkSession.builder.master("local[2]").appName("RetailLakehouse")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def classify_orders(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Demo deduplication is safe for identical sample duplicates only."""
    unique = raw.dropDuplicates(["order_id"])
    typed = (unique
             .withColumn("order_id", F.trim("order_id"))
             .withColumn("customer_id", F.trim("customer_id"))
             .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
             .withColumn("quantity", F.col("quantity").cast("int"))
             .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
             .withColumn("status", F.lower(F.trim("status"))))
    reason = (
        F.when(F.col("order_id").isNull() | (F.length("order_id") == 0), "missing_order_id")
         .when(F.col("customer_id").isNull() | (F.length("customer_id") == 0), "missing_customer")
         .when(F.col("order_date").isNull(), "invalid_date")
         .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
         .when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), "invalid_unit_price")
         .when(~F.col("status").isin("paid", "cancelled"), "invalid_status")
    )
    labelled = typed.withColumn("reject_reason", reason)
    return (labelled.filter(F.col("reject_reason").isNull()).drop("reject_reason"),
            labelled.filter(F.col("reject_reason").isNotNull()))


def aggregate_revenue(valid: DataFrame) -> DataFrame:
    return (valid.filter(F.col("status") == "paid")
            .groupBy("order_date", "category")
            .agg(F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("revenue_mad"),
                 F.countDistinct("order_id").alias("paid_orders"))
            .orderBy("order_date", "category"))


def run_pipeline(spark: SparkSession, source: str, destination: str) -> dict[str, int]:
    raw = spark.read.option("header", True).schema(RAW_SCHEMA).csv(source)
    silver, rejected = classify_orders(raw)
    gold = aggregate_revenue(silver)
    dest = Path(destination)
    dest.mkdir(parents=True, exist_ok=True)
    raw.write.mode("overwrite").parquet(str(dest / "bronze"))
    silver.write.mode("overwrite").partitionBy("order_date").parquet(str(dest / "silver"))
    rejected.write.mode("overwrite").parquet(str(dest / "rejected"))
    gold.write.mode("overwrite").parquet(str(dest / "gold"))
    count = raw.count()
    return {
        "source_rows": count,
        "duplicate_ids_removed": count - raw.dropDuplicates(["order_id"]).count(),
        "silver_rows": silver.count(),
        "rejected_rows": rejected.count(),
        "gold_rows": gold.count(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/orders.csv")
    parser.add_argument("--output", default="output")
    args = parser.parse_args()
    if not Path(args.input).is_file():
        parser.error("Input file not found: " + args.input)
    spark = create_spark()
    try:
        print(json.dumps(run_pipeline(spark, args.input, args.output), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
