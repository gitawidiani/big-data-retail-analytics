from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from src.config import get_paths, validate_source_files


SALES_SCHEMA = T.StructType(
    [
        T.StructField("date", T.DateType(), False),
        T.StructField("receipt_id", T.StringType(), False),
        T.StructField("store_id", T.StringType(), False),
        T.StructField("sku_id", T.StringType(), False),
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("quantity", T.IntegerType(), False),
        T.StructField("unit_price", T.DoubleType(), False),
        T.StructField("total_value", T.DoubleType(), False),
        T.StructField("channel", T.StringType(), False),
        T.StructField("discount_pct", T.DoubleType(), False),
        T.StructField("promo_id", T.StringType(), True),
    ]
)

STORE_SCHEMA = T.StructType(
    [
        T.StructField("store_id", T.StringType(), False),
        T.StructField("store_name", T.StringType(), False),
        T.StructField("city", T.StringType(), False),
        T.StructField("store_type", T.StringType(), False),
        T.StructField("opening_date", T.DateType(), False),
    ]
)

SKU_SCHEMA = T.StructType(
    [
        T.StructField("sku_id", T.StringType(), False),
        T.StructField("sku_name", T.StringType(), False),
        T.StructField("category", T.StringType(), False),
        T.StructField("subcategory", T.StringType(), False),
        T.StructField("unit_price", T.DoubleType(), False),
        T.StructField("cost_price", T.DoubleType(), False),
        T.StructField("brand", T.StringType(), False),
    ]
)

CUSTOMER_SCHEMA = T.StructType(
    [
        T.StructField("cust_id", T.StringType(), False),
        T.StructField("age", T.IntegerType(), False),
        T.StructField("gender", T.StringType(), False),
        T.StructField("city", T.StringType(), False),
        T.StructField("loyalty_segment", T.StringType(), False),
        T.StructField("preferred_channel", T.StringType(), False),
        T.StructField("registration_date", T.DateType(), False),
    ]
)

PROMOTION_SCHEMA = T.StructType(
    [
        T.StructField("promo_id", T.StringType(), False),
        T.StructField("promo_name", T.StringType(), False),
        T.StructField("start_date", T.DateType(), False),
        T.StructField("end_date", T.DateType(), False),
        T.StructField("discount_pct", T.DoubleType(), False),
        T.StructField("promo_type", T.StringType(), False),
        T.StructField("target_type", T.StringType(), False),
        T.StructField("target_value", T.StringType(), False),
    ]
)


def create_spark(master: str = "local[*]") -> SparkSession:
    return (
        SparkSession.builder.appName("RetailSalesInventoryAnalytics")
        .master(master)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: Path, schema: T.StructType) -> DataFrame:
    return spark.read.option("header", True).schema(schema).csv(str(path))


def read_sources(spark: SparkSession, source_dir: Path) -> dict[str, DataFrame]:
    return {
        "sales": read_csv(spark, source_dir / "sales_transactions.csv", SALES_SCHEMA),
        "stores": read_csv(spark, source_dir / "store_master.csv", STORE_SCHEMA),
        "skus": read_csv(spark, source_dir / "sku_master.csv", SKU_SCHEMA),
        "customers": read_csv(spark, source_dir / "customer_master.csv", CUSTOMER_SCHEMA),
        "promotions": read_csv(spark, source_dir / "promotions.csv", PROMOTION_SCHEMA),
    }


def validate_sources(frames: dict[str, DataFrame]) -> dict[str, int]:
    sales = frames["sales"]
    required_columns = [
        "date",
        "receipt_id",
        "store_id",
        "sku_id",
        "customer_id",
        "quantity",
        "unit_price",
        "total_value",
        "channel",
        "discount_pct",
    ]
    required_null_condition = F.lit(False)
    for column in required_columns:
        required_null_condition = required_null_condition | F.col(column).isNull()

    calculated_total = F.round(
        F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2
    )
    nonblank_promos = sales.filter(F.col("promo_id").isNotNull())

    checks = {
        "input_rows": sales.count(),
        "rows_with_required_nulls": sales.filter(required_null_condition).count(),
        "invalid_quantity_rows": sales.filter(F.col("quantity") <= 0).count(),
        "invalid_unit_price_rows": sales.filter(F.col("unit_price") < 0).count(),
        "invalid_discount_rows": sales.filter(~F.col("discount_pct").between(0, 100)).count(),
        "formula_mismatch_rows": sales.filter(
            F.abs(F.col("total_value") - calculated_total) > F.lit(0.02)
        ).count(),
        "orphan_store_rows": sales.join(frames["stores"], "store_id", "left_anti").count(),
        "orphan_sku_rows": sales.join(frames["skus"], "sku_id", "left_anti").count(),
        "orphan_customer_rows": sales.join(
            frames["customers"],
            sales.customer_id == frames["customers"].cust_id,
            "left_anti",
        ).count(),
        "orphan_promo_rows": nonblank_promos.join(frames["promotions"], "promo_id", "left_anti").count(),
    }
    return checks


def transform_sources(frames: dict[str, DataFrame]) -> DataFrame:
    sales = frames["sales"]
    stores = frames["stores"].select(
        "store_id",
        "store_name",
        F.col("city").alias("store_city"),
        "store_type",
        "opening_date",
    )
    skus = frames["skus"].select(
        "sku_id",
        "sku_name",
        "category",
        "subcategory",
        F.col("unit_price").alias("list_unit_price"),
        "cost_price",
        "brand",
    )
    customers = frames["customers"].select(
        F.col("cust_id").alias("customer_id"),
        "age",
        "gender",
        F.col("city").alias("customer_city"),
        "loyalty_segment",
        "preferred_channel",
        "registration_date",
    )
    promotions = frames["promotions"].select(
        "promo_id",
        "promo_name",
        "start_date",
        "end_date",
        F.col("discount_pct").alias("promo_discount_pct"),
        "promo_type",
        "target_type",
        "target_value",
    )

    enriched = (
        sales.join(F.broadcast(stores), "store_id", "left")
        .join(F.broadcast(skus), "sku_id", "left")
        .join(F.broadcast(customers), "customer_id", "left")
        .join(F.broadcast(promotions), "promo_id", "left")
        .withColumn("year", F.year("date"))
        .withColumn("month", F.date_trunc("month", "date").cast("date"))
        .withColumn("month_key", F.date_format("date", "yyyy-MM"))
        .withColumn("gross_sales", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("discount_amount", F.round(F.col("gross_sales") - F.col("total_value"), 2))
        .withColumn(
            "calculated_total_value",
            F.round(
                F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100),
                2,
            ),
        )
        .withColumn("gross_profit", F.round(F.col("total_value") - F.col("quantity") * F.col("cost_price"), 2))
        .withColumn(
            "profit_margin",
            F.when(F.col("total_value") != 0, F.col("gross_profit") / F.col("total_value")),
        )
        .withColumn("is_promoted", F.col("promo_id").isNotNull())
    )
    return enriched


def aggregate_outputs(enriched: DataFrame) -> dict[str, DataFrame]:
    metrics = [
        F.count("*").alias("line_items"),
        F.countDistinct("receipt_id").alias("receipts"),
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("total_value"), 2).alias("sales_value"),
        F.round(F.sum("gross_profit"), 2).alias("gross_profit"),
    ]
    receipt_promotion = enriched.groupBy("receipt_id").agg(
        F.max(F.col("is_promoted").cast("int")).alias("receipt_has_promo"),
        F.sum("quantity").alias("basket_units"),
        F.round(F.sum("total_value"), 2).alias("basket_value"),
        F.round(F.sum("gross_profit"), 2).alias("basket_profit"),
    )
    promotion_comparison = (
        receipt_promotion.groupBy("receipt_has_promo")
        .agg(
            F.count("*").alias("receipts"),
            F.round(F.avg("basket_units"), 2).alias("avg_units_per_receipt"),
            F.round(F.avg("basket_value"), 2).alias("avg_value_per_receipt"),
            F.round(F.avg("basket_profit"), 2).alias("avg_profit_per_receipt"),
            F.round(
                F.sum("basket_profit") / F.sum("basket_value") * 100,
                2,
            ).alias("profit_margin_pct"),
        )
        .withColumn(
            "promotion_group",
            F.when(F.col("receipt_has_promo") == 1, "Dengan Promosi").otherwise(
                "Tanpa Promosi"
            ),
        )
        .select(
            "receipt_has_promo",
            "promotion_group",
            "receipts",
            "avg_units_per_receipt",
            "avg_value_per_receipt",
            "avg_profit_per_receipt",
            "profit_margin_pct",
        )
        .orderBy("receipt_has_promo")
    )

    return {
        "monthly_sales": enriched.groupBy("month").agg(*metrics).orderBy("month"),
        "store_sales": enriched.groupBy("store_id", "store_name", "store_city", "store_type")
        .agg(*metrics)
        .orderBy(F.desc("sales_value")),
        "category_sales": enriched.groupBy("category", "subcategory")
        .agg(*metrics)
        .orderBy(F.desc("sales_value")),
        "channel_sales": enriched.groupBy("channel")
        .agg(*metrics)
        .orderBy(F.desc("sales_value")),
        "promotion_sales": enriched.groupBy("is_promoted", "promo_type")
        .agg(*metrics)
        .orderBy(F.desc("sales_value")),
        "promotion_comparison": promotion_comparison,
    }


def write_csv_folder(df: DataFrame, path: Path) -> None:
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(path))


def run_pipeline(variant: str, master: str, skip_write: bool = False) -> dict[str, object]:
    paths = get_paths(variant)
    validate_source_files(paths)
    paths.validation_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark(master)
    started = time.perf_counter()
    frames = read_sources(spark, paths.source_dir)
    checks = validate_sources(frames)
    enriched = transform_sources(frames).persist(StorageLevel.MEMORY_AND_DISK)
    output_rows = enriched.count()

    failed_checks = {
        name: value
        for name, value in checks.items()
        if name != "input_rows" and value != 0
    }
    if failed_checks:
        raise ValueError(f"Quality checks gagal: {failed_checks}")
    if output_rows != checks["input_rows"]:
        raise ValueError(f"Jumlah baris berubah: input={checks['input_rows']}, output={output_rows}")

    aggregates = aggregate_outputs(enriched)
    if not skip_write:
        enriched.write.mode("overwrite").partitionBy("year", "month_key").parquet(
            str(paths.parquet_dir)
        )
        for name, frame in aggregates.items():
            write_csv_folder(frame, paths.validation_dir / name)

    result = {
        "variant": variant,
        "input_rows": checks["input_rows"],
        "output_rows": output_rows,
        "checks": checks,
        "elapsed_seconds": round(time.perf_counter() - started, 2),
        "parquet_path": str(paths.parquet_dir),
    }
    print(json.dumps(result, indent=2))
    enriched.unpersist()
    spark.stop()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline PySpark retail analytics")
    parser.add_argument(
        "--variant",
        choices=["retail_clean_dataset", "retail_contaminated_dataset"],
        default="retail_contaminated_dataset",
    )
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--skip-write", action="store_true")
    args = parser.parse_args()
    run_pipeline(args.variant, args.master, args.skip_write)


if __name__ == "__main__":
    main()

