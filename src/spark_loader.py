"""Spark execution and MongoDB output for the hybrid data pipeline."""
import json
import math
import os
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.settings import (
    COLLECTION_QUARANTINE, COLLECTION_RAW, COLLECTION_VALIDATED, DB_NAME,
    MONGO_BATCH_SIZE, MONGO_SPARK_CONNECTOR_PACKAGE, MONGODB_URI,
    SPARK_APP_NAME, SPARK_LOCAL_DIR, SPARK_MASTER, SPARK_MAX_PARTITION_BYTES,
    SPARK_SHUFFLE_PARTITIONS, RAW_SOURCE_COLLECTION,
)
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window
RAW_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_phone", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("delivery_type", StringType(), True),
    StructField("delivery_cost", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("items_json", StringType(), True),
])
_CSV_COLUMNS = [field.name for field in RAW_SCHEMA.fields]
ITEM_SCHEMA = ArrayType(StructType([
    StructField("sku", StringType(), True),
    StructField("name", StringType(), True),
    StructField("qty", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("total", StringType(), True),
]))
CORRECTION_SCHEMA = StructType([
    StructField("field", StringType(), False),
    StructField("original_value", StringType(), True),
    StructField("corrected_value", StringType(), True),
    StructField("rule_code", StringType(), False),
])
def _arabic_digit_text(column):
    return F.translate(column, "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
def _clean_text(column):
    return F.trim(
        F.regexp_replace(
            F.regexp_replace(F.coalesce(column, F.lit("")), "\\u00a0", " "),
            "\\u200b",
            "",
        )
    )
def _money_expression(column):
    normalized = _arabic_digit_text(F.trim(F.coalesce(column, F.lit(""))))
    normalized = F.regexp_replace(normalized, "٫", ".")
    normalized = F.regexp_replace(normalized, "لاير يمني|ريال يمني|لاير|ريال|ر\\.ي\\.?", "")
    normalized = F.regexp_replace(normalized, "٬", "")
    normalized = F.regexp_replace(normalized, ",(?=\\d{3}(?:\\D|$))", "")
    normalized = F.trim(normalized)
    word_values = {
        "ألف": "1000", "الف": "1000", "ألفان": "2000", "الفان": "2000",
        "ألفين": "2000", "الفين": "2000", "ثلاثة آلاف": "3000", "ثلاثة الاف": "3000",
        "أربعة آلاف": "4000", "اربعة الاف": "4000", "خمسة آلاف": "5000",
        "خمسة الاف": "5000", "ستة آلاف": "6000", "ستة الاف": "6000",
        "سبعة آلاف": "7000", "سبعة الاف": "7000", "ثمانية آلاف": "8000",
        "ثمانية الاف": "8000", "تسعة آلاف": "9000", "تسعة الاف": "9000",
        "عشرة آلاف": "10000", "عشرة الاف": "10000",
    }
    word_map = F.create_map(*[item for pair in word_values.items() for item in (F.lit(pair[0]), F.lit(pair[1]))])
    normalized = F.coalesce(F.element_at(word_map, normalized), normalized)
    valid_number = normalized.rlike(r"^-?\d+(?:\.\d+)?$")
    return F.when(valid_number, normalized.cast(DoubleType())).otherwise(F.lit(None).cast(DoubleType()))
def _correction(field, original, corrected, rule_code):
    return F.struct(
        F.lit(field).alias("field"),
        original.cast(StringType()).alias("original_value"),
        corrected.cast(StringType()).alias("corrected_value"),
        F.lit(rule_code).alias("rule_code"),
    )
def _native_transformations(df, id_run, file_source):
    text_columns = ["status", "customer_name", "city", "district", "delivery_type", "payment_method", "payment_status"]
    cleaned = df
    for name in text_columns:
        cleaned = cleaned.withColumn(name, _clean_text(F.col(name)))
    cleaned = (
        cleaned
        .withColumn("order_id_clean", _clean_text(F.col("order_id")))
        .withColumn("customer_id_clean", _clean_text(F.col("customer_id")))
        .withColumn("customer_phone_digits", F.regexp_replace(_arabic_digit_text(_clean_text(F.col("customer_phone"))), "[^0-9]", ""))
        .withColumn("customer_phone_clean", F.when(F.col("customer_phone_digits").startswith("00967"), F.expr("substring(customer_phone_digits, 6, 99)")).when(F.col("customer_phone_digits").startswith("967"), F.expr("substring(customer_phone_digits, 4, 99)")).otherwise(F.col("customer_phone_digits")))
        .withColumn("customer_email_clean", F.regexp_replace(F.regexp_replace(_clean_text(F.col("customer_email")), "@@+", "@"), "\\.{2,}", "."))
        .withColumn("order_date_text", F.regexp_replace(_arabic_digit_text(_clean_text(F.col("order_date"))), "/", "-"))
        .withColumn("order_date_clean", F.coalesce(
            F.expr("try_to_timestamp(order_date_text, 'yyyy-MM-dd HH:mm:ss')"),
            F.expr("try_to_timestamp(order_date_text, \"yyyy-MM-dd'T'HH:mm:ss\")"),
            F.expr("try_to_timestamp(order_date_text, 'yyyy-MM-dd HH:mm')"),
            F.expr("try_to_timestamp(order_date_text, 'yyyy-MM-dd')"),
            F.expr("try_to_timestamp(order_date_text, 'dd-MM-yyyy HH:mm:ss')"),
            F.expr("try_to_timestamp(order_date_text, 'dd-MM-yyyy HH:mm')"),
            F.expr("try_to_timestamp(order_date_text, 'dd-MM-yyyy')")
        ))
        .withColumn("delivery_cost_clean", _money_expression(F.col("delivery_cost")))
        .withColumn("payment_amount_clean", _money_expression(F.col("payment_amount")))
        .withColumn("total_amount_clean", _money_expression(F.col("total_amount")))
        .withColumn("currency_clean", F.lit("YER"))
        .withColumn("items_parsed", F.from_json(F.col("items_json"), ITEM_SCHEMA))
    )
    cleaned = cleaned.withColumn(
        "items_numeric",
        F.transform(
            "items_parsed",
            lambda item: F.struct(
                item["sku"].alias("sku"),
                item["name"].alias("name"),
                _money_expression(item["qty"]).alias("qty"),
                _money_expression(item["unit_price"]).alias("unit_price"),
                _money_expression(item["total"]).alias("total"),
            ),
        ),
    )
    cleaned = cleaned.withColumn(
        "items_clean",
        F.filter(
            "items_numeric",
            lambda item: item["qty"].isNotNull()
            & item["unit_price"].isNotNull()
            & item["total"].isNotNull()
            & (item["qty"] >= 0)
            & (item["unit_price"] >= 0)
            & (item["total"] >= 0),
        ),
    )
    has_bad_items = F.col("items_parsed").isNull()
    has_negative_item_values = F.coalesce(
        (~has_bad_items)
        & F.exists(
            "items_numeric",
            lambda item: (item["qty"] < 0) | (item["unit_price"] < 0) | (item["total"] < 0),
        ),
        F.lit(False),
    )
    has_empty_items = (~has_bad_items) & (
        (F.size("items_parsed") == 0)
        | ((F.size("items_clean") == 0) & ~has_negative_item_values)
    )
    has_invalid_date = (
        (F.col("order_date_text") == "")
        | F.col("order_date_clean").isNull()
        | (F.year("order_date_clean") < 2000)
        | (F.year("order_date_clean") > 2100)
    )
    has_bad_email = (
        (F.col("customer_email_clean") != "")
        & ~F.col("customer_email_clean").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    )
    items_total = F.aggregate(
        F.transform(F.col("items_clean"), lambda item: item["total"]),
        F.lit(0.0),
        lambda total, value: total + value,
    )
    can_recompute_total = (~has_bad_items) & (~has_empty_items) & (~has_negative_item_values)
    delivery_cost_final = F.coalesce(F.col("delivery_cost_clean"), F.lit(0.0))
    recomputed_total = items_total + delivery_cost_final
    total_was_recomputed = can_recompute_total & (
        F.col("total_amount_clean").isNull() | (F.abs(F.col("total_amount_clean") - recomputed_total) > F.lit(0.5))
    )
    cleaned = (
        cleaned
        .withColumn("delivery_cost_final", delivery_cost_final)
        .withColumn("total_amount_final", F.when(can_recompute_total, recomputed_total).otherwise(F.col("total_amount_clean")))
    )
    order_date_iso = F.when(F.col("order_date_clean").isNotNull(), F.date_format(F.col("order_date_clean"), "yyyy-MM-dd'T'HH:mm:ss"))
    money_correction_pattern = r"[٠-٩۰-۹٫٬]|,\d{3}|ريال|لاير|ر\.ي|آلاف|ألف|الف"
    correction_candidates = F.array(
        F.when(F.col("order_date_clean").isNotNull() & (order_date_iso != _clean_text(F.col("order_date"))), _correction("order_date", F.col("order_date"), order_date_iso, "DATE_FORMAT_NORMALIZED")),
        F.when((_clean_text(F.col("currency")) != "") & (F.col("currency_clean") != _clean_text(F.col("currency"))), _correction("currency", F.col("currency"), F.col("currency_clean"), "CURRENCY_UNIFIED")),
        F.when(_clean_text(F.col("delivery_cost")).rlike(money_correction_pattern), _correction("delivery_cost", F.col("delivery_cost"), F.col("delivery_cost_clean"), "ARABIC_DIGITS_CONVERTED")),
        F.when(_clean_text(F.col("payment_amount")).rlike(money_correction_pattern), _correction("payment_amount", F.col("payment_amount"), F.col("payment_amount_clean"), "ARABIC_DIGITS_CONVERTED")),
        F.when(_clean_text(F.col("total_amount")).rlike(money_correction_pattern), _correction("total_amount", F.col("total_amount"), F.col("total_amount_clean"), "ARABIC_DIGITS_CONVERTED")),
        F.when(F.col("items_json").rlike(money_correction_pattern), _correction("items_json", F.col("items_json"), F.col("items_clean").cast(StringType()), "ARABIC_DIGITS_CONVERTED")),
        F.when(F.col("customer_email_clean") != _clean_text(F.col("customer_email")), _correction("customer_email", F.col("customer_email"), F.col("customer_email_clean"), "EMAIL_REPEATED_SYMBOLS")),
        F.when(F.col("customer_phone_clean") != _clean_text(F.col("customer_phone")), _correction("customer_phone", F.col("customer_phone"), F.col("customer_phone_clean"), "PHONE_FORMAT_NORMALIZED")),
        F.when(total_was_recomputed, _correction("total_amount", F.col("total_amount"), F.col("total_amount_final"), "ORDER_TOTAL_RECALCULATED")),
    )
    cleaned = cleaned.withColumn("corrections", F.filter(correction_candidates, lambda value: value.isNotNull()))
    errors = F.array(
        F.when(F.col("order_id_clean") == "", F.lit("MISSING_ORDER_ID")),
        F.when(F.col("customer_id_clean") == "", F.lit("MISSING_CUSTOMER_ID")),
        F.when(has_invalid_date, F.lit("INVALID_IMPOSSIBLE_DATE")),
        F.when(has_bad_items, F.lit("CORRUPTED_ITEMS_JSON")),
        F.when(has_empty_items, F.lit("EMPTY_ITEMS")),
        F.when(has_negative_item_values, F.lit("AMBIGUOUS_NEGATIVE_VALUE")),
        F.when(~can_recompute_total & F.col("total_amount_final").isNull(), F.lit("UNKNOWN_PRICE")),
        F.when(has_bad_email, F.lit("EMAIL_UNFIXABLE")),
    )
    return (
        cleaned
        .withColumn("error_candidates", F.filter(errors, lambda value: value.isNotNull()))
        .withColumn("run_id", F.lit(id_run))
        .withColumn("source_file", F.lit(file_source))
        .withColumn("engine_used", F.lit("pyspark"))
        .withColumn("source_row_number", F.monotonically_increasing_id())
        .withColumn("ingested_at", F.current_timestamp())
    )
def build_spark_session():
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.mongodb.write.connection.uri", f"{MONGODB_URI.rstrip('/')}/{DB_NAME}")
        .config("spark.jars.packages", MONGO_SPARK_CONNECTOR_PACKAGE)
        .config("spark.sql.shuffle.partitions", str(SPARK_SHUFFLE_PARTITIONS))
        .config("spark.local.dir", SPARK_LOCAL_DIR)
        .config("spark.sql.files.maxPartitionBytes", str(SPARK_MAX_PARTITION_BYTES))
        .config("spark.python.worker.timeout", "600")
        .config("spark.network.timeout", "600s")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )
def _connector_write(df, collection, upsert=False):
    writer = (
        df.write.format("mongodb")
        .mode("append")
        .option("database", DB_NAME)
        .option("collection", collection)
        .option("batchSize", str(MONGO_BATCH_SIZE))
    )
    if upsert:
        writer = writer.option("operationType", "replace").option("idFieldList", "order_id").option("upsertDocument", "true")
    writer.save()
def run_spark_load(file_path, spark, id_run, metrics):
    file_source = str(file_path)
    resume_id_run = os.getenv("SPARK_RESUME_ID_RUN", "").strip()
    resume_skip_raw = os.getenv("SPARK_RESUME_SKIP_RAW", "").lower() == "true"
    resume_from_raw = os.getenv("SPARK_RESUME_FROM_RAW", "").lower() == "true"
    if resume_id_run:
        id_run = resume_id_run
    start = time.perf_counter()
    if resume_from_raw:
        raw_projection = {name: 1 for name in _CSV_COLUMNS}
        raw_pipeline = json.dumps([
            {"$match": {"run_id": id_run}},
            {"$project": raw_projection},
        ])
        df = (
            spark.read.format("mongodb")
            .option("database", DB_NAME)
            .option("collection", RAW_SOURCE_COLLECTION)
            .option("aggregation.pipeline", raw_pipeline)
            .option("spark.mongodb.read.batchSize", "32")
            .load()
            .select(*_CSV_COLUMNS)
        )
        file_source = f"raw://{COLLECTION_RAW}/{id_run}"
        metrics.partitions = None
        print(f"[SparkLoader] Resume from MongoDB Raw: collection={RAW_SOURCE_COLLECTION}, id_run={id_run}")
    else:
        df = spark.read.option("header", "true").option("multiLine", "false").option("escape", '"').schema(RAW_SCHEMA).csv(file_source)
        try:
            metrics.partitions = max(1, math.ceil(Path(file_path).stat().st_size / SPARK_MAX_PARTITION_BYTES))
        except (OSError, TypeError, ValueError):
            metrics.partitions = None
    actual_partitions = df.rdd.getNumPartitions()
    metrics.partitions = actual_partitions
    print(f"[SparkLoader] Actual input partitions: {actual_partitions}")
    total_rows = df.count()
    metrics.read_rows = total_rows
    print(f"[SparkLoader] عدد السجلات المقروءة: {total_rows}")
    raw_df = (
        df.withColumn("run_id", F.lit(id_run))
        .withColumn("source_file", F.lit(file_source))
        .withColumn("source_row_number", F.monotonically_increasing_id())
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("engine_used", F.lit("pyspark"))
        .withColumn("raw_record", F.struct(*[F.col(c) for c in _CSV_COLUMNS]))
        .select("run_id", "source_file", "source_row_number", "ingested_at", "engine_used", "raw_record")
    )
    if resume_skip_raw:
        print(f"[SparkLoader] استئناف: تم تجاوز إعادة كتابة {COLLECTION_RAW} باستخدام id_run={id_run}.")
    else:
        _connector_write(raw_df, COLLECTION_RAW)
    metrics.loaded_raw = total_rows
    transformed = _native_transformations(df, id_run, file_source)
    duplicate_window = Window.partitionBy("order_id_clean").orderBy(F.col("source_row_number"))
    transformed = transformed.withColumn(
        "duplicate_rank",
        F.when(
            F.col("order_id_clean") != "",
            F.row_number().over(duplicate_window),
        ).otherwise(F.lit(1)),
    )
    transformed = transformed.withColumn(
        "error_codes",
        F.array_union(
            F.col("error_candidates"),
            F.when(F.col("duplicate_rank") > 1, F.array(F.lit("DUPLICATE_ORDER_ID")))
            .otherwise(F.array().cast(ArrayType(StringType()))),
        ),
    )
    transformed = transformed.withColumn(
        "quality_status",
        F.when(F.size("error_codes") > 0, F.lit("quarantine"))
        .when(F.size("corrections") > 0, F.lit("corrected"))
        .otherwise(F.lit("valid")),
    ).withColumn(
        "error_details",
        F.concat_ws("; ", F.col("error_codes")),
    ).withColumn(
        "error_codes",
        F.when(F.size("error_codes") > 1, F.array(F.lit("MULTIPLE_CONFLICTING_ERRORS"))).otherwise(F.col("error_codes")),
    )
    validated_df = transformed.filter(F.col("quality_status") != "quarantine").select(
        F.col("order_id_clean").alias("order_id"),
        F.col("order_date_clean").alias("order_date"),
        "status", F.col("customer_id_clean").alias("customer_id"), "customer_name",
        F.col("customer_phone_clean").alias("customer_phone"), F.col("customer_email_clean").alias("customer_email"),
        "city", "district", "delivery_type", F.col("delivery_cost_final").alias("delivery_cost"),
        "payment_method", "payment_status", F.col("payment_amount_clean").alias("payment_amount"),
        F.col("currency_clean").alias("currency"), F.col("total_amount_final").alias("total_amount"),
        F.col("items_clean").alias("items"), "quality_status", "corrections", "run_id", "source_file", "engine_used", "ingested_at",
    ).withColumn("record_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in ["order_id", "order_date", "status", "customer_id", "customer_name", "customer_phone", "customer_email", "city", "district", "delivery_type", "delivery_cost", "payment_method", "payment_status", "payment_amount", "currency", "total_amount", "items", "quality_status", "corrections"]])), 256))
    quarantine_df = transformed.filter(F.col("quality_status") == "quarantine").select(
        "error_codes", "error_details", F.col("corrections").alias("partial_corrections_attempted"),
        "run_id", "source_file", "source_row_number", "engine_used", "ingested_at",
        F.struct(*[F.col(c) for c in _CSV_COLUMNS]).alias("raw_record"),
    )
    try:
        existing_raw = (
            spark.read.format("mongodb")
            .option("database", DB_NAME)
            .option("collection", COLLECTION_VALIDATED)
            .load()
        )
        existing_columns = set(existing_raw.columns)
        if {"order_id", "record_hash"}.issubset(existing_columns):
            existing = (
                existing_raw.select("order_id", "record_hash")
                .withColumnRenamed("record_hash", "old_record_hash")
                .dropDuplicates(["order_id"])
            )
            compared = validated_df.join(existing, on="order_id", how="left")
            metrics.count_inserted = compared.filter(F.col("old_record_hash").isNull()).count()
            metrics.count_updated = compared.filter(
                F.col("old_record_hash").isNotNull() & (F.col("old_record_hash") != F.col("record_hash"))
            ).count()
            metrics.count_unchanged = compared.filter(F.col("old_record_hash") == F.col("record_hash")).count()
        else:
            print("[SparkLoader] orders_validated فارغة أو بلا schema؛ اعتُبرت كل النتائج جديدة.")
            metrics.count_inserted = validated_df.count()
            metrics.count_updated = 0
            metrics.count_unchanged = 0
    except Exception as exc:  # First run or an unavailable collection.
        print(f"[SparkLoader] تعذر قراءة حالة validated السابقة؛ اعتُبرت كل النتائج جديدة: {exc}")
        metrics.count_inserted = validated_df.count()
        metrics.count_updated = 0
        metrics.count_unchanged = 0
    _connector_write(validated_df, COLLECTION_VALIDATED, upsert=True)
    _connector_write(quarantine_df, COLLECTION_QUARANTINE)
    metrics.count_quarantine = quarantine_df.count()
    metrics.count_valid = validated_df.filter(F.col("quality_status") == "valid").count()
    metrics.count_corrected = validated_df.filter(F.col("quality_status") == "corrected").count()
    error_rows = (
        quarantine_df.select(F.explode("error_codes").alias("code"))
        .groupBy("code").count().toLocalIterator()
    )
    metrics.counts_case_error = {row["code"]: row["count"] for row in error_rows}
    elapsed = time.perf_counter() - start
    print(f"[SparkLoader] انتهى: {total_rows} سجل خلال {elapsed:.2f}s ({total_rows / elapsed:.1f} سجل/ثانية).")
    return metrics