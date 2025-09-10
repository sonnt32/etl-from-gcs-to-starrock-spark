# etl_spark.py
# ================================
# Clean ETL: Extract → Transform → Load
# - Export GA4 (BQ) -> GCS (Parquet)
# - Read Parquet from GCS -> Transform (Spark)
# - Write processed Parquet to GCS -> Load to BigQuery
# ================================

import os
import argparse
import json
import logging
from datetime import datetime, timedelta

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf, col, to_json, trim, to_timestamp, datediff, to_date, expr
)
from pyspark.sql.types import StringType

from google.cloud import bigquery, storage
from google.oauth2 import service_account

# ------------------------
# Logging
# ------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("etl-spark")

# ------------------------
# CLI args
# ------------------------
parser = argparse.ArgumentParser(description="ETL Pipeline with Spark")
parser.add_argument("--date", type=str, help="YYYYMMDD; default = yesterday (UTC)")
parser.add_argument("--credentials", type=str, required=True, help="Path to service account key JSON")
parser.add_argument("--processed_dataset", type=str, default="processed_dataset")
parser.add_argument("--out_partitions", type=int, default=0, help="Number of output partitions when writing to GCS")
args = parser.parse_args()

print("DEBUG >>> out_partitions =", args.out_partitions)
if args.date:
    input_date = args.date
else:
    input_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")

GOOGLE_CREDENTIALS_PATH = args.credentials
credentials = service_account.Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH)
PROJECT_ID = credentials.project_id
BUCKET_NAME = f"{PROJECT_ID}-export-demo"

storage_client = storage.Client(credentials=credentials, project=PROJECT_ID)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# ------------------------
# Spark init (GCS configs included)
# ------------------------
spark = (
    SparkSession.builder
    .appName("ETL Spark GCS")
    # (nếu chạy local, bật local[*] hoặc để spark tự quyết)
    .config("spark.master", "local[*]")
    # tăng heap cho driver/executor (chỉnh theo RAM máy bạn)
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")
    # filesystem impl + credentials
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_CREDENTIALS_PATH)
    # GCS chunk size: dùng bội số 8MB (8388608)
    .config("spark.hadoop.fs.gs.outputstream.buffer.size", "8388608")
    .config("spark.hadoop.fs.gs.outputstream.upload.chunk.size", "8388608")
    .config("spark.hadoop.fs.gs.outputstream.upload.retry.limit", "10")
    .config("spark.hadoop.fs.gs.inputstream.connect.timeout", "60000")
    .config("spark.hadoop.fs.gs.inputstream.read.timeout", "60000")
    # shuffle partitions (tùy chỉnh; nếu out_partitions lớn, bạn có thể set = max(200, out_parts))
    .config("spark.sql.shuffle.partitions", str(max(200, args.out_partitions)))
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")
    .getOrCreate()
)
# giảm kích thước row-group Parquet để mỗi writer tiêu tốn ít RAM hơn
spark.conf.set("parquet.block.size", 33554432)   # 32 MB row groups (mặc định 128MB)
spark.conf.set("parquet.page.size", 1048576)     # 1MB pages

# ------------------------
# Constants
# ------------------------
NESTED_PARAMS_COLS = ["event_params", "user_properties"]
DICT_COLS = ["geo", "device", "app_info", "traffic_source"]
TIMESTAMP_COLS = ["event_timestamp", "user_first_touch_timestamp"]
TRIM_EVENT_NAME = True

EXPECTED_OUTPUT_COLS = [
    "event_date", "event_timestamp", "event_name", "user_pseudo_id",
    "event_params", "user_properties", "geo", "device", "app_info",
    "platform", "traffic_source", "event_value_in_usd", "retention_day"
]

# ------------------------
# Utils
# ------------------------
def ensure_bucket(bucket_name: str):
    try:
        storage_client.get_bucket(bucket_name)
        log.info(f"✅ GCS bucket exists: {bucket_name}")
    except Exception:
        log.info(f"⚠️  GCS bucket not found, creating: {bucket_name}")
        storage_client.create_bucket(bucket_name, location="US")
        log.info(f"✅ Created bucket: {bucket_name}")

def ensure_dataset(dataset_id: str):
    full_id = f"{PROJECT_ID}.{dataset_id}"
    try:
        bq_client.get_dataset(full_id)
        log.info(f"✅ BigQuery dataset exists: {full_id}")
    except Exception:
        log.info(f"⚠️  Creating BigQuery dataset: {full_id}")
        bq_client.create_dataset(bigquery.Dataset(full_id), exists_ok=True)

# ------------------------
# Source table detection
# ------------------------
datasets = [d.dataset_id for d in bq_client.list_datasets() if d.dataset_id.startswith("analytics_")]
if not datasets:
    raise ValueError("❌ No dataset starts with 'analytics_'")
DATASET_ID = datasets[0]
log.info(f"📦 Using dataset: {DATASET_ID}")

tables = [t.table_id for t in bq_client.list_tables(DATASET_ID)]
intraday_table = f"events_intraday_{input_date}"
daily_table = f"events_{input_date}"

if intraday_table in tables:
    SOURCE_TABLE_ID = intraday_table
    log.info(f"✅ Using intraday table: {intraday_table}")
elif daily_table in tables:
    SOURCE_TABLE_ID = daily_table
    log.info(f"⚠️ No intraday, using daily: {daily_table}")
else:
    raise ValueError(f"❌ Table not found: {intraday_table} or {daily_table}")

EXTRACTED_PREFIX = f"extracted/{SOURCE_TABLE_ID}"
TRANSFORMED_PREFIX = f"transformed/events_{input_date}"

# ------------------------
# Extract
# ------------------------
def extract_to_gcs(dataset_id: str, source_table_id: str):
    table_ref = bigquery.TableReference.from_string(f"{PROJECT_ID}.{dataset_id}.{source_table_id}")
    destination_uri = f"gs://{BUCKET_NAME}/{EXTRACTED_PREFIX}/part-*.parquet"
    job_config = bigquery.ExtractJobConfig(destination_format=bigquery.DestinationFormat.PARQUET)

    log.info(f"🚀 Exporting {table_ref} → {destination_uri}")
    job = bq_client.extract_table(table_ref, destination_uri, job_config=job_config)
    job.result()
    log.info("✅ Export completed.")

# ------------------------
# Transform
# ------------------------
def transform(df):
    # --- Robust Python fallback for nested params ---
    def convert_nested(params):
        if not params:
            return None
        result = {}
        for p in params:
            # Row -> dict if needed
            try:
                if hasattr(p, "asDict"):
                    pd = p.asDict()
                elif isinstance(p, dict):
                    pd = p
                else:
                    # unknown type: try converting via json if possible
                    pd = dict(p)
            except Exception:
                continue

            key = pd.get("key")
            if key is None:
                continue

            v = pd.get("value", {}) or {}

            if isinstance(v, dict):
                # prioritized typed fields
                if v.get("string_value") is not None:
                    result[key] = v["string_value"]
                elif v.get("int_value") is not None:
                    result[key] = v["int_value"]
                elif v.get("float_value") is not None:
                    result[key] = v["float_value"]
                elif v.get("double_value") is not None:
                    result[key] = v["double_value"]
                else:
                    # fallback: try first primitive-like entry
                    for k2, vv in v.items():
                        if vv is not None and isinstance(vv, (str, int, float, bool)):
                            result[key] = vv
                            break
            else:
                # primitive already
                result[key] = v

        return json.dumps(result) if result else None

    convert_nested_udf = udf(convert_nested, StringType())

    # Try to use Spark SQL higher-order functions first (much faster).
    # If that fails due to schema mismatch, fallback to Python UDF per column.
    for c in NESTED_PARAMS_COLS:
        if c in df.columns:
            try:
                # This expression creates a map from entries and then json-encodes it.
                # coalesce tries string, int, float, double in order, cast ints/doubles to string.
                expr_template = f"""
                  to_json(
                    map_from_entries(
                      transform({c}, x ->
                        struct(
                          x.key,
                          coalesce(
                            x.value.string_value,
                            cast(x.value.int_value as string),
                            cast(x.value.float_value as string),
                            cast(x.value.double_value as string)
                          )
                        )
                      )
                    )
                  )
                """
                df = df.withColumn(c, expr(expr_template))
                log.info(f"ℹ️  Applied SQL transformation for column: {c}")
            except Exception as e:
                log.warning(f"⚠️  SQL transform failed for {c}: {e}. Falling back to Python UDF.")
                df = df.withColumn(c, convert_nested_udf(col(c)))

    # Dict cols → JSON string + normalize (keep stable JSON)
    def normalize_json(s):
        try:
            # ensure stable canonical JSON string
            parsed = json.loads(s) if s is not None else None
            return json.dumps(parsed) if parsed is not None else None
        except Exception:
            return s

    normalize_json_udf = udf(normalize_json, StringType())
    for c in DICT_COLS:
        if c in df.columns:
            try:
                df = df.withColumn(c, to_json(col(c)))
                df = df.withColumn(c, normalize_json_udf(col(c)))
            except Exception as e:
                log.warning(f"⚠️  Could not to_json column {c}: {e}")

    # Timestamp microseconds → timestamp
    for c in TIMESTAMP_COLS:
        if c in df.columns:
            try:
                df = df.withColumn(c, to_timestamp(col(c) / 1_000_000))
            except Exception as e:
                log.warning(f"⚠️  Could not convert timestamp for {c}: {e}")

    # Trim event_name
    if TRIM_EVENT_NAME and "event_name" in df.columns:
        df = df.withColumn("event_name", trim(col("event_name")))

    # Retention day
    if set(TIMESTAMP_COLS).issubset(df.columns):
        df = df.withColumn(
            "retention_day",
            datediff(to_date(col("event_timestamp")), to_date(col("user_first_touch_timestamp")))
        )

    # Select expected cols (if exist)
    cols = [c for c in EXPECTED_OUTPUT_COLS if c in df.columns]
    if cols:
        df = df.select(*cols)

    return df

def read_raw_parquet_to_df():
    uri = f"gs://{BUCKET_NAME}/{EXTRACTED_PREFIX}/*.parquet"
    log.info(f"📥 Reading parquet files from {uri}")
    df = spark.read.parquet(uri)
    # count may be expensive; keep but wrapped
    try:
        cnt = df.count()
    except Exception:
        cnt = "unknown"
    log.info(f"✅ Read completed: {cnt} rows, {len(df.columns)} columns")
    return df

# ------------------------
# Load
# ------------------------
def write_transformed_to_gcs(df):
    # Prepare output path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_uri = f"gs://{BUCKET_NAME}/{TRANSFORMED_PREFIX}/"

    # Nếu user truyền tham số thì dùng, ngược lại tự động tính dựa trên row_count
    if args.out_partitions and args.out_partitions > 0:
        out_parts = args.out_partitions
        log.info(f"ℹ️ Using user-provided out_partitions = {out_parts}")
    else:
        log.info("ℹ️ out_partitions not provided, determining automatically...")
        try:
            row_count = df.count()
            if row_count < 100_000:
                out_parts = 8
            elif row_count < 1_000_000:
                out_parts = 16
            elif row_count < 5_000_000:
                out_parts = 64
            else:
                out_parts = 128
            log.info(f"ℹ️ Row count = {row_count:,}, selected out_partitions = {out_parts}")
        except Exception as e:
            log.warning(f"⚠️ Could not determine row count automatically: {e}")
            out_parts = 16  # fallback mặc định

    try:
        df_to_write = df.repartition(out_parts)
    except Exception:
        df_to_write = df

    log.info(f"📤 Writing transformed parquet to {output_uri} with {out_parts} partitions")

    try:
        # Main write to GCS
        df_to_write.write.mode("overwrite") \
            .option("maxRecordsPerFile", 50000) \
            .parquet(output_uri)
        log.info(f"✅ Transformed parquet written to: {output_uri}")
    except Exception as e:
        log.error(f"❌ Failed to write transformed parquet to GCS: {e}")
        # fallback: write locally then copy (if local disk available)
        local_tmp = f"/tmp/df_transformed_{timestamp}"
        try:
            log.info(f"➡️  Attempting fallback: write locally to {local_tmp}")
            df_to_write.write.mode("overwrite").parquet(local_tmp)
            from pathlib import Path
            p = Path(local_tmp)
            files = list(p.rglob("*.parquet"))
            if not files:
                raise RuntimeError("No parquet files produced locally")
            bucket = storage_client.bucket(BUCKET_NAME)
            for fpath in files:
                rel = fpath.relative_to(local_tmp)
                remote_blob = f"{TRANSFORMED_PREFIX}/{rel}"
                blob = bucket.blob(remote_blob)
                blob.upload_from_filename(str(fpath))
            log.info("✅ Fallback upload to GCS succeeded.")
            output_uri = f"gs://{BUCKET_NAME}/{TRANSFORMED_PREFIX}/"
        except Exception as e2:
            log.error(f"❌ Fallback also failed: {e2}")
            raise
    return output_uri


def load_parquet_to_bq(gcs_uri: str, dataset_id: str, table_base: str, date_str: str):
    ensure_dataset(dataset_id)
    table_id = f"{PROJECT_ID}.{dataset_id}.{table_base}_{date_str}"

    # 🔧 Nếu chỉ truyền folder → thêm pattern *.parquet
    if not gcs_uri.endswith(".parquet") and not gcs_uri.endswith("*.parquet"):
        gcs_uri = gcs_uri.rstrip("/") + "/*.parquet"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    log.info(f"🚀 Loading {gcs_uri} → {table_id}")
    job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    job.result()

    rows = bq_client.get_table(table_id).num_rows
    log.info(f"✅ Load completed. Rows in table: {rows:,}")

# ------------------------
# Main
# ------------------------
def main():
    ensure_bucket(BUCKET_NAME)

    log.info("➡️  Starting extract")
    extract_to_gcs(DATASET_ID, SOURCE_TABLE_ID)

    log.info("➡️  Reading extracted parquet into Spark")
    df_raw = read_raw_parquet_to_df()

    log.info("➡️  Transforming dataframe")
    df_transformed = transform(df_raw)

    log.info("➡️  Writing transformed parquet to GCS")
    transformed_uri = write_transformed_to_gcs(df_transformed)

    log.info("➡️  Loading transformed parquet to BigQuery")
    load_parquet_to_bq(
        transformed_uri,
        args.processed_dataset,
        "transformed_events",
        input_date
    )

if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            spark.stop()
        except Exception:
            pass
##python etl-spark.py --credentials "C:\Users\PC\Downloads\service_account\brain-puzzle-tricky-test.json" --date 20250904
