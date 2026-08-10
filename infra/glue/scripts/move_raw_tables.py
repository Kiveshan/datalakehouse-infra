import sys, traceback
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, when, length, trim, lower, regexp_extract, isnan,
    sum as spark_sum  # Spark aggregator (NOT Python sum)
)

# ===================== Glue / Spark bootstrap =====================
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_database',
    'target_bucket',
    'target_prefix',
])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext); job.init(args['JOB_NAME'], args)

def safe_set_conf(k, v):
    try:
        spark.conf.set(k, v)
    except Exception as e:
        print(f"ℹ️ Could not set Spark conf '{k}': {e}")

# Parquet / timestamp compat (guarded)
safe_set_conf("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
safe_set_conf("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
safe_set_conf("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
safe_set_conf("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

# Light tuning (guarded). Avoid forbidden confs like spark.dynamicAllocation.enabled / spark.scheduler.mode.
safe_set_conf("spark.sql.shuffle.partitions", "96")

# ===================== Config =====================
# Source database (crawled from raw/ by the raw-folder crawler, see catalog.tf)
# and target bucket/prefix (curated/, see storage.tf) are passed in as Glue job
# arguments rather than hardcoded, so this script is environment-agnostic.
source_database = args['source_database']
bucket = args['target_bucket']
# final path: s3://<bucket>/<target_prefix><table>/
curated_prefix = args['target_prefix']
if not curated_prefix.endswith('/'):
    curated_prefix += '/'

# Tables to SKIP (case-insensitive)
EXCLUDE_TABLES = [
    "accounts_useremail",
    "accounts_userforgetpasswordtoken",
    "audit_auditdgcompliancerequestbatch",
    "auth_permission",
    "axes_accessattempt",
    "axes_accesslog",
    "background_task",
    "background_task_completedtask",
    "company_bankingdetails",
    "configurable_bank",
    "configurable_bankaccounttype",
    "configurable_questiontype",
    "configurable_questiontypeoptions",
    "django_content_type",
    "django_migrations",
    "django_session",
    "manual_section",
    "manual_section_roles",
    "manual_usermanual",
    "manual_usermanual_roles",
    "manual_usermanualmedia",
    "src_announcement",
    "src_announcement_roles",
    "src_boardmemberbankdetails"
]
exclude_set = set(t.lower() for t in EXCLUDE_TABLES)

# Optional: force-drop specific column names (case-insensitive)
FORCE_DROP_COLUMNS = [
    # e.g., "supporting_document", "proof_of_payment", "file_path"
]
force_drop_set = set(c.lower() for c in FORCE_DROP_COLUMNS)

# Heuristics for file/path detection
# Case-insensitive + allow query/fragment after extension (? or #)
FILE_EXTENSION_PATTERN = r'(?i)\.(pdf|doc|docx|xls|xlsx|jpg|jpeg|png|gif|txt|csv|zip|rar|ppt|pptx|mp4|mp3|wav)(?:[?#].*)?$'
FILE_KEYWORDS = [
    'letter', 'document', 'file', 'pdf', 'doc', 'attachment', 'upload',
    'certificate', 'report', 'form', 'application', 'submission',
    'acknowledgement', 'approval', 'receipt', 'invoice', 'contract', 'path', 'url'
]
NORMAL_MIN_PCT = 0.05       # 5% of non-empty values look like files/paths → drop
SUSPICIOUS_MIN_PCT = 0.005  # if col name looks suspicious, drop at 0.5%

# Parallelism across tables (tune per DPU; 3–6 is usually safe)
DEFAULT_MAX_WORKERS = 4

# ===================== AWS clients =====================
glue_client = boto3.client('glue')

# ===================== Helpers =====================
def list_all_tables(database: str):
    names = []
    paginator = glue_client.get_paginator('get_tables')
    for page in paginator.paginate(DatabaseName=database):
        for t in page.get('TableList', []):
            if 'Name' in t and t.get('StorageDescriptor'):
                names.append(t['Name'])
    return names

def suspicious_by_name(colname: str) -> bool:
    lc = colname.lower()
    return any(k in lc for k in FILE_KEYWORDS) or (lc in force_drop_set)

def decide_columns_to_keep(df, table_name):
    """
    Single-pass decision:
      • One count() for total rows
      • One .agg() computing:
          - non-empty counts for every column
          - file-like matches for string columns
      • Decide drops from those aggregates
    Returns: (valid_columns, dropped_columns, total_rows)
    """
    total_rows = df.count()
    if total_rows == 0:
        return [], [], 0

    dtypes = dict(df.dtypes)
    columns = df.columns

    # Build aggregations in one go
    agg_exprs = []
    for c in columns:
        cexpr = col(c)
        ctype = dtypes[c]
        if ctype in ('string', 'varchar'):
            non_empty = cexpr.isNotNull() & (cexpr != '') & (length(trim(cexpr)) > 0)
            file_like = regexp_extract(lower(cexpr), FILE_EXTENSION_PATTERN, 0)  # '' if no match
            agg_exprs.append(spark_sum(when(non_empty, 1).otherwise(0)).alias(f"{c}__ne"))
            agg_exprs.append(spark_sum(when(length(file_like) > 0, 1).otherwise(0)).alias(f"{c}__fx"))
        elif ctype in ('double', 'float', 'decimal'):
            agg_exprs.append(spark_sum(when(cexpr.isNotNull() & ~isnan(cexpr), 1).otherwise(0)).alias(f"{c}__ne"))
        else:
            agg_exprs.append(spark_sum(when(cexpr.isNotNull(), 1).otherwise(0)).alias(f"{c}__ne"))

    stats = df.agg(*agg_exprs).collect()[0].asDict()

    valid, dropped = [], []
    for c in columns:
        ctype = dtypes[c]
        ne = int(stats.get(f"{c}__ne", 0))

        if c.lower() in force_drop_set:
            dropped.append(c); continue

        if ctype in ('string', 'varchar'):
            fx = int(stats.get(f"{c}__fx", 0))
            if ne > 0:
                pct = fx / float(ne)
                threshold = SUSPICIOUS_MIN_PCT if suspicious_by_name(c) else NORMAL_MIN_PCT
                if pct >= threshold:
                    dropped.append(c); continue

        valid.append(c)

    if dropped:
        print(f"🚫 {table_name}: dropping {len(dropped)} suspected file/path columns → {dropped[:10]}{'...' if len(dropped) > 10 else ''}")
    print(f"✅ {table_name}: keeping {len(valid)}/{len(columns)} columns (rows={total_rows})")
    return valid, dropped, total_rows

def process_single_table(table: str):
    t0 = time()
    try:
        dyf = glueContext.create_dynamic_frame.from_catalog(
            database=source_database,
            table_name=table,
            transformation_ctx=f"dyf_{table}"
        )
        df = dyf.toDF()

        valid_cols, dropped_cols, total_rows = decide_columns_to_keep(df, table)
        if not valid_cols:
            msg = f"⚠️ {table}: no valid columns (empty or all dropped). Skipping."
            print(msg); return msg

        # Drop columns entirely by selecting only the valid ones
        cleaned_df = df.select(*valid_cols)

        # Repartition only for large datasets
        if total_rows > 1_000_000:
            cleaned_df = cleaned_df.repartition(16)
        elif total_rows > 100_000:
            cleaned_df = cleaned_df.repartition(8)

        output_path = f"s3://{bucket}/{curated_prefix}{table}/"

        # IMPORTANT: do NOT pre-delete anymore; use overwrite to avoid emptying on failure
        (cleaned_df.write
            .format("parquet")
            .option("compression", "snappy")
            .mode("overwrite")
            .save(output_path))

        msg = (f"[OK] {table} → {output_path} in {time() - t0:.2f}s "
               f"(kept_cols={len(valid_cols)}, dropped_cols={len(dropped_cols)}, rows={total_rows})")
        print(msg); return msg

    except Exception as e:
        err = f"[ERROR] {table}: {str(e)}"
        print(err); traceback.print_exc()
        return err

def run_parallel(tables, max_workers=DEFAULT_MAX_WORKERS):
    max_workers = max(1, min(int(max_workers), len(tables)))
    print(f"🚀 Starting parallel processing with {max_workers} worker(s) over {len(tables)} table(s)")

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = {exe.submit(process_single_table, tbl): tbl for tbl in tables}
        completed = 0
        for fut in as_completed(futures):
            completed += 1
            try:
                res = fut.result()
                results.append(res)
                print(f"✅ [{completed}/{len(tables)}] {res}")
            except Exception as e:
                print(f"❌ [{completed}/{len(tables)}] Failed {futures[fut]}: {str(e)}")

    return results

# ===================== Orchestrate =====================
def main():
    job_start = time()
    all_tables = list_all_tables(source_database)
    target_tables = [t for t in all_tables if t.lower() not in exclude_set]

    print(f"Found {len(all_tables)} tables in '{source_database}'. "
          f"Excluding {len(exclude_set)} → processing {len(target_tables)}.")

    # Tune based on your Glue DPUs (3–6 is typical)
    workers = DEFAULT_MAX_WORKERS
    run_parallel(target_tables, max_workers=workers)

    print(f"🎉 All done in {time() - job_start:.2f}s")

if __name__ == "__main__":
    main()
    job.commit()
