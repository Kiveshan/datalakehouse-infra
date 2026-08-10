# build_layer2_facts_ctas.py
#
# Builds fact_sdf_application and fact_wsp_submission directly via Iceberg
# CTAS (CREATE TABLE ... AS SELECT), unlike the other facts in
# build_layer2_facts.py which are Python-assembled DataFrames. Then mirrors
# those two plus fact_company to Postgres (full replace, PK only, no FKs).
#
# Run manually after build_layer2_facts.py (fact_company must already exist).
# Credentials come from Secrets Manager at runtime, never hardcoded — see
# infra/publish_postgres.tf.

import sys, json, hashlib, re
import boto3
from typing import Dict, Any, List, Tuple

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    ShortType,
    ByteType,
    FloatType,
    DoubleType,
    DecimalType,
    DateType,
    TimestampType,
    StringType,
    BooleanType,
    ArrayType,
)

# ========= Glue / Spark bootstrap =========
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'dims_database',
    'dims_catalog_name',
    'warehouse_path',
    'pg_host',
    'pg_port',
    'pg_database',
    'pg_schema',
    'pg_secret_arn',
])
ICE_CATALOG = args['dims_catalog_name']
ICE_DB = args['dims_database']

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext); job.init(args['JOB_NAME'], args)

spark.conf.set(f"spark.sql.catalog.{ICE_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{ICE_CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set(f"spark.sql.catalog.{ICE_CATALOG}.warehouse", args['warehouse_path'])
spark.conf.set(f"spark.sql.catalog.{ICE_CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.defaultCatalog", ICE_CATALOG)

PG_URL = f"jdbc:postgresql://{args['pg_host']}:{args['pg_port']}/{args['pg_database']}"
PG_SCHEMA = args['pg_schema']
jdbc_driver = "org.postgresql.Driver"

sm = boto3.client("secretsmanager")
_secret = json.loads(sm.get_secret_value(SecretId=args['pg_secret_arn'])["SecretString"])
JDBC_USER = _secret["username"]
JDBC_PASS = _secret["password"]

# ========= FACT CONFIG (PK only, no FKs -- matches the original) =========
FACT_CONFIG: Dict[str, Dict[str, Any]] = {
    "fact_company": {"pk": "company_fact_id", "fks": []},
    "fact_wsp_submission": {"pk": "wsp_submission_sk", "fks": []},
    "fact_sdf_application": {"pk": None, "fks": []},  # intentionally no PK
}

# ========= Recreate fact_sdf_application and fact_wsp_submission in Iceberg =========
spark.sql(f"DROP TABLE IF EXISTS {ICE_CATALOG}.{ICE_DB}.fact_sdf_application")
spark.sql(f"DROP TABLE IF EXISTS {ICE_CATALOG}.{ICE_DB}.fact_wsp_submission")

# LOCATION is intentionally omitted: the Iceberg GlueCatalog places new tables
# under spark.sql.catalog.<CAT>.warehouse (args['warehouse_path']) automatically.
ctas_sdf = f"""
CREATE TABLE {ICE_DB}.fact_sdf_application
USING iceberg
TBLPROPERTIES (
  'format-version'='2',
  'write.format.default'='parquet'
) AS
WITH current_submissions AS (
    SELECT id AS company_wsp_submission_id
    FROM {ICE_DB}.dim_ssp_companywspsubmission
    WHERE current_flag = TRUE
),
tracker_events AS (
    SELECT
        t.company_wsp_submission_id,
        t.sdf_id,
        t.status,
        t.created_at
    FROM {ICE_DB}.dim_ssp_sdfregistrationtracker t
    INNER JOIN current_submissions cs
      ON t.company_wsp_submission_id = cs.company_wsp_submission_id
    WHERE t.current_flag = TRUE
),
ranked_events AS (
    SELECT
        company_wsp_submission_id,
        sdf_id,
        status,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY company_wsp_submission_id, sdf_id
            ORDER BY created_at ASC
        ) AS first_seq,
        ROW_NUMBER() OVER (
            PARTITION BY company_wsp_submission_id, sdf_id
            ORDER BY created_at DESC
        ) AS latest_seq
    FROM tracker_events
),
first_event AS (
    SELECT
        company_wsp_submission_id,
        sdf_id,
        created_at AS submission_date
    FROM ranked_events
    WHERE first_seq = 1
),
approved_event AS (
    SELECT
        company_wsp_submission_id,
        sdf_id,
        MIN(created_at) AS approved_date
    FROM ranked_events
    WHERE status = 'Application Approved'
    GROUP BY company_wsp_submission_id, sdf_id
),
delinked_event AS (
    SELECT
        company_wsp_submission_id,
        sdf_id,
        MIN(created_at) AS delinked_date
    FROM ranked_events
    WHERE status = 'Application Delinked'
    GROUP BY company_wsp_submission_id, sdf_id
),
valid_applications AS (
    SELECT
        f.company_wsp_submission_id,
        f.sdf_id,
        f.submission_date,
        a.approved_date,
        d.delinked_date
    FROM first_event f
    LEFT JOIN approved_event a
      ON f.company_wsp_submission_id = a.company_wsp_submission_id
     AND f.sdf_id = a.sdf_id
    LEFT JOIN delinked_event d
      ON f.company_wsp_submission_id = d.company_wsp_submission_id
     AND f.sdf_id = d.sdf_id
)
SELECT
    va.company_wsp_submission_id                     AS deg_wsp_submission_id,
    fc.company_fact_id                               AS company_id,
    sdf.company_sdf_sk                               AS sdf_id,

    COALESCE(dd_sub.date_id, 0)                      AS submissiondate_id,
    COALESCE(dd_app.date_id, 0)                      AS approvaldate_id,
    COALESCE(dd_del.date_id, 0)                      AS delinkeddate_id,

    1                                                AS m_sdf_application_count,

    CASE
        WHEN va.approved_date IS NOT NULL
        THEN datediff(CAST(va.approved_date AS DATE), CAST(va.submission_date AS DATE))
        ELSE 0
    END                                              AS m_days_to_approval,

    CASE
        WHEN va.delinked_date IS NOT NULL AND va.approved_date IS NOT NULL
        THEN datediff(CAST(va.delinked_date AS DATE), CAST(va.approved_date AS DATE))
        ELSE 0
    END                                              AS m_days_to_delink

FROM valid_applications va
JOIN {ICE_DB}.dim_ssp_companywspsubmission sub
  ON va.company_wsp_submission_id = sub.id
  AND sub.current_flag = TRUE
JOIN {ICE_DB}.dim_company_company comp
  ON sub.company_id = comp.id
  AND comp.current_flag = TRUE
JOIN {ICE_DB}.fact_company fc
  ON comp.company_company_sk = fc.company_sk
LEFT JOIN {ICE_DB}.dim_company_sdf sdf
  ON va.sdf_id = sdf.id
  AND sdf.current_flag = TRUE
LEFT JOIN {ICE_DB}.dim_date dd_sub
  ON CAST(va.submission_date AS DATE) = dd_sub.date_actual
LEFT JOIN {ICE_DB}.dim_date dd_app
  ON CAST(va.approved_date AS DATE) = dd_app.date_actual
LEFT JOIN {ICE_DB}.dim_date dd_del
  ON CAST(va.delinked_date AS DATE) = dd_del.date_actual
"""
spark.sql(ctas_sdf)

ctas_wsp = f"""
CREATE TABLE {ICE_DB}.fact_wsp_submission
USING iceberg
TBLPROPERTIES (
  'format-version'='2',
  'write.format.default'='parquet'
) AS
WITH ranked_approved AS (
    SELECT
        company_wsp_submission_id,
        created_at AS approved_at,
        ROW_NUMBER() OVER (PARTITION BY company_wsp_submission_id ORDER BY created_at DESC) AS rn
    FROM {ICE_DB}.dim_ssp_wspsubmissionaudittracker
    WHERE status = 'Approved' AND current_flag = TRUE
),
latest_approved AS (
    SELECT company_wsp_submission_id, approved_at
    FROM ranked_approved WHERE rn = 1
),
ranked_closed AS (
    SELECT
        company_wsp_submission_id,
        created_at AS closed_at,
        ROW_NUMBER() OVER (PARTITION BY company_wsp_submission_id ORDER BY created_at DESC) AS rn
    FROM {ICE_DB}.dim_ssp_wspsubmissionaudittracker
    WHERE status = 'Closed' AND current_flag = TRUE
),
latest_closed AS (
    SELECT company_wsp_submission_id, closed_at
    FROM ranked_closed WHERE rn = 1
),
submission_with_audit AS (
    SELECT DISTINCT
        sub.ssp_companywspsubmission_sk,
        sub.id AS submission_natural_id,
        sub.company_id,
        sub.sdf_id,
        sub.wsp_period_id,
        sub.date_wsp_submitted,
        la.approved_at,
        lc.closed_at
    FROM {ICE_DB}.dim_ssp_companywspsubmission sub
    INNER JOIN {ICE_DB}.dim_ssp_wspsubmissionaudittracker audit
      ON sub.id = audit.company_wsp_submission_id
      AND audit.current_flag = TRUE
    LEFT JOIN latest_approved la ON sub.id = la.company_wsp_submission_id
    LEFT JOIN latest_closed lc ON sub.id = lc.company_wsp_submission_id
    WHERE sub.current_flag = TRUE
)
SELECT
    s.ssp_companywspsubmission_sk                    AS wsp_submission_sk,
    s.submission_natural_id,
    fc.company_fact_id                               AS company_fact_id,
    sdf.company_sdf_sk                               AS company_sdf_sk,
    per.ssp_wspperiod_sk                             AS wsp_period_sk,
    COALESCE(dd_submitted.date_id, 0)                AS submitted_date_id,
    COALESCE(dd_approved.date_id, 0)                 AS approved_date_id,
    COALESCE(dd_closed.date_id, 0)                   AS closed_date_id,
    CASE
        WHEN s.approved_at IS NOT NULL
        THEN datediff(CAST(s.approved_at AS DATE), CAST(s.date_wsp_submitted AS DATE))
        ELSE 0
    END                                              AS approval_days
FROM submission_with_audit s
LEFT JOIN {ICE_DB}.dim_company_company comp
  ON s.company_id = comp.id
  AND comp.current_flag = TRUE
LEFT JOIN {ICE_DB}.fact_company fc
  ON comp.company_company_sk = fc.company_sk
LEFT JOIN {ICE_DB}.dim_company_sdf sdf
  ON s.sdf_id = sdf.id
  AND sdf.current_flag = TRUE
LEFT JOIN {ICE_DB}.dim_ssp_wspperiod per
  ON s.wsp_period_id = per.id
  AND per.current_flag = TRUE
LEFT JOIN {ICE_DB}.dim_date dd_submitted
  ON CAST(s.date_wsp_submitted AS DATE) = dd_submitted.date_actual
LEFT JOIN {ICE_DB}.dim_date dd_approved
  ON CAST(s.approved_at AS DATE) = dd_approved.date_actual
LEFT JOIN {ICE_DB}.dim_date dd_closed
  ON CAST(s.closed_at AS DATE) = dd_closed.date_actual
"""
spark.sql(ctas_wsp)

# ========= Identifier-safety helpers (Postgres) =========
PG_MAX_IDENT = 63
_invalid = re.compile(r"[^a-z0-9_]+")

PG_RESERVED = {
    "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "authorization",
    "binary", "both", "case", "cast", "check", "collate", "collation", "column", "concurrently",
    "constraint", "create", "cross", "current_catalog", "current_date", "current_role",
    "current_schema", "current_time", "current_timestamp", "current_user", "default", "deferrable",
    "desc", "distinct", "do", "else", "end", "except", "false", "fetch", "for", "foreign", "freeze",
    "from", "full", "grant", "group", "having", "ilike", "in", "initially", "inner", "intersect",
    "into", "is", "isnull", "join", "lateral", "leading", "left", "like", "limit", "localtime",
    "localtimestamp", "natural", "not", "notnull", "null", "offset", "on", "only", "or", "order",
    "outer", "overlaps", "placing", "primary", "references", "returning", "right", "select",
    "session_user", "similar", "some", "symmetric", "table", "tablesample", "then", "to", "trailing",
    "true", "union", "unique", "user", "using", "variadic", "verbose", "when", "where", "window", "with",
}


def _sanitize_base(name: str) -> str:
    if name is None:
        name = ""
    s = name.strip().lower()
    s = _invalid.sub("_", s)
    s = re.sub(r"__+", "_", s)
    if not re.match(r"^[a-z_]", s):
        s = "_" + s
    return s


def pg_safe_ident(name: str, extra_suffix: str, keyword_suffix: str) -> str:
    base = _sanitize_base(name)
    if base in PG_RESERVED:
        base = base + keyword_suffix
    if len(base) <= PG_MAX_IDENT:
        return base
    short = base[:50]
    h = hashlib.sha1((name + "|" + extra_suffix).encode("utf-8")).hexdigest()[:7]
    return f"{short}_{h}"


def pg_safe_table(base_name: str) -> str:
    return pg_safe_ident(base_name, extra_suffix="table", keyword_suffix="_t")


def pg_safe_column(base_name: str) -> str:
    return pg_safe_ident(base_name, extra_suffix="column", keyword_suffix="_col")


def exec_sql(sql: str):
    sql_clean = sql.strip().rstrip(";")
    (
        spark.read
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "(select 1) as t")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASS)
        .option("driver", jdbc_driver)
        .option("sessionInitStatement", sql_clean)
        .load()
        .count()
    )


def query_df(sql: str):
    sql_clean = sql.strip().rstrip(";")
    return (
        spark.read
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", f"({sql_clean}) as q")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASS)
        .option("driver", jdbc_driver)
        .load()
    )


def map_pg_type(dtype):
    if isinstance(dtype, ArrayType):
        elem = map_pg_type(dtype.elementType)
        return f"{elem}[]"
    if isinstance(dtype, LongType):       return "BIGINT"
    if isinstance(dtype, IntegerType):    return "INTEGER"
    if isinstance(dtype, ShortType):      return "SMALLINT"
    if isinstance(dtype, ByteType):       return "SMALLINT"
    if isinstance(dtype, BooleanType):    return "BOOLEAN"
    if isinstance(dtype, DoubleType):     return "DOUBLE PRECISION"
    if isinstance(dtype, FloatType):      return "REAL"
    if isinstance(dtype, DecimalType):    return f"DECIMAL({dtype.precision},{dtype.scale})"
    if isinstance(dtype, DateType):       return "DATE"
    if isinstance(dtype, TimestampType):  return "TIMESTAMPTZ"
    return "TEXT"


def map_spark_cast(dtype):
    if isinstance(dtype, ArrayType):
        return dtype
    if isinstance(dtype, LongType):       return LongType()
    if isinstance(dtype, IntegerType):    return IntegerType()
    if isinstance(dtype, ShortType):      return ShortType()
    if isinstance(dtype, ByteType):       return ShortType()
    if isinstance(dtype, BooleanType):    return BooleanType()
    if isinstance(dtype, DoubleType):     return DoubleType()
    if isinstance(dtype, FloatType):      return FloatType()
    if isinstance(dtype, DecimalType):    return DecimalType(dtype.precision, dtype.scale)
    if isinstance(dtype, DateType):       return DateType()
    if isinstance(dtype, TimestampType):  return TimestampType()
    return StringType()


def _is_empty(df) -> bool:
    return df.rdd.isEmpty()


# ========= Control table =========
CONTROL_TBL = f"{PG_SCHEMA}._mirror_control_facts"
exec_sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROL_TBL}(
  table_name TEXT PRIMARY KEY,
  last_run_ts TIMESTAMPTZ DEFAULT now()
)
""")

# ========= Main loop: per fact table full replace =========
total_rows = 0
JDBC_BATCH_SIZE = 50000
TARGET_WRITE_PARTITIONS = 4

for fact_name, cfg in FACT_CONFIG.items():
    pk_src = cfg.get("pk")
    fk_defs = cfg.get("fks", [])

    try:
        df = spark.table(f"{ICE_CATALOG}.{ICE_DB}.{fact_name}")
    except Exception as e:
        print(f"[WARN] Fact table {fact_name} not found: {e}")
        continue

    fields = df.schema.fields
    src_cols = [f.name for f in fields]
    spark_by_name = {f.name: f.dataType for f in fields}

    if pk_src is not None and pk_src not in src_cols:
        raise ValueError(f"[ERROR] PK column '{pk_src}' not found in {fact_name}")

    safe_map: Dict[str, str] = {}
    seen_targets = set()
    for s in src_cols:
        candidate = pg_safe_column(s)
        if candidate in seen_targets:
            candidate = pg_safe_column(f"{candidate}_dup")
        seen_targets.add(candidate)
        safe_map[s] = candidate

    target_table = pg_safe_table(fact_name)
    fq = f"{PG_SCHEMA}.{target_table}"

    pk_col = safe_map.get(pk_src) if pk_src else None

    pg_cols: List[Tuple[str, str]] = []
    cast_types: Dict[str, Any] = {}
    for s in src_cols:
        tgt = safe_map[s]
        dt = spark_by_name[s]
        pg_type = map_pg_type(dt)
        pg_cols.append((tgt, pg_type))
        cast_types[tgt] = map_spark_cast(dt)

    cols_sql_parts = [f"{tgt} {pg_type}{' NOT NULL' if pk_col == tgt else ''}" for tgt, pg_type in pg_cols]
    if pk_col is not None:
        create_sql = "CREATE TABLE IF NOT EXISTS " + fq + " (\n  " + ",\n  ".join(cols_sql_parts) + f",\n  PRIMARY KEY ({pk_col})\n)"
    else:
        create_sql = "CREATE TABLE IF NOT EXISTS " + fq + " (\n  " + ",\n  ".join(cols_sql_parts) + "\n)"
    exec_sql(create_sql)

    if pk_col is None:
        pk_check = query_df(f"""
          SELECT constraint_name
          FROM information_schema.table_constraints
          WHERE constraint_schema = '{PG_SCHEMA}'
            AND table_name = '{target_table}'
            AND constraint_type = 'PRIMARY KEY'
          LIMIT 1
        """)
        if pk_check.count() > 0:
            existing_pk = pk_check.collect()[0]["constraint_name"]
            try:
                exec_sql(f"ALTER TABLE {fq} DROP CONSTRAINT {existing_pk}")
                print(f"[INFO] Dropped existing PK {existing_pk} on {fq}")
            except Exception as e:
                print(f"[WARN] Failed to drop PK: {e}")

    selects = [F.col(s).cast(cast_types[safe_map[s]]).alias(safe_map[s]) for s in src_cols]
    proj = df.select(*selects)

    if _is_empty(proj):
        print(f"[INFO] {fact_name} is empty, skipping.")
        exec_sql(f"""
          INSERT INTO {CONTROL_TBL}(table_name, last_run_ts)
          VALUES ('{target_table}', now())
          ON CONFLICT (table_name) DO UPDATE SET last_run_ts = EXCLUDED.last_run_ts
        """)
        continue

    stage_core = pg_safe_table(target_table + "__stg")
    stage = f"{PG_SCHEMA}.{stage_core}"
    exec_sql(f"DROP TABLE IF EXISTS {stage}")
    exec_sql(f"CREATE UNLOGGED TABLE {stage} (LIKE {fq} INCLUDING DEFAULTS EXCLUDING CONSTRAINTS EXCLUDING INDEXES)")
    exec_sql(f"TRUNCATE TABLE {stage}")

    tgt_cols_only = [c for c, _ in pg_cols]
    col_list = ", ".join(tgt_cols_only)

    parts = max(1, min(TARGET_WRITE_PARTITIONS, proj.rdd.getNumPartitions()))
    (
        proj.coalesce(parts)
        .write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", stage)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASS)
        .option("driver", jdbc_driver)
        .option("batchsize", str(JDBC_BATCH_SIZE))
        .mode("append")
        .save()
    )

    cnt = query_df(f"SELECT COUNT(1) FROM {stage}").collect()[0][0]

    exec_sql(f"TRUNCATE TABLE {fq}")
    exec_sql(f"INSERT INTO {fq} ({col_list}) SELECT {col_list} FROM {stage}")
    exec_sql(f"DROP TABLE {stage}")

    total_rows += cnt

    exec_sql(f"""
      INSERT INTO {CONTROL_TBL}(table_name, last_run_ts)
      VALUES ('{target_table}', now())
      ON CONFLICT (table_name) DO UPDATE SET last_run_ts = EXCLUDED.last_run_ts
    """)

    for fk_col_src, dim_name, dim_col_src in fk_defs:
        if fk_col_src not in src_cols:
            print(f"[WARN] FK col {fk_col_src} missing in {fact_name}")
            continue

        fk_col = safe_map[fk_col_src]
        ref_table = pg_safe_table(dim_name)
        ref_col = pg_safe_column(dim_col_src)

        col_check = query_df(f"""
          SELECT 1 FROM information_schema.columns
          WHERE table_schema = '{PG_SCHEMA}'
            AND table_name = '{ref_table}'
            AND column_name = '{ref_col}'
          LIMIT 1
        """)
        if col_check.count() == 0:
            print(f"[WARN] Ref {PG_SCHEMA}.{ref_table}({ref_col}) not found")
            continue

        cons_name = pg_safe_ident(f"{target_table}_{fk_col}_fk", "constraint", "_c")
        cons_check = query_df(f"""
          SELECT 1 FROM information_schema.table_constraints
          WHERE constraint_schema = '{PG_SCHEMA}'
            AND table_name = '{target_table}'
            AND constraint_name = '{cons_name}'
            AND constraint_type = 'FOREIGN KEY'
          LIMIT 1
        """)
        if cons_check.count() > 0:
            continue

        fk_sql = f"""
          ALTER TABLE {fq}
          ADD CONSTRAINT {cons_name}
          FOREIGN KEY ({fk_col})
          REFERENCES {PG_SCHEMA}.{ref_table}({ref_col})
        """
        try:
            exec_sql(fk_sql)
            print(f"[INFO] Added FK: {fq}({fk_col}) -> {PG_SCHEMA}.{ref_table}({ref_col})")
        except Exception as e:
            print(f"[WARN] FK failed: {e}")

print(f"Fact mirror complete. Rows loaded: ~{total_rows}")
job.commit()
