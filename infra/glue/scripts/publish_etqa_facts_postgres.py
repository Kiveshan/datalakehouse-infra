# publish_etqa_facts_postgres.py
#
# Mirrors fact_certifylearners and fact_assessor_moderator from Iceberg to
# Postgres with FK constraints. These two facts are NOT built by this
# pipeline -- they were built manually, directly in the Iceberg warehouse,
# outside of Terraform/Glue. This job only publishes whatever is already
# there; if either table doesn't exist yet it is skipped with a warning.
#
# Run manually after publish_layer2_dims_postgres.py (FKs reference mirrored
# dim_ tables).

import sys, json, re, hashlib
import boto3
from typing import Dict, Any, List, Tuple

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
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

JDBC_BATCH_SIZE = 50000
TARGET_WRITE_PARTITIONS = 4

sm = boto3.client("secretsmanager")
_secret = json.loads(sm.get_secret_value(SecretId=args['pg_secret_arn'])["SecretString"])
JDBC_USER = _secret["username"]
JDBC_PASS = _secret["password"]

# ========= FACT CONFIG =========
FACT_CONFIG: Dict[str, Dict[str, Any]] = {
    "fact_certifylearners": {
        "pk": None,  # no natural PK
        "fks": [
            ("learner_id",                       "dim_learner_learner",                "learner_learner_sk"),
            ("etqa_trainingprogrammelearner_id", "dim_etqa_trainingprogrammelearner",  "etqa_trainingprogrammelearner_sk"),
            ("disability_id",                    "dim_configurable_disability",        "configurable_disability_sk"),
            ("gender_id",                        "dim_configurable_gender",            "configurable_gender_sk"),
            ("race_id",                          "dim_configurable_race",              "configurable_race_sk"),
            ("training_provider_programme_id",   "dim_etqa_trainingproviderprogramme", "etqa_trainingproviderprogramme_sk"),
            ("date_learer_entered_id",           "dim_date",                           "date_id"),
            ("date_programme_start_id",          "dim_date",                           "date_id"),
            ("certification_date_id",            "dim_date",                           "date_id"),
            ("training_programme_start_date_id", "dim_date",                           "date_id"),
            ("training_programme_end_date_id",   "dim_date",                           "date_id"),
        ],
    },
    "fact_assessor_moderator": {
        "pk": None,  # no natural PK
        "fks": [
            ("assessor_moderator_id",            "dim_etqa_assessormoderator",                        "etqa_assessormoderator_sk"),
            ("disability_id",                    "dim_configurable_disability",                       "configurable_disability_sk"),
            ("gender_id",                        "dim_configurable_gender",                           "configurable_gender_sk"),
            ("race_id",                          "dim_configurable_race",                             "configurable_race_sk"),
            ("training_provider_id",             "dim_company_trainingprovider",                      "company_trainingprovider_sk"),
            ("application_id",                   "dim_etqa_assessormoderatorapplication",             "etqa_assessormoderatorapplication_sk"),
            ("training_provider_application_id", "dim_etqa_trainingproviderapplicationassessor",      "etqa_trainingproviderapplicationassessor_sk"),
            ("unit_standard_application_id",     "dim_etqa_assessormoderatorunitstandardapplication", "etqa_assessormoderatorunitstandardapplication_sk"),
            ("unit_standard_id",                 "dim_src_unitstandard",                             "src_unitstandard_sk"),
            ("assessor_approve_date_id",         "dim_date",                                          "date_id"),
            ("assessor_end_date_id",              "dim_date",                                         "date_id"),
            ("moderator_approve_date_id",        "dim_date",                                          "date_id"),
            ("moderator_end_date_id",            "dim_date",                                          "date_id"),
            ("application_created_at_id",        "dim_date",                                          "date_id"),
        ],
    },
}

# ========= Identifier-safety helpers =========
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

# ========= Main loop =========
total_rows = 0

for fact_name, cfg in FACT_CONFIG.items():
    pk_src = cfg.get("pk")
    fk_defs = cfg.get("fks", [])

    try:
        df = spark.table(f"{ICE_CATALOG}.{ICE_DB}.{fact_name}")
    except Exception as e:
        print(f"[WARN] Fact table {fact_name} not found (built manually -- has it been created yet?): {e}")
        continue

    fields = df.schema.fields
    src_cols = [f.name for f in fields]
    spark_by_name = {f.name: f.dataType for f in fields}

    if pk_src is not None and pk_src not in src_cols:
        raise ValueError(f"[ERROR] PK column '{pk_src}' not in {fact_name}")

    safe_map: Dict[str, str] = {}
    seen_targets = set()
    for s in src_cols:
        candidate = pg_safe_column(s)
        i = 1
        orig = candidate
        while candidate in seen_targets:
            candidate = f"{orig}_{i}"
            i += 1
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
    create_sql = f"CREATE TABLE IF NOT EXISTS {fq} (\n  " + ",\n  ".join(cols_sql_parts)
    if pk_col:
        create_sql += f",\n  PRIMARY KEY ({pk_col})\n)"
    else:
        create_sql += "\n)"
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

print(f"ETQA fact mirror complete. Rows loaded: ~{total_rows}")
job.commit()
