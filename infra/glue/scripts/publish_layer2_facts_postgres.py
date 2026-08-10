# publish_layer2_facts_postgres.py
#
# Mirrors the fact_<x> tables built by build_layer2_facts.py from Iceberg to
# Postgres, full-replace per table, with PK + FK constraints (FKs reference
# dim_ tables already mirrored by publish_layer2_dims_postgres.py).
#
# Run manually after build_layer2_facts.py and publish_layer2_dims_postgres.py
# (FKs will just warn-and-skip if a referenced dim table/column isn't there
# yet, so job order matters but a partial run is not destructive).

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

# ========= FACT CONFIG (PK + FK definitions) =========
FACT_CONFIG: Dict[str, Dict[str, Any]] = {
    "fact_dg_window": {
        "pk": "dg_window_fact_id",
        "fks": [
            ("dg_window_sk",      "dim_lpd_dgwindow",          "lpd_dgwindow_sk"),
            ("financial_year_sk", "dim_src_financialyear",    "src_financialyear_sk"),
        ],
    },
    "fact_loi_status_history": {
        "pk": "loi_id",
        "fks": [
            ("loi_sk",            "dim_lpd_loi",            "lpd_loi_sk"),
            ("company_sk",        "dim_company_company",    "company_company_sk"),
            ("dg_window_sk",      "dim_lpd_dgwindow",       "lpd_dgwindow_sk"),
            ("created_date_id",   "dim_date",               "date_id"),
            ("submitted_date_id", "dim_date",               "date_id"),
        ],
    },
    "fact_intervention": {
        "pk": "intervention_fact_id",
        "fks": [
            ("intervention_created_date_id", "dim_date",                        "date_id"),
            ("loi_sk",                       "dim_lpd_loi",                     "lpd_loi_sk"),
            ("intervention_sk",              "dim_lpd_loiintervention",         "lpd_loiintervention_sk"),
            ("company_sk",                   "dim_company_company",             "company_company_sk"),
            ("indicator_sk",                 "dim_src_indicator",              "src_indicator_sk"),
            ("grant_type_sk",                "dim_src_descretionarygrant",     "src_descretionarygrant_sk"),
            ("subsector_sk",                 "dim_src_subsectoractivity",      "src_subsectoractivity_sk"),
            ("municipality_sk",              "dim_configurable_suburb",         "configurable_suburb_sk"),
        ],
    },
    "fact_vetting": {
        "pk": "vetting_fact_id",
        "fks": [
            ("vetting_sk",          "dim_lpd_loivetting",         "lpd_loivetting_sk"),
            ("vetting_approval_sk", "dim_lpd_loivettingapproval", "lpd_loivettingapproval_sk"),
            ("intervention_sk",     "dim_lpd_loiintervention",    "lpd_loiintervention_sk"),
            ("loi_sk",              "dim_lpd_loi",                "lpd_loi_sk"),
        ],
    },
    "fact_appeal": {
        "pk": "appeal_fact_id",
        "fks": [
            ("appeal_sk",       "dim_lpd_loiinterventionappeal", "lpd_loiinterventionappeal_sk"),
            ("intervention_no", "dim_lpd_loiintervention",       "lpd_loiintervention_sk"),
            ("loi_no",          "dim_lpd_loi",                   "lpd_loi_sk"),
            ("appeal_date",     "dim_date",                      "date_id"),
            ("appeal_status",   "dim_status",                    "status_id"),
        ],
    },
    "fact_learning_programme": {
        "pk": "learning_programme_fact_id",
        "fks": [
            ("sla_sk",                       "dim_lpd_learningprogrammesla",               "lpd_learningprogrammesla_sk"),
            ("learning_programme_sk",        "dim_lpd_learningprogramme",                  "lpd_learningprogramme_sk"),
            ("learningprogrammeaddendum_sk", "dim_lpd_learningprogrammeaddendum",          "lpd_learningprogrammeaddendum_sk"),
            ("commitmentregister_sk",        "dim_lpd_learningprogrammecommitmentregister", "lpd_learningprogrammecommitmentregister_sk"),
            ("writeback_sk",                 "dim_lpd_learningprogrammewriteback",         "lpd_learningprogrammewriteback_sk"),
            ("trainingprovider_sk",          "dim_lpd_learningprogrammetrainingprovider",  "lpd_learningprogrammetrainingprovider_sk"),
            ("slalearnerlistrequest_sk",     "dim_lpd_learningprogrammeslalearnerlistrequest", "lpd_learningprogrammeslalearnerlistrequest_sk"),
        ],
    },
    "fact_nonfunded_programme": {
        "pk": "nfp_id",
        "fks": [
            ("nf_sk", "dim_lpd_nonfundedprogramme",                 "lpd_nonfundedprogramme_sk"),
            ("nf_cc", "dim_lpd_nonfundedcompliancecheck",           "lpd_nonfundedcompliancecheck_sk"),
            ("nf_cl", "dim_lpd_nonfundedchecklist",                 "lpd_nonfundedchecklist_sk"),
            ("nf_tp", "dim_lpd_nonfundedprogrammetrainingprovider", "lpd_nonfundedprogrammetrainingprovider_sk"),
        ],
    },
    "fact_learningprogramme_disbursement": {
        "pk": "learningprogramme_disbursement_fact_id",
        "fks": [
            ("company_sk",               "dim_company_company",              "company_company_sk"),
            ("learningprogramme_sk",     "dim_lpd_learningprogramme",        "lpd_learningprogramme_sk"),
            ("sla_sk",                   "dim_lpd_learningprogrammesla",     "lpd_learningprogrammesla_sk"),
            ("dg_window_sk",             "dim_lpd_dgwindow",                 "lpd_dgwindow_sk"),
            ("disbursement_stage_sk",    "dim_src_discretionarygrantdisbursement", "src_discretionarygrantdisbursement_sk"),
            ("discretionarygrant_sk",    "dim_src_descretionarygrant",      "src_descretionarygrant_sk"),
            ("indicator_sk",             "dim_src_indicator",               "src_indicator_sk"),
            ("trade_payable_finyear_sk", "dim_src_financialyear",           "src_financialyear_sk"),
            ("payment_batch_sk",         "dim_finance_paymentbatchschedule", "finance_paymentbatchschedule_sk"),
            ("grants_admin_user_sk",     "dim_accounts_user",                "accounts_user_sk"),
            ("scheduled_date_sk",        "dim_date",                         "date_id"),
            ("approved_date_sk",         "dim_date",                         "date_id"),
        ],
    },
    "fact_learner": {
        "pk": "learner_fact_id",
        "fks": [
            ("contract_date_id",      "dim_date",                     "date_id"),
            ("placement_date_id",     "dim_date",                     "date_id"),
            ("completion_date_id",    "dim_date",                     "date_id"),
            ("employment_date_id",    "dim_date",                     "date_id"),
            ("learner_programme_sk",  "dim_lpd_learnerprogramme",     "lpd_learnerprogramme_sk"),
            ("learner_sk",            "dim_learner_learner",          "learner_learner_sk"),
            ("sla_sk",                "dim_lpd_learningprogrammesla", "lpd_learningprogrammesla_sk"),
            ("learning_programme_sk", "dim_lpd_learningprogramme",    "lpd_learningprogramme_sk"),
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

# ========= Main loop: per fact table full replace =========
total_rows = 0

for fact_name, cfg in FACT_CONFIG.items():
    pk_src = cfg["pk"]
    fk_defs = cfg.get("fks", [])

    try:
        df = spark.table(f"{ICE_CATALOG}.{ICE_DB}.{fact_name}")
    except Exception as e:
        print(f"[WARN] Fact table {fact_name} not found in {ICE_CATALOG}.{ICE_DB}: {e}")
        continue

    fields = df.schema.fields
    src_cols = [f.name for f in fields]
    spark_by_name = {f.name: f.dataType for f in fields}

    if pk_src not in src_cols:
        raise ValueError(f"[ERROR] PK column '{pk_src}' not found in fact {fact_name}")

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

    pk_col = safe_map[pk_src]

    pg_cols: List[Tuple[str, str]] = []
    cast_types: Dict[str, Any] = {}
    for s in src_cols:
        tgt = safe_map[s]
        dt = spark_by_name[s]
        pg_type = map_pg_type(dt)
        pg_cols.append((tgt, pg_type))
        cast_types[tgt] = map_spark_cast(dt)

    cols_sql_parts = []
    for tgt, pg_type in pg_cols:
        notnull = " NOT NULL" if tgt == pk_col else ""
        cols_sql_parts.append(f"{tgt} {pg_type}{notnull}")
    create_sql = (
        "CREATE TABLE IF NOT EXISTS " + fq + " (\n  " +
        ",\n  ".join(cols_sql_parts) + f",\n  PRIMARY KEY ({pk_col})\n)"
    )
    exec_sql(create_sql)

    selects = [
        F.col(s).cast(cast_types[safe_map[s]]).alias(safe_map[s])
        for s in src_cols
    ]
    proj = df.select(*selects)

    if _is_empty(proj):
        print(f"[INFO] Fact {fact_name} is empty; skipping load.")
        exec_sql(f"""
          INSERT INTO {CONTROL_TBL}(table_name, last_run_ts)
          VALUES ('{target_table}', now())
          ON CONFLICT (table_name) DO UPDATE SET last_run_ts = EXCLUDED.last_run_ts
        """)
        continue

    stage_core = pg_safe_table(target_table + "__stg")
    stage = f"{PG_SCHEMA}.{stage_core}"

    exec_sql(f"CREATE UNLOGGED TABLE IF NOT EXISTS {stage} (LIKE {fq} INCLUDING ALL)")
    exec_sql(f"TRUNCATE TABLE {stage}")

    tgt_cols_only = [c for (c, _) in pg_cols]
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

    cnt = query_df(f"SELECT COUNT(1) AS c FROM {stage}").collect()[0]["c"]

    exec_sql(f"TRUNCATE TABLE {fq}")
    exec_sql(f"""
      INSERT INTO {fq} ({col_list})
      SELECT {col_list} FROM {stage}
    """)
    exec_sql(f"DROP TABLE IF EXISTS {stage}")

    total_rows += cnt

    exec_sql(f"""
      INSERT INTO {CONTROL_TBL}(table_name, last_run_ts)
      VALUES ('{target_table}', now())
      ON CONFLICT (table_name) DO UPDATE SET last_run_ts = EXCLUDED.last_run_ts
    """)

    for fk_col_src, dim_name, dim_col_src in fk_defs:
        if fk_col_src not in src_cols:
            print(f"[WARN] FK column {fk_col_src} not present in fact {fact_name}, skipping FK.")
            continue

        fk_col = safe_map[fk_col_src]
        ref_table = pg_safe_table(dim_name)
        ref_col = pg_safe_column(dim_col_src)

        col_check = query_df(f"""
          SELECT 1 AS ok
          FROM information_schema.columns
          WHERE table_schema = '{PG_SCHEMA}'
            AND table_name   = '{ref_table}'
            AND column_name  = '{ref_col}'
          LIMIT 1
        """)
        if col_check.count() == 0:
            print(f"[WARN] Referenced dim {PG_SCHEMA}.{ref_table}({ref_col}) not found, skipping FK {fact_name}.{fk_col}.")
            continue

        cons_name = pg_safe_ident(
            f"{target_table}_{fk_col}_fk",
            extra_suffix="constraint",
            keyword_suffix="_c",
        )

        cons_check = query_df(f"""
          SELECT 1 AS ok
          FROM information_schema.table_constraints
          WHERE constraint_schema = '{PG_SCHEMA}'
            AND table_name        = '{target_table}'
            AND constraint_name   = '{cons_name}'
            AND constraint_type   = 'FOREIGN KEY'
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
            print(f"[INFO] Added FK {cons_name}: {fq}({fk_col}) -> {PG_SCHEMA}.{ref_table}({ref_col})")
        except Exception as e:
            print(f"[WARN] Failed to add FK {cons_name} on {fq}({fk_col}): {e}")

print(f"Fact mirror with PK/FK complete. Rows processed ~ {total_rows}")
job.commit()
