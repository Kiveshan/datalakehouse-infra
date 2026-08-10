# publish_layer2_dims_postgres.py
#
# Autodiscovers every dim_<table> Iceberg table in the layer2 database and
# mirrors it incrementally into Postgres (upsert on PK, watermarked by
# surrogate key / effective_from so re-runs only ship new/changed rows).
# Skips fact_/bridge_ prefixed tables -- those are published separately
# (publish_layer2_facts_postgres.py, build_layer2_facts_ctas.py).
#
# Identifier safety: <=63 chars, valid chars, avoid reserved keywords
# (append _col/_t), stable hash suffix on truncate. Uses UNLOGGED staging
# tables and drops them after each upsert.
#
# Run manually after build_layer2_dims_scd2.py. Credentials come from
# Secrets Manager at runtime -- see infra/publish_postgres.tf.

import sys, json, re, hashlib
import boto3
from typing import Dict, Any, List, Tuple
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

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

sm = boto3.client('secretsmanager')
_secret = json.loads(sm.get_secret_value(SecretId=args['pg_secret_arn'])['SecretString'])
JDBC_USER = _secret['username']
JDBC_PASS = _secret['password']

# === Embedded overrides -- edit here if a table needs a specific PK/cast/rename ===
OVERRIDES = {
    "exclude": [],
    "pk": {
        "dim_date": "date_id",
        "dim_status": "status_id",
    },
    "casts": {},
    "rename": {},
}

# ========= Helpers =========
PG_MAX_IDENT = 63
_invalid = re.compile(r'[^a-z0-9_]+')

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
    s = re.sub(r'__+', "_", s)
    if not re.match(r'^[a-z_]', s):
        s = "_" + s
    return s


def pg_safe_ident(name: str, extra_suffix: str, keyword_suffix: str) -> str:
    base = _sanitize_base(name)
    if base in PG_RESERVED:
        base = base + keyword_suffix
    if len(base) <= PG_MAX_IDENT:
        return base
    short = base[:50]
    h = hashlib.sha1((name + '|' + extra_suffix).encode("utf-8")).hexdigest()[:7]
    return f"{short}_{h}"


def pg_safe_table(base_name: str) -> str:
    return pg_safe_ident(base_name, extra_suffix="table", keyword_suffix="_t")


def pg_safe_column(base_name: str) -> str:
    return pg_safe_ident(base_name, extra_suffix="column", keyword_suffix="_col")


def exec_sql(sql: str):
    (spark.read
         .format('jdbc')
         .option('url', PG_URL)
         .option('dbtable', '(select 1) as t')
         .option('user', JDBC_USER)
         .option('password', JDBC_PASS)
         .option('driver', jdbc_driver)
         .option('sessionInitStatement', sql)
         .load()
         .count())


def query_df(sql: str):
    return (spark.read
         .format('jdbc')
         .option('url', PG_URL)
         .option('dbtable', f'({sql}) as q')
         .option('user', JDBC_USER)
         .option('password', JDBC_PASS)
         .option('driver', jdbc_driver)
         .load())


def map_pg_type(dtype):
    if isinstance(dtype, LongType):       return 'BIGINT'
    if isinstance(dtype, IntegerType):    return 'INTEGER'
    if isinstance(dtype, ShortType):      return 'SMALLINT'
    if isinstance(dtype, BooleanType):    return 'BOOLEAN'
    if isinstance(dtype, DoubleType):     return 'DOUBLE PRECISION'
    if isinstance(dtype, FloatType):      return 'REAL'
    if isinstance(dtype, DecimalType):    return f'DECIMAL({dtype.precision},{dtype.scale})'
    if isinstance(dtype, DateType):       return 'DATE'
    if isinstance(dtype, TimestampType):  return 'TIMESTAMPTZ'
    return 'TEXT'


def map_spark_cast(dtype):
    if isinstance(dtype, LongType):       return LongType()
    if isinstance(dtype, IntegerType):    return IntegerType()
    if isinstance(dtype, ShortType):      return ShortType()
    if isinstance(dtype, BooleanType):    return BooleanType()
    if isinstance(dtype, DoubleType):     return DoubleType()
    if isinstance(dtype, FloatType):      return FloatType()
    if isinstance(dtype, DecimalType):    return DecimalType(dtype.precision, dtype.scale)
    if isinstance(dtype, DateType):       return DateType()
    if isinstance(dtype, TimestampType):  return TimestampType()
    return StringType()


def infer_pk_from_list(columns: List[str]) -> str:
    for c in columns:
        if c.endswith('_sk'):
            return c
    for c in columns:
        if c.endswith('_id'):
            return c
    return columns[0] if columns else None


def glue_table_params(db: str, name: str) -> Dict[str, str]:
    try:
        glue = boto3.client('glue')
        res = glue.get_table(DatabaseName=db, Name=name)
        return (res.get('Table', {}).get('Parameters', {}) or {})
    except Exception:
        return {}


# ========= Bootstrap target schema + control =========
exec_sql(f'CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};')
CONTROL_TBL = f'{PG_SCHEMA}._mirror_control'
exec_sql(f'''
CREATE TABLE IF NOT EXISTS {PG_SCHEMA}._mirror_control(
  table_name TEXT PRIMARY KEY,
  last_max_sk BIGINT,
  last_max_effective_from TIMESTAMPTZ,
  last_run_ts TIMESTAMPTZ DEFAULT now()
);
''')

rows = spark.sql(f'SHOW TABLES IN {ICE_CATALOG}.{ICE_DB}').collect()
tables = [r.tableName for r in rows]

total_rows = 0

for t in tables:
    if t.startswith('fact_') or t.startswith('bridge_'):
        continue

    ov_exclude = set(OVERRIDES.get('exclude', []))
    tbl_over_pk = OVERRIDES.get('pk', {}).get(t)
    tbl_over_cast = OVERRIDES.get('casts', {}).get(t, {})
    tbl_over_ren = OVERRIDES.get('rename', {}).get(t, {})
    params = glue_table_params(ICE_DB, t)
    if params.get('mirror.exclude', 'false').lower() == 'true' or t in ov_exclude:
        continue

    for k, v in params.items():
        if k.startswith('mirror.cast.'):
            col = k.split('.', 2)[2]
            tbl_over_cast[col] = v
        if k.startswith('mirror.rename.'):
            col = k.split('.', 2)[2]
            tbl_over_ren[col] = v
    pk_hint = params.get('mirror.pk') or tbl_over_pk

    df = spark.table(f'{ICE_CATALOG}.{ICE_DB}.{t}')
    fields = df.schema.fields

    src_cols = [f.name for f in fields]
    human_map = {f.name: tbl_over_ren.get(f.name, f.name) for f in fields}

    tgt_names: List[str] = []
    safe_map: Dict[str, str] = {}
    seen_targets = set()
    for s in src_cols:
        candidate = pg_safe_column(human_map[s])
        if candidate in seen_targets:
            candidate = pg_safe_column(f"{candidate}_dup")
        seen_targets.add(candidate)
        safe_map[s] = candidate
        tgt_names.append(candidate)

    target_table = pg_safe_table(t)
    fq = f'{PG_SCHEMA}.{target_table}'

    pk = None
    if pk_hint and pk_hint in src_cols:
        pk = safe_map[pk_hint]
    elif pk_hint and pk_hint in tgt_names:
        pk = pk_hint
    else:
        inferred_src = infer_pk_from_list(src_cols)
        if inferred_src and inferred_src in safe_map:
            pk = safe_map[inferred_src]
    if not pk or pk not in tgt_names:
        pk = tgt_names[0]
        print(f"[WARN] Table {t}: PK could not be resolved from hint '{pk_hint}'. Using '{pk}'.")

    pg_cols: List[Tuple[str, str]] = []
    cast_types: Dict[str, DataType] = {}
    spark_by_name = {f.name: f.dataType for f in fields}
    for s in src_cols:
        target_col = safe_map[s]
        base_dt = spark_by_name[s]
        override_pg = tbl_over_cast.get(target_col)
        pg_type = override_pg or map_pg_type(base_dt)
        pg_cols.append((target_col, pg_type))
        cast_types[target_col] = map_spark_cast(base_dt)

    cols_sql = []
    for tgt, pg_type in pg_cols:
        notnull = ' NOT NULL' if tgt == pk else ''
        cols_sql.append(f'{tgt} {pg_type}{notnull}')
    create_sql = 'CREATE TABLE IF NOT EXISTS ' + fq + ' (\n  ' + ',\n  '.join(cols_sql) + f',\n  PRIMARY KEY ({pk})\n);'
    exec_sql(create_sql)

    has_eff_from = 'effective_from' in tgt_names
    ctrl = query_df(f"SELECT last_max_sk, last_max_effective_from FROM {CONTROL_TBL} WHERE table_name = '{target_table}'")
    last_sk = None
    last_eff = None
    if ctrl.count() > 0:
        r = ctrl.collect()[0]
        last_sk = r['last_max_sk']
        last_eff = r['last_max_effective_from']

    selects = [F.col(s).cast(cast_types[safe_map[s]]).alias(safe_map[s]) for s in src_cols]
    proj = df.select(*selects)

    pred = None
    if pk in proj.columns and last_sk is not None:
        pred = F.col(pk) > F.lit(last_sk)
    if has_eff_from and last_eff is not None:
        eff_pred = F.col('effective_from') > F.lit(last_eff)
        pred = eff_pred if pred is None else (pred | eff_pred)
    if pred is not None:
        proj = proj.where(pred)

    if proj.rdd.isEmpty():
        exec_sql(f"""
          INSERT INTO {CONTROL_TBL}(table_name, last_run_ts)
          VALUES ('{target_table}', now())
          ON CONFLICT (table_name) DO UPDATE SET last_run_ts = EXCLUDED.last_run_ts;
        """)
        continue

    stage_core = pg_safe_table(target_table + "__stg")
    stage = f"{PG_SCHEMA}.{stage_core}"
    tgt_cols_only = [c for c, _ in pg_cols]
    col_list = ", ".join(tgt_cols_only)

    exec_sql(f"CREATE UNLOGGED TABLE IF NOT EXISTS {stage} (LIKE {fq} INCLUDING ALL);")
    exec_sql(f"TRUNCATE TABLE {stage};")

    parts = max(1, min(TARGET_WRITE_PARTITIONS, proj.rdd.getNumPartitions()))
    (proj.coalesce(parts)
         .write
         .format('jdbc')
         .option('url', PG_URL)
         .option('dbtable', stage)
         .option('user', JDBC_USER)
         .option('password', JDBC_PASS)
         .option('driver', jdbc_driver)
         .option('batchsize', str(JDBC_BATCH_SIZE))
         .mode('append')
         .save())

    set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in tgt_cols_only if c != pk])
    exec_sql(f"""
      INSERT INTO {fq} ({col_list})
      SELECT {col_list} FROM {stage}
      ON CONFLICT ({pk}) DO UPDATE SET {set_clause};
    """)

    cnt = query_df(f"SELECT COUNT(1) AS c FROM {stage}").collect()[0]["c"]
    exec_sql(f"DROP TABLE IF EXISTS {stage};")
    total_rows += cnt

    sets = ['last_run_ts = now()']
    if pk in proj.columns:
        new_last_sk = proj.agg(F.max(F.col(pk))).collect()[0][0]
        if new_last_sk is not None:
            sets.append(f'last_max_sk = {int(new_last_sk)}')
    if has_eff_from:
        new_last_eff = proj.agg(F.max(F.col('effective_from'))).collect()[0][0]
        if new_last_eff is not None:
            sets.append(f"last_max_effective_from = '{str(new_last_eff)}'")
    exec_sql(f"""
      INSERT INTO {CONTROL_TBL}(table_name, last_max_sk, last_max_effective_from, last_run_ts)
      VALUES ('{target_table}', NULL, NULL, now())
      ON CONFLICT (table_name) DO UPDATE SET {', '.join(sets)};
    """)

print(f'Incremental autodiscovery mirror complete. Rows processed ~ {total_rows}')
job.commit()
