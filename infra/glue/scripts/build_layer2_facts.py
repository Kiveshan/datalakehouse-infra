# build_layer2_facts.py
#
# Builds the Kimball fact_<x> Iceberg tables (LPD star schema) from the
# dim_<table> tables built by create_layer2_dims.py / kept current by
# build_layer2_dims_scd2.py. Joins dims together, resolves surrogate keys,
# and maps dates/statuses via dim_date / dim_status.
#
# Run manually after build_layer2_dims_scd2.py, same manual-trigger pattern as
# the rest of this pipeline. Most facts are full-replace on every run
# (T() reads only current_flag = true rows from each dim); fact_learner is
# versioned (SCD2-style, via versioned_upsert); fact_learningprogramme_disbursement
# is insert-only with a self-healing surrogate PK.

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import DataFrame
from typing import List, Optional, Union, Sequence
from functools import reduce
from pyspark.sql.types import (
    IntegerType,
    LongType,
    ShortType,
    ByteType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    BooleanType,
    ArrayType,
)

# -----------------------------------
# Glue / Spark bootstrap
# -----------------------------------
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'dims_database',
    'dims_catalog_name',
    'warehouse_path',
])
CAT = args['dims_catalog_name']
DB = args['dims_database']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sc.setLogLevel("WARN")
job = Job(glueContext); job.init(args['JOB_NAME'], args)

spark.conf.set(f"spark.sql.catalog.{CAT}", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{CAT}.warehouse", args['warehouse_path'])
spark.conf.set(f"spark.sql.catalog.{CAT}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set(f"spark.sql.catalog.{CAT}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.{DB}")

# -----------------------------------
# Helpers
# -----------------------------------
FACT_PREFIX = "fact_"
VERBOSE = False


def _assert_fact_table(name: str):
    if not name.startswith(FACT_PREFIX):
        raise ValueError(f"Refusing to write to non-fact table: {name}")


def T(name: str) -> DataFrame:
    df = spark.table(f"{CAT}.{DB}.{name}")
    if name in ("dim_date", "dim_status"):
        return df
    return df.filter("current_flag = true") if "current_flag" in df.columns else df


def to_date_id(df: DataFrame, src_col: str, alias: str) -> DataFrame:
    if src_col not in df.columns:
        return df.withColumn(alias, F.lit(0).cast("int"))
    dd = T("dim_date").select(
        F.col("date_actual").alias("__d"),
        F.col("date_id").cast("int").alias("__id"),
    )
    df = df.withColumn(f"__{alias}_d", F.to_date(F.col(src_col)))
    df = (
        df.join(dd, F.col(f"__{alias}_d") == dd["__d"], "left")
        .drop("__d")
        .withColumn(alias, F.coalesce(F.col("__id"), F.lit(0)).cast("int"))
        .drop("__id", f"__{alias}_d")
    )
    return df


def map_status(df: DataFrame, src_col: str, out_alias: str, domain: str) -> DataFrame:
    if src_col not in df.columns:
        return df.withColumn(out_alias, F.lit(0).cast("bigint"))
    s = (
        df.withColumn("__sd", F.lit(domain))
        .withColumn("__sn", F.lower(F.trim(F.col(src_col).cast("string"))))
    )
    dim = T("dim_status").select(
        F.col("status_id").cast("bigint"),
        F.col("status_domain").alias("__d"),
        F.col("status_nk").alias("__n"),
        "current_flag",
    )
    j = (
        (s["__sd"] == dim["__d"])
        & (s["__sn"] == dim["__n"])
        & (dim["current_flag"] == F.lit(True))
    )
    return (
        s.join(dim, j, "left")
        .withColumn(out_alias, F.coalesce(F.col("status_id"), F.lit(0)).cast("bigint"))
        .drop("status_id", "__sd", "__sn", "__d", "__n", "current_flag")
    )


def create_if_not_exists(name: str, cols_sql: str, versioned: bool):
    _assert_fact_table(name)
    extra = ", fact_current_flag boolean" if versioned else ""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {CAT}.{DB}.{name} ({cols_sql}{extra}) USING iceberg"
    )


def replace_with(df: DataFrame, name: str):
    _assert_fact_table(name)
    df.createOrReplaceTempView(f"vw_{name}")
    spark.sql(f"DELETE FROM {CAT}.{DB}.{name}")
    spark.sql(f"INSERT INTO {CAT}.{DB}.{name} SELECT * FROM vw_{name}")


def insert_only(df: DataFrame, name: str):
    _assert_fact_table(name)
    df.createOrReplaceTempView(f"vw_ins_{name}")
    spark.sql(f"INSERT INTO {CAT}.{DB}.{name} SELECT * FROM vw_ins_{name}")


def read_fact(name: str) -> Optional[DataFrame]:
    try:
        return spark.table(f"{CAT}.{DB}.{name}")
    except Exception:
        return None


def xxhash_signature(df: DataFrame, cols: Sequence[str], as_col: str) -> DataFrame:
    sig_inputs = [
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols if c in df.columns
    ]
    return df.withColumn(
        as_col,
        F.xxhash64(*sig_inputs).cast("long") if sig_inputs else F.lit(0).cast("long"),
    )


def _is_empty(df: DataFrame) -> bool:
    return df.rdd.isEmpty()


def colnames_sql(sql_cols: str) -> List[str]:
    return [part.strip().split()[0] for part in sql_cols.strip().split(",")]


def ensure_fact_columns(df: DataFrame, ordered_cols: List[str]) -> DataFrame:
    for c in ordered_cols:
        if c in df.columns:
            continue
        if c.endswith("_date_id"):
            df = df.withColumn(c, F.lit(0).cast("int"))
        elif c.endswith("_fact_id"):
            df = df.withColumn(c, F.lit(None).cast("bigint"))
        elif c.endswith("_sks") or (c.endswith("_sk") and "array" in c):
            df = df.withColumn(c, F.lit(None).cast("array<long>"))
        else:
            df = df.withColumn(c, F.lit(None).cast("bigint"))
    return df


# generic null-filler for facts: numeric -> 0, string -> "N/A", bool -> False, arrays -> []
def fill_nulls_for_fact(df: DataFrame, ordered_cols: List[str]) -> DataFrame:
    schema = df.schema
    type_map = {f.name: f.dataType for f in schema.fields}

    for c in ordered_cols:
        if c not in type_map:
            continue
        dt = type_map[c]

        if isinstance(dt, (IntegerType, LongType, ShortType, ByteType, FloatType, DoubleType, DecimalType)):
            df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0).cast(dt)))
        elif isinstance(dt, StringType):
            df = df.withColumn(c, F.coalesce(F.col(c), F.lit("N/A")))
        elif isinstance(dt, BooleanType):
            df = df.withColumn(c, F.coalesce(F.col(c), F.lit(False)))
        elif isinstance(dt, ArrayType):
            df = df.withColumn(
                c,
                F.when(F.col(c).isNull(), F.array().cast(dt)).otherwise(F.col(c)),
            )

    return df


def project_fact(df: DataFrame, ordered_cols: List[str]) -> DataFrame:
    df = ensure_fact_columns(df, ordered_cols)
    df = fill_nulls_for_fact(df, ordered_cols)
    bad = [c for c in df.columns if c.endswith("_id") and c not in ordered_cols]
    if bad:
        df = df.drop(*bad)
    return df.select(*ordered_cols)


def debug_fact(df: DataFrame, name: str, ordered_cols: List[str]) -> DataFrame:
    if not VERBOSE:
        return df
    print(f"\n[DEBUG] {name}: selected columns -> {ordered_cols}")
    df.printSchema()
    for r in df.limit(5).collect():
        print(r)
    return df


# -----------------------------------
# FACT DDLs
# -----------------------------------

DGW_COLS = """
  dg_window_fact_id bigint,
  window_open_date_id int,
  window_close_date_id int,
  dg_window_sk bigint,
  financial_year_sk bigint,
  dgwindowindicatorgrant_sk array<bigint>
"""

LOI_EVT_COLS = """
  loi_id bigint,
  loi_sk bigint,
  intervention_count int,
  intervention_sks array<bigint>,
  created_date_id int,
  submitted_date_id int,
  company_sk bigint,
  dg_window_sk bigint
"""

INTERV_COLS = """
  intervention_fact_id bigint,
  intervention_created_date_id int,
  loi_sk bigint,
  intervention_sk bigint,
  company_sk bigint,
  indicator_sk bigint,
  grant_type_sk bigint,
  subsector_sk bigint,
  municipality_sk bigint,
  is_current boolean
"""

VETTING_COLS = """
  vetting_fact_id bigint,
  vetting_sk bigint,
  vetting_approval_sk bigint,
  intervention_sk bigint,
  loi_sk bigint,
  vettingrisk_sks array<bigint>
"""

APPEAL_COLS = """
  appeal_fact_id bigint,
  appeal_sk bigint,
  intervention_no bigint,
  loi_no bigint,
  appeal_date int,
  appeal_status bigint,
  is_current boolean
"""

LP_COLS = """
  learning_programme_fact_id bigint,
  sla_sk bigint,
  learning_programme_sk bigint,
  learningprogrammeaddendum_sk bigint,
  commitmentregister_sk bigint,
  disbursement_sk array<bigint>,
  writeback_sk bigint,
  trainingprovider_sk bigint,
  slalearnerlistrequest_sk bigint
"""

LEARNER_COLS = """
  learner_fact_id bigint,
  contract_date_id int,
  placement_date_id int,
  completion_date_id int,
  employment_date_id int,
  learner_programme_sk bigint,
  learner_sk bigint,
  sla_sk bigint,
  learning_programme_sk bigint
"""

NFP_COLS = """
  nfp_id bigint,
  nf_sk bigint,
  nf_learners array<bigint>,
  nf_cc bigint,
  nf_cl bigint,
  nf_tp bigint
"""

COMPANY_COLS = """
  company_fact_id bigint,
  company_sk bigint,
  industry_sk bigint,
  province_sk bigint,
  director_sks array<bigint>
"""

LPD_DISB_FACT_COLS = """
  learningprogramme_disbursement_fact_id bigint,
  learningprogrammedisbursement_id bigint,
  company_sk bigint,
  learningprogramme_sk bigint,
  sla_sk bigint,
  dg_window_sk bigint,
  disbursement_stage_sk bigint,
  discretionarygrant_sk bigint,
  indicator_sk bigint,
  trade_payable_finyear_sk bigint,
  payment_batch_sk bigint,
  grants_admin_user_sk bigint,
  scheduled_date_sk int,
  approved_date_sk int
"""


# -----------------------------------
# One-time migration helper (scalar -> array) for fact_company
# -----------------------------------
def migrate_fact_company_to_array_directors():
    tbl = f"{CAT}.{DB}.fact_company"
    try:
        df = spark.table(tbl)
        fields = {f.name: f.dataType.simpleString().upper() for f in df.schema.fields}
    except Exception:
        return

    has_director_sks = "DIRECTOR_SKS" in fields
    has_director_sk = "DIRECTOR_SK" in fields

    if has_director_sks and fields["DIRECTOR_SKS"].startswith("ARRAY"):
        if has_director_sk:
            try:
                spark.sql(f"ALTER TABLE {tbl} DROP COLUMN director_sk")
            except Exception:
                pass
        return

    if has_director_sk and not has_director_sks:
        tmp = f"{CAT}.{DB}.fact_company_tmp"
        spark.sql(f"DROP TABLE IF EXISTS {tmp}")
        spark.sql(
            f"""
            CREATE TABLE {tmp} (
              company_fact_id BIGINT,
              company_sk BIGINT,
              industry_sk BIGINT,
              province_sk BIGINT,
              director_sks ARRAY<BIGINT>
            ) USING iceberg
            """
        )
        (
            spark.table(tbl)
            .select(
                "company_fact_id",
                "company_sk",
                "industry_sk",
                "province_sk",
                F.array(F.col("director_sk")).alias("director_sks"),
            )
            .writeTo(tmp)
            .append()
        )
        spark.sql(f"ALTER TABLE {tbl} RENAME TO fact_company_old_legacy_scalar")
        spark.sql(f"ALTER TABLE {tmp} RENAME TO fact_company")


# -----------------------------------
# FACT BUILDERS
# -----------------------------------

def build_fact_dg_window() -> DataFrame:
    dgw = T("dim_lpd_dgwindow").alias("dgw")

    dgw = to_date_id(dgw, "beginning", "window_open_date_id")
    dgw = to_date_id(dgw, "ending", "window_close_date_id")

    fy = T("dim_src_financialyear").select(
        F.col("id").alias("__fy_id"),
        F.col("src_financialyear_sk").cast("bigint").alias("financial_year_sk"),
    )
    dgw = dgw.join(fy, dgw["financial_year_id"] == fy["__fy_id"], "left")

    wig = T("dim_lpd_dgwindowindicatorgrant").select(
        F.col("dg_window_id").alias("__dgw_id"),
        F.col("lpd_dgwindowindicatorgrant_sk")
        .cast("bigint")
        .alias("dgwindowindicatorgrant_sk_single"),
    )
    wig_agg = (
        wig.groupBy("__dgw_id")
        .agg(
            F.collect_set("dgwindowindicatorgrant_sk_single").alias(
                "dgwindowindicatorgrant_sk"
            )
        )
    )

    dgw = dgw.join(wig_agg, dgw["id"] == wig_agg["__dgw_id"], "left")

    out = (
        dgw.withColumn("dg_window_sk", F.col("dgw.lpd_dgwindow_sk").cast("bigint"))
        .withColumn("financial_year_sk", F.col("financial_year_sk").cast("bigint"))
        .withColumn(
            "dgwindowindicatorgrant_sk",
            F.when(
                F.col("dgwindowindicatorgrant_sk").isNull(),
                F.array().cast("array<long>"),
            ).otherwise(F.col("dgwindowindicatorgrant_sk")),
        )
    )

    out = out.withColumn(
        "dg_window_fact_id",
        F.row_number()
        .over(W.orderBy(F.col("dgw.id").asc_nulls_last()))
        .cast("bigint"),
    )

    ordered = colnames_sql(DGW_COLS)
    out = project_fact(out, ordered)
    return debug_fact(out, "fact_dg_window", ordered)


def build_fact_loi_status_history() -> DataFrame:
    loi = T("dim_lpd_loi").alias("loi")

    loi = to_date_id(loi, "created_at", "created_date_id")
    loi = to_date_id(loi, "date_submitted", "submitted_date_id")

    iv = T("dim_lpd_loiintervention").select(
        F.col("loi_id").alias("__iv_loiid"),
        F.col("lpd_loiintervention_sk").cast("bigint").alias("intervention_sk"),
    )

    iv_agg = (
        iv.groupBy("__iv_loiid")
        .agg(
            F.count("intervention_sk").cast("int").alias("intervention_count"),
            F.collect_set("intervention_sk").alias("intervention_sks"),
        )
    )

    loi = loi.join(iv_agg, loi["id"] == iv_agg["__iv_loiid"], "left")

    cmp = T("dim_company_company").select(
        F.col("id").alias("__cmp_id"),
        F.col("company_company_sk").cast("bigint").alias("company_sk"),
    )
    loi = loi.join(cmp, loi["company_id"] == cmp["__cmp_id"], "left")

    dgw = T("dim_lpd_dgwindow").select(
        F.col("id").alias("__dgw_id"),
        F.col("lpd_dgwindow_sk").cast("bigint").alias("dg_window_sk"),
    )
    loi = loi.join(dgw, loi["dg_window_id"] == dgw["__dgw_id"], "left")

    out = (
        loi.withColumn("loi_sk", F.col("loi.lpd_loi_sk").cast("bigint"))
        .withColumn(
            "intervention_count",
            F.coalesce(F.col("intervention_count"), F.lit(0).cast("int")),
        )
        .withColumn(
            "intervention_sks",
            F.when(
                F.col("intervention_sks").isNull(),
                F.array().cast("array<long>"),
            ).otherwise(F.col("intervention_sks")),
        )
    )

    out = out.withColumn(
        "loi_id",
        F.row_number()
        .over(W.orderBy(F.col("loi.id").asc_nulls_last()))
        .cast("bigint"),
    )

    ordered = colnames_sql(LOI_EVT_COLS)
    out = project_fact(out, ordered)
    return debug_fact(out, "fact_loi_status_history", ordered)


def build_fact_intervention_full() -> DataFrame:
    iv = T("dim_lpd_loiintervention").alias("iv")

    iv = to_date_id(iv, "created_at", "intervention_created_date_id")

    loi = T("dim_lpd_loi").select(
        F.col("id").alias("__iv_loiid"),
        F.col("lpd_loi_sk").alias("loi_sk"),
        F.col("company_id").alias("__cmp_id"),
    )
    iv = iv.join(loi, iv["loi_id"] == loi["__iv_loiid"], "left")

    cmp = T("dim_company_company").select(
        F.col("id").alias("__cmp_id2"),
        F.col("company_company_sk").alias("company_sk"),
    )
    iv = iv.join(cmp, loi["__cmp_id"] == cmp["__cmp_id2"], "left")

    sub = T("dim_configurable_suburb").select(
        F.col("id").alias("__sub_id"),
        F.col("configurable_suburb_sk").cast("bigint").alias("__suburb_sk"),
    )
    iv = iv.join(sub, iv["area_id"] == sub["__sub_id"], "left")

    sic = T("dim_src_subsectoractivity").select(
        F.col("id").alias("__sic_id"),
        F.col("src_subsectoractivity_sk").cast("bigint").alias("__subsector_sk"),
    )
    iv = iv.join(sic, iv["sic_code_id"] == sic["__sic_id"], "left")

    dg = T("dim_src_descretionarygrant").select(
        F.col("id").alias("__dg_id"),
        F.col("src_descretionarygrant_sk").cast("bigint").alias("__grant_type_sk"),
    )
    iv = iv.join(dg, iv["intervention_id"] == dg["__dg_id"], "left")

    def resolve_indicator(iv_df: DataFrame) -> DataFrame:
        if "src_indicator_sk" in iv_df.columns:
            return iv_df.withColumn(
                "indicator_sk", F.col("src_indicator_sk").cast("bigint")
            )
        dimind = T("dim_src_indicator").select(
            F.col("id").alias("__ind_id"),
            F.col("src_indicator_sk").cast("bigint").alias("indicator_sk"),
        )
        candidates = ["indicator_id", "src_indicator_id", "indicator", "indicator_fk"]
        for cand in candidates:
            if cand in iv_df.columns:
                return (
                    iv_df.join(dimind, iv_df[cand] == dimind["__ind_id"], "left")
                    .drop("__ind_id")
                )
        return iv_df.withColumn("indicator_sk", F.lit(0).cast("bigint"))

    iv = resolve_indicator(iv)

    out = (
        iv.withColumn(
            "intervention_sk", F.col("iv.lpd_loiintervention_sk").cast("bigint")
        )
        .withColumn("grant_type_sk", F.col("__grant_type_sk"))
        .withColumn("subsector_sk", F.col("__subsector_sk"))
        .withColumn("municipality_sk", F.col("__suburb_sk"))
        .withColumn("company_sk", F.col("company_sk"))
        .withColumn("loi_sk", F.col("loi_sk"))
    )

    out = out.withColumn(
        "is_current",
        F.col("iv.current_flag").cast("boolean")
        if "current_flag" in iv.columns
        else F.lit(True),
    )

    out = out.withColumn(
        "intervention_fact_id",
        F.row_number()
        .over(W.orderBy(F.col("iv.id").asc_nulls_last()))
        .cast("bigint"),
    )

    ordered = colnames_sql(INTERV_COLS)
    out = project_fact(out, ordered)
    return debug_fact(out, "fact_intervention", ordered)


def build_fact_vetting() -> DataFrame:
    vt = T("dim_lpd_loivetting").alias("vt")

    vt = vt.withColumn("vetting_sk", F.col("vt.lpd_loivetting_sk").cast("bigint"))

    vap = T("dim_lpd_loivettingapproval").select(
        F.col("vetting_id").alias("__vap_vetting_id"),
        F.col("lpd_loivettingapproval_sk")
        .cast("bigint")
        .alias("vetting_approval_sk"),
    )
    vt = vt.join(vap, vt["id"] == vap["__vap_vetting_id"], "left")

    iv = T("dim_lpd_loiintervention").select(
        F.col("id").alias("__iv_id"),
        F.col("lpd_loiintervention_sk").cast("bigint").alias("intervention_sk"),
    )
    vt = vt.join(iv, vt["intervention_id"] == iv["__iv_id"], "left")

    loi = T("dim_lpd_loi").select(
        F.col("id").alias("__loi_id"),
        F.col("lpd_loi_sk").cast("bigint").alias("loi_sk"),
    )
    vt = vt.join(loi, vt["loi_id"] == loi["__loi_id"], "left")

    lvr = T("dim_lpd_loivettingrisk").select(
        F.col("vetting_id").alias("__vetting_id"),
        F.col("risk_id").alias("__risk_id"),
    )

    dr = T("dim_lpd_vettingrisk").select(
        F.col("id").alias("__risk_id2"),
        F.col("lpd_vettingrisk_sk").cast("bigint").alias("vettingrisk_sk"),
    )

    vt_for_bridge = vt.select(
        F.col("id").alias("__vt_id_for_bridge"), F.col("vetting_sk")
    )

    lvr_with_sk = (
        lvr.join(
            vt_for_bridge,
            lvr["__vetting_id"] == vt_for_bridge["__vt_id_for_bridge"],
            "left",
        )
        .join(dr, lvr["__risk_id"] == dr["__risk_id2"], "left")
    )

    risk_by_vetting_sk = lvr_with_sk.groupBy("vetting_sk").agg(
        F.collect_set("vettingrisk_sk").alias("vettingrisk_sks")
    )

    vt = vt.join(risk_by_vetting_sk, "vetting_sk", "left")

    out = (
        vt.withColumn(
            "vetting_sk",
            F.coalesce(F.col("vetting_sk"), F.lit(0).cast("bigint")),
        )
        .withColumn(
            "vetting_approval_sk",
            F.coalesce(F.col("vetting_approval_sk"), F.lit(0).cast("bigint")),
        )
        .withColumn(
            "intervention_sk",
            F.coalesce(F.col("intervention_sk"), F.lit(0).cast("bigint")),
        )
        .withColumn("loi_sk", F.coalesce(F.col("loi_sk"), F.lit(0).cast("bigint")))
    )

    out = out.withColumn(
        "vettingrisk_sks",
        F.when(
            (F.col("vettingrisk_sks").isNull()) | (F.size("vettingrisk_sks") == 0),
            F.array(F.lit(0).cast("bigint")),
        ).otherwise(F.col("vettingrisk_sks")),
    )

    out = out.withColumn(
        "vetting_fact_id",
        F.row_number()
        .over(W.orderBy(F.col("vt.id").asc_nulls_last()))
        .cast("bigint"),
    )

    ordered = colnames_sql(VETTING_COLS)
    out = project_fact(out, ordered)
    return debug_fact(out, "fact_vetting", ordered)


def build_fact_appeal_full() -> DataFrame:
    ap = T("dim_lpd_loiinterventionappeal").alias("ap")

    ap = ap.withColumn(
        "appeal_sk", F.col("ap.lpd_loiinterventionappeal_sk").cast("bigint")
    )

    iv = T("dim_lpd_loiintervention").select(
        F.col("id").alias("__iv_id"),
        F.col("lpd_loiintervention_sk").cast("bigint").alias("intervention_no"),
        F.col("loi_id").alias("__iv_loiid"),
    )
    ap = ap.join(iv, ap["intervention_id"] == iv["__iv_id"], "left")

    loi = T("dim_lpd_loi").select(
        F.col("id").alias("__loi_id"),
        F.col("lpd_loi_sk").cast("bigint").alias("loi_no"),
    )
    ap = ap.join(loi, iv["__iv_loiid"] == loi["__loi_id"], "left")

    ap = to_date_id(ap, "date_appeal", "appeal_date_id")
    ap = ap.withColumnRenamed("appeal_date_id", "appeal_date")

    ap = map_status(ap, "status", "__appeal_status_sk", "lpd_loiinterventionappeal")
    ap = ap.withColumn("appeal_status", F.col("__appeal_status_sk").cast("bigint"))

    ap = ap.withColumn(
        "is_current",
        F.col("ap.current_flag").cast("boolean")
        if "current_flag" in ap.columns
        else F.lit(True),
    )

    for colname in ["__appeal_status_sk", "__iv_id", "__iv_loiid", "__loi_id"]:
        if colname in ap.columns:
            ap = ap.drop(colname)

    ap = ap.withColumn(
        "appeal_fact_id",
        F.row_number()
        .over(W.orderBy(F.col("ap.id").asc_nulls_last()))
        .cast("bigint"),
    )

    ordered = colnames_sql(APPEAL_COLS)
    out = project_fact(ap, ordered)
    return debug_fact(out, "fact_appeal", ordered)


def ensure_versioned_table(name: str, cols_sql: str):
    create_if_not_exists(name, cols_sql, versioned=True)


def versioned_upsert(
    fact_name: str,
    df_new: DataFrame,
    nk_cols: Union[str, List[str]],
    sig_cols: Sequence[str],
    ordered_cols: List[str],
):
    nk_cols = [nk_cols] if isinstance(nk_cols, str) else list(nk_cols)
    pk_col = ordered_cols[0]

    n = df_new
    if pk_col in n.columns:
        n = n.drop(pk_col)
    n = xxhash_signature(n, sig_cols, "__sig").alias("n")

    fact = read_fact(fact_name)
    if fact is None:
        w = W.orderBy(*[F.col(c).asc_nulls_last() for c in nk_cols if c in n.columns])
        out = (
            n.withColumn(pk_col, F.row_number().over(w).cast("bigint")).withColumn(
                "fact_current_flag", F.lit(True)
            )
        )
        out = project_fact(out, ordered_cols + ["fact_current_flag"])
        out = debug_fact(out, fact_name, ordered_cols + ["fact_current_flag"])
        replace_with(out, fact_name)
        return

    c = fact.filter("fact_current_flag = true").alias("c")

    join_cond = [
        F.col(f"n.{k}") == F.col(f"c.{k}") for k in nk_cols if k in n.columns and k in c.columns
    ]
    if join_cond:
        j = n.join(
            c,
            reduce(lambda a, b: a & b, join_cond),
            "left",
        ).select("n.*", F.col(f"c.{pk_col}").alias("__curr_pk"))
    else:
        j = n.withColumn("__curr_pk", F.lit(None).cast("bigint"))

    base_cols: List[str] = []
    for x in list(nk_cols) + list(sig_cols):
        if x in c.columns and x not in base_cols:
            base_cols.append(x)

    cur_base = c.select(*base_cols)
    cur_sig = xxhash_signature(
        cur_base,
        [col for col in sig_cols if col in cur_base.columns],
        "__cur_sig",
    )
    for k in nk_cols:
        if k in cur_sig.columns:
            cur_sig = cur_sig.withColumnRenamed(k, f"__k_{k}")
    cur_sig = cur_sig.alias("cs")

    cond2 = [
        F.col(f"n.{k}") == F.col(f"cs.__k_{k}")
        for k in nk_cols
        if k in n.columns and f"__k_{k}" in cur_sig.columns
    ]
    if cond2:
        j = j.join(
            cur_sig,
            reduce(lambda a, b: a & b, cond2),
            "left",
        ).select(
            "n.*",
            "__curr_pk",
            "__cur_sig",
        )
    else:
        j = j.withColumn("__cur_sig", F.lit(None).cast("long"))

    to_close = (
        j.filter("__curr_pk IS NOT NULL AND (__sig != __cur_sig OR __cur_sig IS NULL)")
        .select(F.col("__curr_pk").alias("__pk_to_close"))
        .distinct()
    )
    if not _is_empty(to_close):
        upd = (
            fact.join(to_close, fact[pk_col] == to_close["__pk_to_close"], "left")
            .withColumn(
                "fact_current_flag",
                F.when(F.col("__pk_to_close").isNotNull(), F.lit(False)).otherwise(
                    F.col("fact_current_flag")
                ),
            )
            .drop("__pk_to_close")
        )
        replace_with(upd, fact_name)

    target = read_fact(fact_name)
    max_pk = (
        target.agg(F.max(pk_col).alias("m")).collect()[0]["m"]
        if target is not None
        else 0
    )
    max_pk = int(max_pk) if max_pk is not None else 0

    to_insert = j.filter(
        "__curr_pk IS NULL OR (__sig != __cur_sig OR __cur_sig IS NULL)"
    ).drop("__curr_pk", "__cur_sig", "__sig")

    if not _is_empty(to_insert):
        w = W.orderBy(
            *[F.col(k).asc_nulls_last() for k in nk_cols if k in to_insert.columns]
        )
        to_insert = (
            to_insert.withColumn(
                pk_col, (F.row_number().over(w) + F.lit(max_pk)).cast("bigint")
            ).withColumn("fact_current_flag", F.lit(True))
        )
        to_insert = project_fact(to_insert, ordered_cols + ["fact_current_flag"])
        to_insert = debug_fact(to_insert, fact_name, ordered_cols + ["fact_current_flag"])
        insert_only(to_insert, fact_name)


def build_learner_versioned():
    lr = T("dim_lpd_learnerprogramme").alias("lr")

    lr = to_date_id(lr, "created_at", "contract_date_id")

    pl = T("dim_lpd_learnerprogrammeplacement").select(
        F.col("learner_programme_id").alias("__lp_id"),
        F.col("created_at").alias("__place_ts"),
    )
    lr = lr.join(pl, lr["id"] == pl["__lp_id"], "left")
    lr = to_date_id(lr, "__place_ts", "placement_date_id")

    term = T("dim_lpd_learnerprogrammetermination").select(
        F.col("learner_programme_id").alias("__lp_id2"),
        F.col("termination_date").alias("__term_dt"),
    )
    lr = lr.join(term, lr["id"] == term["__lp_id2"], "left")
    lr = to_date_id(lr, "__term_dt", "completion_date_id")

    lr = to_date_id(lr, "date_employed", "employment_date_id")

    lpdim = T("dim_lpd_learningprogramme").select(
        F.col("id").alias("__lp_id_dim"),
        F.col("lpd_learningprogramme_sk").cast("bigint").alias("learning_programme_sk"),
    )
    lr = lr.join(lpdim, lr["learning_programme_id"] == lpdim["__lp_id_dim"], "left")

    sladim = T("dim_lpd_learningprogrammesla").select(
        F.col("id").alias("__sla_id_dim"),
        F.col("lpd_learningprogrammesla_sk").cast("bigint").alias("sla_sk"),
    )
    lr = lr.join(
        sladim, lr["learning_programme_sla_id"] == sladim["__sla_id_dim"], "left"
    )

    learndim = T("dim_learner_learner").select(
        F.col("id").alias("__learner_id_dim"),
        F.col("learner_learner_sk").cast("bigint").alias("learner_sk"),
    )
    lr = lr.join(learndim, lr["learner_id"] == learndim["__learner_id_dim"], "left")

    out = (
        lr.withColumn(
            "learner_programme_sk",
            F.col("lr.lpd_learnerprogramme_sk").cast("bigint"),
        )
        .withColumn("learner_sk", F.col("learner_sk").cast("bigint"))
        .withColumn("sla_sk", F.col("sla_sk").cast("bigint"))
        .withColumn("learning_programme_sk", F.col("learning_programme_sk").cast("bigint"))
    )

    ordered = colnames_sql(LEARNER_COLS)
    nk = ["learner_programme_sk"]
    sig = [
        "contract_date_id",
        "placement_date_id",
        "completion_date_id",
        "employment_date_id",
        "learner_sk",
        "sla_sk",
        "learning_programme_sk",
        "learner_programme_sk",
    ]
    out = project_fact(out, ordered)
    out = debug_fact(out, "fact_learner", ordered)
    return out, nk, sig, ordered


def build_fact_nonfunded_programme_full() -> DataFrame:
    nf = T("dim_lpd_nonfundedprogramme").alias("nf")

    nf = nf.withColumn("nf_sk", F.col("nf.lpd_nonfundedprogramme_sk").cast("bigint"))

    nfl = T("dim_lpd_nonfundedlearnerprogramme").select(
        F.col("non_funded_programme_id").alias("__nf_id"),
        F.col("lpd_nonfundedlearnerprogramme_sk").cast("bigint").alias("nf_learner_sk"),
    )
    nfl_agg = nfl.groupBy("__nf_id").agg(
        F.collect_set("nf_learner_sk").alias("nf_learners")
    )

    nfcc = T("dim_lpd_nonfundedcompliancecheck").select(
        F.col("nf_id").alias("__nf_id_cc"),
        F.col("lpd_nonfundedcompliancecheck_sk").cast("bigint").alias("nf_cc_sk"),
    )
    nfcc_agg = nfcc.groupBy("__nf_id_cc").agg(F.max("nf_cc_sk").alias("nf_cc"))

    nfcl = T("dim_lpd_nonfundedchecklist").select(
        F.col("nf_id").alias("__nf_id_cl"),
        F.col("lpd_nonfundedchecklist_sk").cast("bigint").alias("nf_cl_sk"),
    )
    nfcl_agg = nfcl.groupBy("__nf_id_cl").agg(F.max("nf_cl_sk").alias("nf_cl"))

    nftp = T("dim_lpd_nonfundedprogrammetrainingprovider").select(
        F.col("nf_programme_id").alias("__nf_id_tp"),
        F.col("lpd_nonfundedprogrammetrainingprovider_sk").cast("bigint").alias(
            "nf_tp_sk"
        ),
    )
    nftp_agg = nftp.groupBy("__nf_id_tp").agg(F.max("nf_tp_sk").alias("nf_tp"))

    out = nf
    out = out.join(nfl_agg, out["id"] == nfl_agg["__nf_id"], "left")
    out = out.join(nfcc_agg, out["id"] == nfcc_agg["__nf_id_cc"], "left")
    out = out.join(nfcl_agg, out["id"] == nfcl_agg["__nf_id_cl"], "left")
    out = out.join(nftp_agg, out["id"] == nftp_agg["__nf_id_tp"], "left")

    out = out.withColumn(
        "nf_learners",
        F.when(F.col("nf_learners").isNull(), F.array().cast("array<long>")).otherwise(
            F.col("nf_learners")
        ),
    )

    out = out.withColumn(
        "nfp_id",
        F.row_number()
        .over(W.orderBy(F.col("nf.id").asc_nulls_last()))
        .cast("bigint"),
    )

    ordered = colnames_sql(NFP_COLS)
    out = project_fact(out, ordered)
    return debug_fact(out, "fact_nonfunded_programme", ordered)


def build_fact_learning_programme_full() -> DataFrame:
    sla = T("dim_lpd_learningprogrammesla").alias("sla")
    sla = sla.withColumn(
        "sla_sk", F.col("sla.lpd_learningprogrammesla_sk").cast("bigint")
    )

    lpdim = T("dim_lpd_learningprogramme").select(
        F.col("id").alias("__lp_id"),
        F.col("lpd_learningprogramme_sk").cast("bigint").alias("learning_programme_sk"),
    )
    sla = sla.join(lpdim, sla["learning_programme_id"] == lpdim["__lp_id"], "left")

    add = T("dim_lpd_learningprogrammeaddendum").select(
        F.col("learning_programme_sla_id").alias("__add_sla_id"),
        F.col("lpd_learningprogrammeaddendum_sk")
        .cast("bigint")
        .alias("learningprogrammeaddendum_sk"),
    )
    sla = sla.join(add, sla["id"] == add["__add_sla_id"], "left")

    cr = T("dim_lpd_learningprogrammecommitmentregister").alias("cr")
    cr = cr.select(
        F.col("id").alias("commitmentregister_id"),
        F.col("lpd_learningprogrammecommitmentregister_sk")
        .cast("bigint")
        .alias("commitmentregister_sk"),
        F.col("learning_programme_sla_id").alias("cr_learning_programme_sla_id"),
        "first_disbursement_id",
        "second_disbursement_id",
        "third_disbursement_id",
        "fourth_disbursement_id",
        "fifth_disbursement_id",
        "sixth_disbursement_id",
    )

    disb_dim = T("dim_lpd_learningprogrammedisbursement").select(
        F.col("id").alias("disb_id"),
        F.col("lpd_learningprogrammedisbursement_sk")
        .cast("bigint")
        .alias("disbursement_sk_single"),
    )

    base = disb_dim

    cr = cr.join(
        base.withColumnRenamed("disb_id", "first_disbursement_id").withColumnRenamed(
            "disbursement_sk_single", "first_disbursement_sk"
        ),
        on="first_disbursement_id",
        how="left",
    )

    cr = cr.join(
        base.withColumnRenamed("disb_id", "second_disbursement_id").withColumnRenamed(
            "disbursement_sk_single", "second_disbursement_sk"
        ),
        on="second_disbursement_id",
        how="left",
    )

    cr = cr.join(
        base.withColumnRenamed("disb_id", "third_disbursement_id").withColumnRenamed(
            "disbursement_sk_single", "third_disbursement_sk"
        ),
        on="third_disbursement_id",
        how="left",
    )

    cr = cr.join(
        base.withColumnRenamed("disb_id", "fourth_disbursement_id").withColumnRenamed(
            "disbursement_sk_single", "fourth_disbursement_sk"
        ),
        on="fourth_disbursement_id",
        how="left",
    )

    cr = cr.join(
        base.withColumnRenamed("disb_id", "fifth_disbursement_id").withColumnRenamed(
            "disbursement_sk_single", "fifth_disbursement_sk"
        ),
        on="fifth_disbursement_id",
        how="left",
    )

    cr = cr.join(
        base.withColumnRenamed("disb_id", "sixth_disbursement_id").withColumnRenamed(
            "disbursement_sk_single", "sixth_disbursement_sk"
        ),
        on="sixth_disbursement_id",
        how="left",
    )

    sk_cols = [
        "first_disbursement_sk",
        "second_disbursement_sk",
        "third_disbursement_sk",
        "fourth_disbursement_sk",
        "fifth_disbursement_sk",
        "sixth_disbursement_sk",
    ]

    cr = cr.withColumn("disbursement_sk_raw", F.array(*[F.col(c) for c in sk_cols]))
    cr = cr.withColumn(
        "disbursement_sk",
        F.expr("filter(disbursement_sk_raw, x -> x IS NOT NULL)"),
    )

    sla = sla.join(
        cr,
        sla["id"] == cr["cr_learning_programme_sla_id"],
        "left",
    )

    wb = T("dim_lpd_learningprogrammewriteback").select(
        F.col("learning_programme_sla_id").alias("__wb_sla_id"),
        F.col("lpd_learningprogrammewriteback_sk").cast("bigint").alias("writeback_sk"),
    )
    sla = sla.join(wb, sla["id"] == wb["__wb_sla_id"], "left")

    lptp = T("dim_lpd_learningprogrammetrainingprovider").select(
        F.col("learning_programme_id").alias("__lptp_lp_id"),
        F.col("lpd_learningprogrammetrainingprovider_sk")
        .cast("bigint")
        .alias("trainingprovider_sk"),
    )
    sla = sla.join(lptp, sla["__lp_id"] == lptp["__lptp_lp_id"], "left")

    llr = T("dim_lpd_learningprogrammeslalearnerlistrequest").select(
        F.col("learning_programme_sla_id").alias("__llr_sla_id"),
        F.col("lpd_learningprogrammeslalearnerlistrequest_sk")
        .cast("bigint")
        .alias("slalearnerlistrequest_sk"),
    )
    sla = sla.join(llr, sla["id"] == llr["__llr_sla_id"], "left")

    out = sla.withColumn(
        "learning_programme_fact_id",
        F.row_number().over(W.orderBy(F.col("sla.id").asc_nulls_last())).cast("bigint"),
    )

    ordered = colnames_sql(LP_COLS)
    out = project_fact(out, ordered)
    return debug_fact(out, "fact_learning_programme", ordered)


def build_company_fact_txn():
    cc = T("dim_company_company").alias("cc")
    ind = T("dim_src_subsectoractivity").select(
        F.col("id").alias("__ind_id"),
        F.col("src_subsectoractivity_sk").cast("bigint").alias("industry_sk"),
    )
    cc = cc.join(ind, cc["industry_id"] == ind["__ind_id"], "left")
    pr = T("dim_configurable_province").select(
        F.col("id").alias("__prov_id"),
        F.col("configurable_province_sk").cast("bigint").alias("province_sk"),
    )
    cc = cc.join(pr, cc["province_id"] == pr["__prov_id"], "left")
    dim_cd = T("dim_company_director")
    have_created = "created_at" in dim_cd.columns
    cd = dim_cd.select(
        F.col("company_id").alias("__dir_co_id"),
        F.col("company_director_sk").cast("bigint").alias("director_sk"),
        *([F.col("created_at")] if have_created else []),
    )
    if have_created:
        order_struct = F.struct(F.col("created_at"), F.col("director_sk"))
    else:
        order_struct = F.struct(F.lit(None).cast("timestamp"), F.col("director_sk"))
    cd_ord = (
        cd.withColumn("__order", order_struct)
        .groupBy("__dir_co_id")
        .agg(
            F.sort_array(
                F.array_distinct(
                    F.collect_list(F.struct("__order", "director_sk"))
                )
            ).alias("__pairs")
        )
        .withColumn("director_sks", F.expr("transform(__pairs, x -> x.director_sk)"))
        .drop("__pairs")
    )
    cc = cc.join(cd_ord, cc["id"] == cd_ord["__dir_co_id"], "left")
    out = (
        cc.withColumn("company_sk", F.col("cc.company_company_sk").cast("bigint"))
        .withColumn("industry_sk", F.col("industry_sk").cast("bigint"))
        .withColumn("province_sk", F.col("province_sk").cast("bigint"))
        .withColumn("director_sks", F.col("director_sks").cast("array<long>"))
    )
    out = out.withColumn(
        "company_fact_id",
        F.row_number().over(W.orderBy(F.col("cc.id").asc_nulls_last())).cast("bigint"),
    )
    ordered = colnames_sql(COMPANY_COLS)
    out = project_fact(out, ordered)
    return debug_fact(out, "fact_company", ordered)


def build_fact_learningprogramme_disbursement() -> DataFrame:
    ds = T("dim_lpd_learningprogrammedisbursement").alias("ds")

    out = ds.withColumn(
        "learningprogrammedisbursement_id",
        F.col("ds.id").cast("bigint"),
    )

    lp = T("dim_lpd_learningprogramme").select(
        F.col("id").alias("__lp_id"),
        F.col("intervention_id").alias("__lp_intervention_id"),
        F.col("lpd_learningprogramme_sk").cast("bigint").alias("learningprogramme_sk"),
    )
    out = out.join(lp, out["learning_programme_id"] == lp["__lp_id"], "left")

    iv = T("dim_lpd_loiintervention").select(
        F.col("id").alias("__iv_id"),
        F.col("loi_id").alias("__iv_loi_id"),
        F.col("indicator_id").alias("__iv_indicator_id"),
    )
    out = out.join(iv, out["__lp_intervention_id"] == iv["__iv_id"], "left")

    loi = T("dim_lpd_loi").select(
        F.col("id").alias("__loi_id"),
        F.col("company_id").alias("__loi_company_id"),
        F.col("dg_window_id").alias("__loi_dg_window_id"),
        F.col("financial_year_id").alias("__loi_finyear_id"),
    )
    out = out.join(loi, iv["__iv_loi_id"] == loi["__loi_id"], "left")

    cmp = T("dim_company_company").select(
        F.col("id").alias("__cmp_id"),
        F.col("company_company_sk").cast("bigint").alias("company_sk"),
    )
    out = out.join(cmp, loi["__loi_company_id"] == cmp["__cmp_id"], "left")

    dgw = T("dim_lpd_dgwindow").select(
        F.col("id").alias("__dgw_id"),
        F.col("lpd_dgwindow_sk").cast("bigint").alias("dg_window_sk"),
    )
    out = out.join(dgw, loi["__loi_dg_window_id"] == dgw["__dgw_id"], "left")

    fy = T("dim_src_financialyear").select(
        F.col("id").alias("__fy_id"),
        F.col("src_financialyear_sk").cast("bigint").alias("trade_payable_finyear_sk"),
    )
    out = out.join(fy, loi["__loi_finyear_id"] == fy["__fy_id"], "left")

    sla = T("dim_lpd_learningprogrammesla").select(
        F.col("id").alias("__sla_id"),
        F.col("lpd_learningprogrammesla_sk").cast("bigint").alias("sla_sk"),
    )
    out = out.join(
        sla,
        out["learning_programme_sla_id"] == sla["__sla_id"],
        "left",
    )

    stage = T("dim_src_discretionarygrantdisbursement").select(
        F.col("id").alias("__stage_id"),
        F.col("grant_id").alias("__grant_id"),
        F.col("src_discretionarygrantdisbursement_sk")
        .cast("bigint")
        .alias("disbursement_stage_sk"),
    )
    out = out.join(stage, out["disbursement_id"] == stage["__stage_id"], "left")

    dg = T("dim_src_descretionarygrant").select(
        F.col("id").alias("__grant_id2"),
        F.col("src_descretionarygrant_sk").cast("bigint").alias("discretionarygrant_sk"),
    )
    out = out.join(dg, stage["__grant_id"] == dg["__grant_id2"], "left")

    ind = T("dim_src_indicator").select(
        F.col("id").alias("__ind_id"),
        F.col("src_indicator_sk").cast("bigint").alias("indicator_sk"),
    )
    out = out.join(ind, iv["__iv_indicator_id"] == ind["__ind_id"], "left")

    pbs = T("dim_finance_paymentbatchschedule").select(
        F.col("id").alias("__pbs_id"),
        F.col("finance_paymentbatchschedule_sk")
        .cast("bigint")
        .alias("payment_batch_sk"),
    )
    out = out.join(pbs, out["payment_batch_schedule_id"] == pbs["__pbs_id"], "left")

    usr = T("dim_accounts_user").select(
        F.col("id").alias("__usr_id"),
        F.col("accounts_user_sk").cast("bigint").alias("grants_admin_user_sk"),
    )
    out = out.join(usr, out["grants_admin_id"] == usr["__usr_id"], "left")

    out = to_date_id(out, "date_paid", "scheduled_date_sk")

    ap = T("dim_lpd_learningprogrammedisbursementapproval").select(
        F.col("disbursement_id").alias("__ap_ds_id"),
        F.col("admin_date_signoff").alias("__appr_dt"),
    )
    out = out.join(
        ap,
        out["learningprogrammedisbursement_id"] == ap["__ap_ds_id"],
        "left",
    )
    out = to_date_id(out, "__appr_dt", "approved_date_sk")

    out = out.withColumn(
        "learningprogramme_disbursement_fact_id",
        F.row_number()
        .over(W.orderBy(F.col("learningprogrammedisbursement_id").asc_nulls_last()))
        .cast("bigint"),
    )

    ordered = colnames_sql(LPD_DISB_FACT_COLS)
    out = project_fact(out, ordered)
    return debug_fact(out, "fact_learningprogramme_disbursement", ordered)


def load_fact_learningprogramme_disbursement_insert_only():
    fact_name = "fact_learningprogramme_disbursement"
    df_all = build_fact_learningprogramme_disbursement()
    existing = read_fact(fact_name)

    if existing is None or _is_empty(existing):
        replace_with(df_all, fact_name)
        return

    if "learningprogramme_disbursement_fact_id" not in existing.columns:
        replace_with(df_all, fact_name)
        return

    stats = existing.agg(
        F.min("learningprogramme_disbursement_fact_id").alias("min_id"),
        F.max("learningprogramme_disbursement_fact_id").alias("max_id"),
    ).collect()[0]
    min_id = stats["min_id"]
    max_id = stats["max_id"]

    if max_id is None or (max_id == 0 and (min_id == 0 or min_id is None)):
        replace_with(df_all, fact_name)
        return

    new_rows = df_all.alias("n").join(
        existing.select("learningprogrammedisbursement_id").alias("e"),
        on="learningprogrammedisbursement_id",
        how="left_anti",
    )

    if _is_empty(new_rows):
        return

    max_id_int = int(max_id) if max_id is not None else 0

    if "learningprogramme_disbursement_fact_id" in new_rows.columns:
        new_rows = new_rows.drop("learningprogramme_disbursement_fact_id")

    w = W.orderBy(F.col("learningprogrammedisbursement_id").asc_nulls_last())
    new_rows = new_rows.withColumn(
        "learningprogramme_disbursement_fact_id",
        (F.row_number().over(w) + F.lit(max_id_int)).cast("bigint"),
    )

    ordered = colnames_sql(LPD_DISB_FACT_COLS)
    new_rows = project_fact(new_rows, ordered)

    insert_only(new_rows, fact_name)


# -----------------------------------
# CREATE / MIGRATE / LOAD
# -----------------------------------

create_if_not_exists("fact_dg_window", DGW_COLS, versioned=False)
create_if_not_exists("fact_loi_status_history", LOI_EVT_COLS, versioned=False)
create_if_not_exists("fact_intervention", INTERV_COLS, versioned=False)
create_if_not_exists("fact_vetting", VETTING_COLS, versioned=False)
create_if_not_exists("fact_appeal", APPEAL_COLS, versioned=False)
create_if_not_exists("fact_learning_programme", LP_COLS, versioned=False)
create_if_not_exists("fact_nonfunded_programme", NFP_COLS, versioned=False)
create_if_not_exists(
    "fact_learningprogramme_disbursement",
    LPD_DISB_FACT_COLS,
    versioned=False,
)

ensure_versioned_table("fact_learner", LEARNER_COLS)

migrate_fact_company_to_array_directors()
create_if_not_exists("fact_company", COMPANY_COLS, versioned=False)

replace_with(build_fact_dg_window(), "fact_dg_window")
replace_with(build_fact_loi_status_history(), "fact_loi_status_history")
replace_with(build_company_fact_txn(), "fact_company")
replace_with(build_fact_intervention_full(), "fact_intervention")
replace_with(build_fact_vetting(), "fact_vetting")
replace_with(build_fact_appeal_full(), "fact_appeal")
replace_with(build_fact_learning_programme_full(), "fact_learning_programme")
replace_with(build_fact_nonfunded_programme_full(), "fact_nonfunded_programme")

load_fact_learningprogramme_disbursement_insert_only()

builders = [
    ("fact_learner", build_learner_versioned),
]

for name, fn in builders:
    df, nk, sig, ordered = fn()
    versioned_upsert(name, df, nk_cols=nk, sig_cols=sig, ordered_cols=ordered)

job.commit()
