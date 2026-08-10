# build_etqa_facts.py
#
# Builds fact_certifylearners and fact_assessor_moderator via Iceberg CTAS,
# the same way build_layer2_facts_ctas.py builds fact_sdf_application /
# fact_wsp_submission. Run after build_layer2_dims_scd2 (the dim_* tables it
# selects from must already exist) and before publish_etqa_facts_postgres.py
# (which mirrors these two tables to Postgres and currently warns-and-skips
# because neither table has ever been built in this account).
#
# ============================================================================
# PLACEHOLDER -- the SELECT body below is NOT verified against a real schema.
#
# This job replaces a Glue job from the original hand-built pipeline (called
# "ETQA CTAS" in the legacy Step Function JSON) whose source was never found:
# not in this repo, not in the two docx step-function exports, and the
# account has zero crawled data (raw/ and layer2/ Glue databases are both
# empty -- DMS has never run here), so there is no live schema to check
# column names against either.
#
# The output schema below (both tables' column lists) IS verified -- it comes
# straight from FACT_CONFIG in publish_etqa_facts_postgres.py, which already
# depends on these exact column names and FK targets. What is NOT verified is
# which source dim table/columns produce them. Fix the two SELECTs once
# curated/ and layer2/ have real data to inspect (after the first DMS reload
# + crawl + create_layer2_dims bootstrap), then delete this comment block.
# ============================================================================

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'dims_database',
    'dims_catalog_name',
    'warehouse_path',
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

# LOCATION is intentionally omitted: the Iceberg GlueCatalog places new tables
# under spark.sql.catalog.<CAT>.warehouse (args['warehouse_path']) automatically.

spark.sql(f"DROP TABLE IF EXISTS {ICE_CATALOG}.{ICE_DB}.fact_certifylearners")
spark.sql(f"DROP TABLE IF EXISTS {ICE_CATALOG}.{ICE_DB}.fact_assessor_moderator")

# ----------------------------------------------------------------------------
# fact_certifylearners
#
# Required output columns (from publish_etqa_facts_postgres.py FACT_CONFIG):
#   learner_id, etqa_trainingprogrammelearner_id, disability_id, gender_id,
#   race_id, training_provider_programme_id, date_learer_entered_id,
#   date_programme_start_id, certification_date_id,
#   training_programme_start_date_id, training_programme_end_date_id
#
# PLACEHOLDER assumption: dim_etqa_trainingprogrammelearner is the base grain
# (one row per learner-on-programme record) and already carries the *_id
# foreign keys verbatim (Django-style "<related>_id" columns) plus raw date
# columns that need resolving to dim_date.date_id, the same pattern
# build_layer2_facts_ctas.py uses for fact_sdf_application/fact_wsp_submission.
# Verify column names on dim_etqa_trainingprogrammelearner before relying on
# this, and confirm whether the *_date_id columns need a dim_date join (raw
# dates) or are already resolved surrogate keys on the source table.
# ----------------------------------------------------------------------------
ctas_certifylearners = f"""
CREATE TABLE {ICE_DB}.fact_certifylearners
USING iceberg
TBLPROPERTIES (
  'format-version'='2',
  'write.format.default'='parquet'
) AS
SELECT
    tpl.learner_id                                    AS learner_id,
    tpl.id                                             AS etqa_trainingprogrammelearner_id,
    tpl.disability_id                                  AS disability_id,
    tpl.gender_id                                      AS gender_id,
    tpl.race_id                                        AS race_id,
    tpl.training_provider_programme_id                 AS training_provider_programme_id,
    COALESCE(dd_entered.date_id, 0)                    AS date_learer_entered_id,
    COALESCE(dd_prog_start.date_id, 0)                 AS date_programme_start_id,
    COALESCE(dd_cert.date_id, 0)                       AS certification_date_id,
    COALESCE(dd_tp_start.date_id, 0)                   AS training_programme_start_date_id,
    COALESCE(dd_tp_end.date_id, 0)                     AS training_programme_end_date_id
FROM {ICE_DB}.dim_etqa_trainingprogrammelearner tpl
LEFT JOIN {ICE_DB}.dim_date dd_entered
  ON CAST(tpl.date_learner_entered AS DATE) = dd_entered.date_actual
LEFT JOIN {ICE_DB}.dim_date dd_prog_start
  ON CAST(tpl.programme_start_date AS DATE) = dd_prog_start.date_actual
LEFT JOIN {ICE_DB}.dim_date dd_cert
  ON CAST(tpl.certification_date AS DATE) = dd_cert.date_actual
LEFT JOIN {ICE_DB}.dim_date dd_tp_start
  ON CAST(tpl.training_programme_start_date AS DATE) = dd_tp_start.date_actual
LEFT JOIN {ICE_DB}.dim_date dd_tp_end
  ON CAST(tpl.training_programme_end_date AS DATE) = dd_tp_end.date_actual
WHERE tpl.current_flag = TRUE
"""
spark.sql(ctas_certifylearners)

# ----------------------------------------------------------------------------
# fact_assessor_moderator
#
# Required output columns (from publish_etqa_facts_postgres.py FACT_CONFIG):
#   assessor_moderator_id, disability_id, gender_id, race_id,
#   training_provider_id, application_id, training_provider_application_id,
#   unit_standard_application_id, unit_standard_id,
#   assessor_approve_date_id, assessor_end_date_id,
#   moderator_approve_date_id, moderator_end_date_id,
#   application_created_at_id
#
# PLACEHOLDER assumption: dim_etqa_assessormoderator is the base grain, same
# caveats as fact_certifylearners above.
# ----------------------------------------------------------------------------
ctas_assessor_moderator = f"""
CREATE TABLE {ICE_DB}.fact_assessor_moderator
USING iceberg
TBLPROPERTIES (
  'format-version'='2',
  'write.format.default'='parquet'
) AS
SELECT
    am.id                                              AS assessor_moderator_id,
    am.disability_id                                   AS disability_id,
    am.gender_id                                       AS gender_id,
    am.race_id                                         AS race_id,
    am.training_provider_id                            AS training_provider_id,
    am.application_id                                  AS application_id,
    am.training_provider_application_id                AS training_provider_application_id,
    am.unit_standard_application_id                    AS unit_standard_application_id,
    am.unit_standard_id                                AS unit_standard_id,
    COALESCE(dd_assessor_approve.date_id, 0)           AS assessor_approve_date_id,
    COALESCE(dd_assessor_end.date_id, 0)               AS assessor_end_date_id,
    COALESCE(dd_moderator_approve.date_id, 0)          AS moderator_approve_date_id,
    COALESCE(dd_moderator_end.date_id, 0)              AS moderator_end_date_id,
    COALESCE(dd_app_created.date_id, 0)                AS application_created_at_id
FROM {ICE_DB}.dim_etqa_assessormoderator am
LEFT JOIN {ICE_DB}.dim_date dd_assessor_approve
  ON CAST(am.assessor_approve_date AS DATE) = dd_assessor_approve.date_actual
LEFT JOIN {ICE_DB}.dim_date dd_assessor_end
  ON CAST(am.assessor_end_date AS DATE) = dd_assessor_end.date_actual
LEFT JOIN {ICE_DB}.dim_date dd_moderator_approve
  ON CAST(am.moderator_approve_date AS DATE) = dd_moderator_approve.date_actual
LEFT JOIN {ICE_DB}.dim_date dd_moderator_end
  ON CAST(am.moderator_end_date AS DATE) = dd_moderator_end.date_actual
LEFT JOIN {ICE_DB}.dim_date dd_app_created
  ON CAST(am.application_created_at AS DATE) = dd_app_created.date_actual
WHERE am.current_flag = TRUE
"""
spark.sql(ctas_assessor_moderator)

job.commit()
