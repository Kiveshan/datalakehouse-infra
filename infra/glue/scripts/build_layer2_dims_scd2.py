# build_layer2_dims_scd2.py
#
# Nightly SCD2 updater from curated/ (move_raw_tables.py's output, see
# transform.tf) into the Iceberg dim_<table> tables created by
# create_layer2_dims.py, using MERGE.
#
# - Input: s3://<curated_bucket>/<curated_prefix><table>/
# - Output: <dims_catalog_name>.<dims_database>.dim_<...>
# - Compare only business columns present in the dim; NK = id or <source_table>_id
# - Writes: MERGE (close changed) + UPDATE (close deletions) + MERGE (insert new/changed)
# - Load timestamp = current UTC timestamp (script runtime)
#
# Run manually after a curated/ refresh, same as move_raw_tables.py and the
# raw-folder crawler (see "Cataloging and transforming raw" in the root
# CLAUDE.md) — nothing here schedules it automatically. Assumes the target
# dim_<table> tables already exist; run create_layer2_dims.py first for any
# newly mapped table.

import sys, re, boto3, traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.context import SparkContext

# ===================== Glue / Spark bootstrap =====================
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'curated_bucket',
    'curated_prefix',
    'dims_database',
    'dims_catalog_name',
    'warehouse_path',
])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

CATALOG = args['dims_catalog_name']
DB = args['dims_database']

spark: SparkSession = (
    glueContext.spark_session
        .builder
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", args['warehouse_path'])
        .getOrCreate()
)
job = Job(glueContext); job.init(args['JOB_NAME'], args)

# ===================== CONFIG =====================
BUCKET = args['curated_bucket']
L1_PREFIX = args['curated_prefix'].strip('/')  # s3://BUCKET/L1_PREFIX/<table>/

# Limit to specific curated tables (empty = all mapped tables found under curated/)
LIMIT_TABLES = [
    # "company_company", "lpd_loi"
]

# Parallel workers across tables
MAX_WORKERS = 2

# RAW->DIM alias for name differences (per source table; maps curated->DIM column names)
RAW_DIM_ALIAS = {
    # Example:
    # "company_company": {"company_registration_no": "registration_no"},
}

# Optional repartition hint for very large/wide curated tables
REPARTITION_HINT = {
    # "company_company": 8,
}

# Optional: compute BIGINT sig_hash from business columns (False = keep NULL)
COMPUTE_SIG_HASH = False

SENTINEL_TO = "9999-12-31 23:59:59"
STATIC_DIMS = {"dim_date", "dim_status"}  # excluded
EXCLUDE_TABLES = set()
BASE_SCD2_COLS = {"sig_hash", "effective_from", "effective_to", "current_flag", "version"}

# ---- Global ancient datetime protection ----
CLAMP_ANCIENT_DATES_FOR_ALL = True
ANCIENT_DATE = "1582-10-15"
ANCIENT_TS = "1900-01-01 00:00:00"

# ===================== FULL MAPPING (dim -> source) =====================
# Kept identical to create_layer2_dims.py's MAPPING_TEXT — the two scripts
# must agree on dim names and NK resolution. Extend both together.
MAPPING_TEXT = """
dim_accounts_role -> accounts_role
dim_accounts_user -> accounts_user
dim_accounts_user_roles -> accounts_user_roles
dim_audit_auditdgcompliancerequest -> audit_auditdgcompliancerequest
dim_audit_auditdgloicompliancerequest -> audit_auditdgloicompliancerequest
dim_audit_auditdgloicompliancerequestbatch -> audit_auditdgloicompliancerequestbatch
dim_audit_financedginvoicerequest -> audit_financedginvoicerequest
dim_audit_financedginvoicerequestbatch -> audit_financedginvoicerequestbatch
dim_audit_financedglearnerrequest -> audit_financedglearnerrequest
dim_audit_financedglearnerrequestbatch -> audit_financedglearnerrequestbatch
dim_audit_financedgslarequest -> audit_financedgslarequest
dim_audit_financedgslarequestbatch -> audit_financedgslarequestbatch
dim_company_company -> company_company
dim_company_company_supplier_services -> company_company_supplier_services
dim_company_companysuppliercontactperson -> company_companysuppliercontactperson
dim_company_companytype -> company_companytype
dim_company_director -> company_director
dim_company_employerfile -> company_employerfile
dim_company_employerfileoutcome -> company_employerfileoutcome
dim_company_highereductioninstitution -> company_highereductioninstitution
dim_company_hostemployer -> company_hostemployer
dim_company_intersetatransfer -> company_intersetatransfer
dim_company_lpf -> company_lpf
dim_company_lpfdocument -> company_lpfdocument
dim_company_sdf -> company_sdf
dim_company_sdfdocument -> company_sdfdocument
dim_company_sdfextended -> company_sdfextended
dim_company_sdffunction -> company_sdffunction
dim_company_tpf -> company_tpf
dim_company_tpfdocument -> company_tpfdocument
dim_company_trainingcommitteedesignation -> company_trainingcommitteedesignation
dim_company_trainingcommitteerole -> company_trainingcommitteerole
dim_company_trainingcommitteerole_designation -> company_trainingcommitteerole_designation
dim_company_trainingprovider -> company_trainingprovider
dim_company_trainingproviderassessor -> company_trainingproviderassessor
dim_company_trainingprovidercontactperson -> company_trainingprovidercontactperson
dim_company_trainingproviderfirstaider -> company_trainingproviderfirstaider
dim_company_trainingprovidersites -> company_trainingprovidersites
dim_company_trainingprovidertpf -> company_trainingprovidertpf
dim_company_typeofsupplierservice -> company_typeofsupplierservice
dim_configurable_alternateidtype -> configurable_alternateidtype
dim_configurable_city -> configurable_city
dim_configurable_country -> configurable_country
dim_configurable_disability -> configurable_disability
dim_configurable_district -> configurable_district
dim_configurable_economicstatus -> configurable_economicstatus
dim_configurable_employmenteconomicstatus -> configurable_employmenteconomicstatus
dim_configurable_etqa -> configurable_etqa
dim_configurable_gender -> configurable_gender
dim_configurable_language -> configurable_language
dim_configurable_municipality -> configurable_municipality
dim_configurable_nationality -> configurable_nationality
dim_configurable_providerclass -> configurable_providerclass
dim_configurable_providertype -> configurable_providertype
dim_configurable_province -> configurable_province
dim_configurable_race -> configurable_race
dim_configurable_residentialstatus -> configurable_residentialstatus
dim_configurable_seta -> configurable_seta
dim_configurable_structurestatusid -> configurable_structurestatusid
dim_configurable_suburb -> configurable_suburb
dim_configurable_typeofid -> configurable_typeofid
dim_etqa_assessorapplicationcompliancecheck -> etqa_assessorapplicationcompliancecheck
dim_etqa_assessormoderator -> etqa_assessormoderator
dim_etqa_assessormoderator_sub_sector -> etqa_assessormoderator_sub_sector
dim_etqa_assessormoderatorapplication -> etqa_assessormoderatorapplication
dim_etqa_assessormoderatorapplicationsubmission -> etqa_assessormoderatorapplicationsubmission
dim_etqa_assessormoderatordocuments -> etqa_assessormoderatordocuments
dim_etqa_assessormoderatoroccupationalapplication -> etqa_assessormoderatoroccupationalapplication
dim_etqa_assessormoderatoroccupationalrejected -> etqa_assessormoderatoroccupationalrejected
dim_etqa_assessormoderatorqualification -> etqa_assessormoderatorqualification
dim_etqa_assessormoderatorqualificationapplication -> etqa_assessormoderatorqualificationapplication
dim_etqa_assessormoderatorqualificationrejected -> etqa_assessormoderatorqualificationrejected
dim_etqa_assessormoderatorrenewal -> etqa_assessormoderatorrenewal
dim_etqa_assessormoderatorunitstandardapplication -> etqa_assessormoderatorunitstandardapplication
dim_etqa_assessormoderatorunitstandardrejected -> etqa_assessormoderatorunitstandardrejected
dim_etqa_assessorreport -> etqa_assessorreport
dim_etqa_assessorsubmissiontracker -> etqa_assessorsubmissiontracker
dim_etqa_cep -> etqa_cep
dim_etqa_cep_sub_sector -> etqa_cep_sub_sector
dim_etqa_cepapplication -> etqa_cepapplication
dim_etqa_cepapplicationcompliancecheck -> etqa_cepapplicationcompliancecheck
dim_etqa_cepapplicationsubmission -> etqa_cepapplicationsubmission
dim_etqa_cepdocuments -> etqa_cepdocuments
dim_etqa_cepqualification -> etqa_cepqualification
dim_etqa_cepqualificationapplication -> etqa_cepqualificationapplication
dim_etqa_cepqualificationrejected -> etqa_cepqualificationrejected
dim_etqa_cepsubmissiontracker -> etqa_cepsubmissiontracker
dim_etqa_certificationprintschedule -> etqa_certificationprintschedule
dim_etqa_qctoapplication -> etqa_qctoapplication
dim_etqa_qctoapplication_sdps -> etqa_qctoapplication_sdps
dim_etqa_qctoapplicationaccreditationcommitteeapproval -> etqa_qctoapplicationaccreditationcommitteeapproval
dim_etqa_qctoapplicationcontactperson -> etqa_qctoapplicationcontactperson
dim_etqa_qctoapplicationevaluationinstrument -> etqa_qctoapplicationevaluationinstrument
dim_etqa_qctoapplicationfacilitator -> etqa_qctoapplicationfacilitator
dim_etqa_qctoapplicationohsrepresentative -> etqa_qctoapplicationohsrepresentative
dim_etqa_qctoapplicationsitevisit -> etqa_qctoapplicationsitevisit
dim_etqa_qctoapplicationsubmission -> etqa_qctoapplicationsubmission
dim_etqa_qctoapplicationsupportingdocument -> etqa_qctoapplicationsupportingdocument
dim_etqa_qctoapplicationsupportingdocumentcompliancecheck -> etqa_qctoapplicationsupportingdocumentcompliancecheck
dim_etqa_qctoassessmentcenterapplication -> etqa_qctoassessmentcenterapplication
dim_etqa_qctoassessmentcenterapplication_contacts -> etqa_qctoassessmentcenterapplication_contacts
dim_etqa_qctoassessmentcenterapplicationassessor -> etqa_qctoassessmentcenterapplicationassessor
dim_etqa_qctoassessmentcenterapplicationassessor_occupational_qudb1a -> etqa_qctoassessmentcenterapplicationassessor_occupational_qudb1a
dim_etqa_qctoassessmentcenterapplicationcontactperson -> etqa_qctoassessmentcenterapplicationcontactperson
dim_etqa_qctoassessmentcenterapplicationinvigilator -> etqa_qctoassessmentcenterapplicationinvigilator
dim_etqa_qctoassessmentcenterapplicationmoderator -> etqa_qctoassessmentcenterapplicationmoderator
dim_etqa_qctoassessmentcenterapplicationmoderator_occupational_qa51d -> etqa_qctoassessmentcenterapplicationmoderator_occupational_qa51d
dim_etqa_qctoassessmentcenterapplicationohsrepresentative -> etqa_qctoassessmentcenterapplicationohsrepresentative
dim_etqa_qctoassessmentcenterapplicationqualification -> etqa_qctoassessmentcenterapplicationqualification
dim_etqa_qctoassessmentcenterschedule -> etqa_qctoassessmentcenterschedule
dim_etqa_qctoassessmentcentersubmissiontracker -> etqa_qctoassessmentcentersubmissiontracker
dim_etqa_qctooccupationalqualificationsubmissiontracker -> etqa_qctooccupationalqualificationsubmissiontracker
dim_etqa_qctoschedule -> etqa_qctoschedule
dim_etqa_qualificationdevelopment -> etqa_qualificationdevelopment
dim_etqa_qualificationdevelopment_re_aligned_qualifications -> etqa_qualificationdevelopment_re_aligned_qualifications
dim_etqa_qualificationdevelopmentattachment -> etqa_qualificationdevelopmentattachment
dim_etqa_qualificationdevelopmentrelation -> etqa_qualificationdevelopmentrelation
dim_etqa_qualificationdevelopmentsubjectmatterexpert -> etqa_qualificationdevelopmentsubjectmatterexpert
dim_etqa_qualificationdevelopmentsubmissiontracker -> etqa_qualificationdevelopmentsubmissiontracker
dim_etqa_qualificationdevelopmentworkshop -> etqa_qualificationdevelopmentworkshop
dim_etqa_trainingprogrammecompliancecheck -> etqa_trainingprogrammecompliancecheck
dim_etqa_trainingprogrammeexitverificationreportcompliancecheck -> etqa_trainingprogrammeexitverificationreportcompliancecheck
dim_etqa_trainingprogrammeinductionverificationreportcompliancecheck -> etqa_trainingprogrammeinductionverificationreportcompliancecheck
dim_etqa_trainingprogrammeintervalverificationreportcompliancecheck -> etqa_trainingprogrammeintervalverificationreportcompliancecheck
dim_etqa_trainingprogrammelearner -> etqa_trainingprogrammelearner
dim_etqa_trainingprogrammelearnerassessmentfeedback -> etqa_trainingprogrammelearnerassessmentfeedback
dim_etqa_trainingprogrammelearnerattachment -> etqa_trainingprogrammelearnerattachment
dim_etqa_trainingprogrammeunitstandard -> etqa_trainingprogrammeunitstandard
dim_etqa_trainingprogrammeunitstandard_integrations -> etqa_trainingprogrammeunitstandard_integrations
dim_etqa_trainingprogrammeunitstandardsamples -> etqa_trainingprogrammeunitstandardsamples
dim_etqa_trainingproviderapplication -> etqa_trainingproviderapplication
dim_etqa_trainingproviderapplicationaccreditationcommitteeapproval -> etqa_trainingproviderapplicationaccreditationcommitteeapproval
dim_etqa_trainingproviderapplicationaccreditationcommitteemeeting -> etqa_trainingproviderapplicationaccreditationcommitteemeeting
dim_etqa_trainingproviderapplicationadvisorcompliancecheck -> etqa_trainingproviderapplicationadvisorcompliancecheck
dim_etqa_trainingproviderapplicationassessor -> etqa_trainingproviderapplicationassessor
dim_etqa_trainingproviderapplicationassessor_assessor_sites -> etqa_trainingproviderapplicationassessor_assessor_sites
dim_etqa_trainingproviderapplicationassessor_moderator_sites -> etqa_trainingproviderapplicationassessor_moderator_sites
dim_etqa_trainingproviderapplicationattachments -> etqa_trainingproviderapplicationattachments
dim_etqa_trainingproviderapplicationcompliancecheck -> etqa_trainingproviderapplicationcompliancecheck
dim_etqa_trainingproviderapplicationfacilitators -> etqa_trainingproviderapplicationfacilitators
dim_etqa_trainingproviderapplicationsiteinspection -> etqa_trainingproviderapplicationsiteinspection
dim_etqa_trainingproviderapplicationsubmission -> etqa_trainingproviderapplicationsubmission
dim_etqa_trainingproviderprogramme -> etqa_trainingproviderprogramme
dim_etqa_trainingproviderprogrammeadvisorevaluation -> etqa_trainingproviderprogrammeadvisorevaluation
dim_etqa_trainingproviderprogrammelearnergroup -> etqa_trainingproviderprogrammelearnergroup
dim_etqa_trainingproviderprogrammelearnergroup_facilitators -> etqa_trainingproviderprogrammelearnergroup_facilitators
dim_etqa_trainingproviderprogrammelearnergroup_sites -> etqa_trainingproviderprogrammelearnergroup_sites
dim_etqa_trainingproviderprogrammelearnergroupassessormoderator -> etqa_trainingproviderprogrammelearnergroupassessormoderator
dim_etqa_trainingproviderprogrammelearnerreprint -> etqa_trainingproviderprogrammelearnerreprint
dim_etqa_trainingproviderprogrammemoderatorexitcompliancecheck -> etqa_trainingproviderprogrammemoderatorexitcompliancecheck
dim_etqa_trainingproviderprogrammemoderatorintervalcompliancecheck -> etqa_trainingproviderprogrammemoderatorintervalcompliancecheck
dim_etqa_trainingproviderprogrammespecialization -> etqa_trainingproviderprogrammespecialization
dim_etqa_trainingproviderprogrammesubmissiontracker -> etqa_trainingproviderprogrammesubmissiontracker
dim_etqa_trainingproviderprogrammesupportingdocuments -> etqa_trainingproviderprogrammesupportingdocuments
dim_etqa_trainingproviderqualificationapplication -> etqa_trainingproviderqualificationapplication
dim_etqa_trainingproviderqualificationapplication_assessors -> etqa_trainingproviderqualificationapplication_assessors
dim_etqa_trainingproviderqualificationapplication_facilitators -> etqa_trainingproviderqualificationapplication_facilitators
dim_etqa_trainingproviderqualificationapplication_moderators -> etqa_trainingproviderqualificationapplication_moderators
dim_etqa_trainingproviderqualificationelective -> etqa_trainingproviderqualificationelective
dim_etqa_trainingproviderqualificationsampledocuments -> etqa_trainingproviderqualificationsampledocuments
dim_etqa_trainingproviderqualificationspecializationelective -> etqa_trainingproviderqualificationspecializationelective
dim_etqa_trainingproviderqualificationspecializationelective_comd607 -> etqa_trainingproviderqualificationspecializationelective_comd607
dim_etqa_trainingproviderqualificationspecializationelective_supa612 -> etqa_trainingproviderqualificationspecializationelective_supa612
dim_etqa_trainingprovidersubmissiontracker -> etqa_trainingprovidersubmissiontracker
dim_etqa_trainingproviderunitstandardapplication -> etqa_trainingproviderunitstandardapplication
dim_finance_adminpaymentbatchschedule -> finance_adminpaymentbatchschedule
dim_finance_adminpaymentbatchscheduleapproval -> finance_adminpaymentbatchscheduleapproval
dim_finance_dgfinancereport -> finance_dgfinancereport
dim_finance_paymentbatchschedule -> finance_paymentbatchschedule
dim_finance_schemeyear -> finance_schemeyear
dim_finance_schemeyearlevycreditor -> finance_schemeyearlevycreditor
dim_finance_schemeyearmgpaymentcycle -> finance_schemeyearmgpaymentcycle
dim_finance_schemeyearmgpaymentcyclemonth -> finance_schemeyearmgpaymentcyclemonth
dim_finance_schemeyearmgpaymentcyclemonthwsplevies -> finance_schemeyearmgpaymentcyclemonthwsplevies
dim_finance_schemeyearmgpaymentcyclepaymentlist -> finance_schemeyearmgpaymentcyclepaymentlist
dim_finance_schemeyearmonthfile -> finance_schemeyearmonthfile
dim_finance_schemeyearmonthfiledata -> finance_schemeyearmonthfiledata
dim_finance_schemeyearmonthfilesubmissiontracker -> finance_schemeyearmonthfilesubmissiontracker
dim_finance_suppliercontract -> finance_suppliercontract
dim_finance_suppliercontractinvoice -> finance_suppliercontractinvoice
dim_finance_suppliercontractinvoiceapproval -> finance_suppliercontractinvoiceapproval
dim_finance_suppliercontractinvoicesubmissiontracker -> finance_suppliercontractinvoicesubmissiontracker
dim_finance_supplierlist -> finance_supplierlist
dim_finance_uploadmonthyear -> finance_uploadmonthyear
dim_fourir_advisorycommitteemeetingfile -> fourir_advisorycommitteemeetingfile
dim_fourir_advisorycommitteemeetingrsvp -> fourir_advisorycommitteemeetingrsvp
dim_fourir_advisorycommitteemember -> fourir_advisorycommitteemember
dim_fourir_advisorycommitteeservice -> fourir_advisorycommitteeservice
dim_fourir_advisorycommitteestream -> fourir_advisorycommitteestream
dim_fourir_advisorycommitteestreammeeting -> fourir_advisorycommitteestreammeeting
dim_fourir_researchchair -> fourir_researchchair
dim_fourir_researchchairproject -> fourir_researchchairproject
dim_fourir_researchchairprojectsla -> fourir_researchchairprojectsla
dim_fourir_strategicpartnercontact -> fourir_strategicpartnercontact
dim_learner_learner -> learner_learner
dim_learner_learnergrievance -> learner_learnergrievance
dim_learner_learnerparent -> learner_learnerparent
dim_learner_level -> learner_level
dim_learner_schoolcodes -> learner_schoolcodes
dim_lpd_advisordgreport -> lpd_advisordgreport
dim_lpd_dgcommitteemeeting -> lpd_dgcommitteemeeting
dim_lpd_dgwindow -> lpd_dgwindow
dim_lpd_dgwindowindicatorgrant -> lpd_dgwindowindicatorgrant
dim_lpd_disbursementdgreport -> lpd_disbursementdgreport
dim_lpd_disbursementsubmissiontracker -> lpd_disbursementsubmissiontracker
dim_lpd_employertpdgreport -> lpd_employertpdgreport
dim_lpd_financialyearbudgetreport -> lpd_financialyearbudgetreport
dim_lpd_financialyeardirectorsreport -> lpd_financialyeardirectorsreport
dim_lpd_interventiondgreport -> lpd_interventiondgreport
dim_lpd_learnerbulkuploadforms -> lpd_learnerbulkuploadforms
dim_lpd_learnerbulkuploadoutcome -> lpd_learnerbulkuploadoutcome
dim_lpd_learnerprogramme -> lpd_learnerprogramme
dim_lpd_learnerprogrammeattachment -> lpd_learnerprogrammeattachment
dim_lpd_learnerprogrammeattachmentanalyzercheck -> lpd_learnerprogrammeattachmentanalyzercheck
dim_lpd_learnerprogrammeattachmentcompliancecheck -> lpd_learnerprogrammeattachmentcompliancecheck
dim_lpd_learnerprogrammeplacement -> lpd_learnerprogrammeplacement
dim_lpd_learnerprogrammeproofofpayment -> lpd_learnerprogrammeproofofpayment
dim_lpd_learnerprogrammereplacement -> lpd_learnerprogrammereplacement
dim_lpd_learnerprogrammetermination -> lpd_learnerprogrammetermination
dim_lpd_learnerprogrammeverification -> lpd_learnerprogrammeverification
dim_lpd_learnerqmrdgreport -> lpd_learnerqmrdgreport
dim_lpd_learningprogramme -> lpd_learningprogramme
dim_lpd_learningprogrammeaddendum -> lpd_learningprogrammeaddendum
dim_lpd_learningprogrammecommitmentregister -> lpd_learningprogrammecommitmentregister
dim_lpd_learningprogrammedgreport -> lpd_learningprogrammedgreport
dim_lpd_learningprogrammedisbursement -> lpd_learningprogrammedisbursement
dim_lpd_learningprogrammedisbursementapproval -> lpd_learningprogrammedisbursementapproval
dim_lpd_learningprogrammedisbursementrequirement -> lpd_learningprogrammedisbursementrequirement
dim_lpd_learningprogrammesecondarylpf -> lpd_learningprogrammesecondarylpf
dim_lpd_learningprogrammesla -> lpd_learningprogrammesla
dim_lpd_learningprogrammeslageneration -> lpd_learningprogrammeslageneration
dim_lpd_learningprogrammeslagenerationsla -> lpd_learningprogrammeslagenerationsla
dim_lpd_learningprogrammeslalearnerlistrequest -> lpd_learningprogrammeslalearnerlistrequest
dim_lpd_learningprogrammetrainingprovider -> lpd_learningprogrammetrainingprovider
dim_lpd_learningprogrammewriteback -> lpd_learningprogrammewriteback
dim_lpd_loi -> lpd_loi
dim_lpd_loichecklist -> lpd_loichecklist
dim_lpd_loicompliancecheck -> lpd_loicompliancecheck
dim_lpd_loidelinkedlpf -> lpd_loidelinkedlpf
dim_lpd_loidginterventiontracker -> lpd_loidginterventiontracker
dim_lpd_loidgreport -> lpd_loidgreport
dim_lpd_loiemailreminder -> lpd_loiemailreminder
dim_lpd_loiintervention -> lpd_loiintervention
dim_lpd_loiinterventionappeal -> lpd_loiinterventionappeal
dim_lpd_loiinterventioncontinuingstudents -> lpd_loiinterventioncontinuingstudents
dim_lpd_loiinterventionrequest -> lpd_loiinterventionrequest
dim_lpd_loiinterventionsignatories -> lpd_loiinterventionsignatories
dim_lpd_loiinterventiontrackindicator -> lpd_loiinterventiontrackindicator
dim_lpd_loisladetails -> lpd_loisladetails
dim_lpd_loislatemplatesection -> lpd_loislatemplatesection
dim_lpd_loisubmissiontracker -> lpd_loisubmissiontracker
dim_lpd_loivetting -> lpd_loivetting
dim_lpd_loivettingapproval -> lpd_loivettingapproval
dim_lpd_loivettingquery -> lpd_loivettingquery
dim_lpd_loivettingrisk -> lpd_loivettingrisk
dim_lpd_loivettingtracker -> lpd_loivettingtracker
dim_lpd_nonfundedchecklist -> lpd_nonfundedchecklist
dim_lpd_nonfundedcompliancecheck -> lpd_nonfundedcompliancecheck
dim_lpd_nonfundedlearnerprogramme -> lpd_nonfundedlearnerprogramme
dim_lpd_nonfundedlearnerprogrammeattachment -> lpd_nonfundedlearnerprogrammeattachment
dim_lpd_nonfundedmoulettergeneration -> lpd_nonfundedmoulettergeneration
dim_lpd_nonfundedprogramme -> lpd_nonfundedprogramme
dim_lpd_nonfundedprogrammetrainingprovider -> lpd_nonfundedprogrammetrainingprovider
dim_lpd_nonfundedsubmissiontracker -> lpd_nonfundedsubmissiontracker
dim_lpd_quaterlysitevisit -> lpd_quaterlysitevisit
dim_lpd_quaterlysitevisitattachment -> lpd_quaterlysitevisitattachment
dim_lpd_setmisfinancialyearreport -> lpd_setmisfinancialyearreport
dim_lpd_sladgreport -> lpd_sladgreport
dim_lpd_vettingdgreport -> lpd_vettingdgreport
dim_lpd_vettingrisk -> lpd_vettingrisk
dim_src_boarddesignation -> src_boarddesignation
dim_src_boardmeeting -> src_boardmeeting
dim_src_boardmeetingtype -> src_boardmeetingtype
dim_src_boardmember -> src_boardmember
dim_src_boardmemberservice -> src_boardmemberservice
dim_src_boardmemberserviceclaim -> src_boardmemberserviceclaim
dim_src_department -> src_department
dim_src_departmentalmeeting -> src_departmentalmeeting
dim_src_descretionarygrant -> src_descretionarygrant
dim_src_descretionarygrant_indicators -> src_descretionarygrant_indicators
dim_src_discretionarygrantdisbursement -> src_discretionarygrantdisbursement
dim_src_discretionarygrantdisbursementrequirement -> src_discretionarygrantdisbursementrequirement
dim_src_discretionarygrantdisbursementrequirementsla -> src_discretionarygrantdisbursementrequirementsla
dim_src_division -> src_division
dim_src_financialyear -> src_financialyear
dim_src_indicator -> src_indicator
dim_src_indicatoradmins -> src_indicatoradmins
dim_src_srcstaff -> src_srcstaff
dim_src_srcstaff_divisions -> src_srcstaff_divisions
dim_src_srcstaff_regions -> src_srcstaff_regions
dim_src_nqflevel -> src_nqflevel
dim_src_occupationalqualification -> src_occupationalqualification
dim_src_ofomajorgroup -> src_ofomajorgroup
dim_src_ofominorgroup -> src_ofominorgroup
dim_src_ofooccupation -> src_ofooccupation
dim_src_ofospecialization -> src_ofospecialization
dim_src_ofosubmajorgroup -> src_ofosubmajorgroup
dim_src_ofotask -> src_ofotask
dim_src_ofounitgroup -> src_ofounitgroup
dim_src_qualification -> src_qualification
dim_src_qualificationspecialization -> src_qualificationspecialization
dim_src_qualificationunitstandard -> src_qualificationunitstandard
dim_src_qualificationunitstandard_specializations -> src_qualificationunitstandard_specializations
dim_src_qualificationunitstandard_supplementary -> src_qualificationunitstandard_supplementary
dim_src_query -> src_query
dim_src_queryresponse -> src_queryresponse
dim_src_regionaloffice -> src_regionaloffice
dim_src_regionaloffice_provinces -> src_regionaloffice_provinces
dim_src_signatory -> src_signatory
dim_src_smequalification -> src_smequalification
dim_src_subsector -> src_subsector
dim_src_subsectoractivity -> src_subsectoractivity
dim_src_unitstandard -> src_unitstandard
dim_ssp_approvedwspsubmission -> ssp_approvedwspsubmission
dim_ssp_bulkuploadtemplates -> ssp_bulkuploadtemplates
dim_ssp_companyprevioussdf -> ssp_companyprevioussdf
dim_ssp_companysecondarysdf -> ssp_companysecondarysdf
dim_ssp_companysubsidiaries -> ssp_companysubsidiaries
dim_ssp_companywspapprovaluser -> ssp_companywspapprovaluser
dim_ssp_companywspbulkuploadforms -> ssp_companywspbulkuploadforms
dim_ssp_companywspsubmission -> ssp_companywspsubmission
dim_ssp_sdflinkedcompanytracker -> ssp_sdflinkedcompanytracker
dim_ssp_sdfregistrationtracker -> ssp_sdfregistrationtracker
dim_ssp_trainingcommittee -> ssp_trainingcommittee
dim_ssp_wspactualtraining -> ssp_wspactualtraining
dim_ssp_wspannualpayroll -> ssp_wspannualpayroll
dim_ssp_wspbulkuploadoutcome -> ssp_wspbulkuploadoutcome
dim_ssp_wspcriticalskills -> ssp_wspcriticalskills
dim_ssp_wspemploymentprofile -> ssp_wspemploymentprofile
dim_ssp_wspimpactanswer -> ssp_wspimpactanswer
dim_ssp_wspimpactsurveyanswer -> ssp_wspimpactsurveyanswer
dim_ssp_wsporganizationanswer -> ssp_wsporganizationanswer
dim_ssp_wsporganizationsurveyanswer -> ssp_wsporganizationsurveyanswer
dim_ssp_wspperiod -> ssp_wspperiod
dim_ssp_wspperiodreport -> ssp_wspperiodreport
dim_ssp_wspplannedtraining -> ssp_wspplannedtraining
dim_ssp_wspprogrammetype -> ssp_wspprogrammetype
dim_ssp_wspptpactualtraining -> ssp_wspptpactualtraining
dim_ssp_wspptpplannedtraining -> ssp_wspptpplannedtraining
dim_ssp_wspquestion -> ssp_wspquestion
dim_ssp_wspscarceskills -> ssp_wspscarceskills
dim_ssp_wspshortage -> ssp_wspshortage
dim_ssp_wspshortagereason -> ssp_wspshortagereason
dim_ssp_wspstructuralscarcity -> ssp_wspstructuralscarcity
dim_ssp_wspsubmissionaudittracker -> ssp_wspsubmissionaudittracker
dim_ssp_wspsubmissionproofoftraining -> ssp_wspsubmissionproofoftraining
dim_ssp_wspsubmissiontracker -> ssp_wspsubmissiontracker
dim_ssp_wspsurvey -> ssp_wspsurvey
dim_ssp_wsptrainingbudget -> ssp_wsptrainingbudget
dim_ssp_wsptrainingbudgetspent -> ssp_wsptrainingbudgetspent
dim_ssp_wsptrainingbudgetspent_descrtionary_grant_type -> ssp_wsptrainingbudgetspent_descrtionary_grant_type
dim_ssp_wsptrainingvariance -> ssp_wsptrainingvariance
dim_ssp_wspvariancereason -> ssp_wspvariancereason
"""

# ===================== Helpers =====================
def _parse_mapping(text: str):
    d = {}
    for line in text.strip().splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "->" not in s:
            continue
        dim, src = [p.strip() for p in s.split("->", 1)]
        if dim in STATIC_DIMS:
            continue
        d[dim] = src
    return d

DIM_TO_SRC = _parse_mapping(MAPPING_TEXT)
SRC_TO_DIM = {v: k for k, v in DIM_TO_SRC.items()}

# ---- Session settings (rebase guards) ----
spark.sql(f"USE {CATALOG}.{DB}")
spark.sql("set spark.sql.session.timeZone=UTC")
spark.sql("set spark.sql.sources.partitionOverwriteMode=dynamic")
spark.sql("set spark.sql.adaptive.enabled=true")
spark.sql("set spark.sql.adaptive.coalescePartitions.enabled=true")
spark.sql("set spark.sql.files.ignoreMissingFiles=true")
spark.sql("set spark.sql.parquet.mergeSchema=false")
DEFAULT_SHUFFLE_PARTS = 600
spark.sql(f"set spark.sql.shuffle.partitions={DEFAULT_SHUFFLE_PARTS}")

# Spark 3 calendar rebase guards (READ+WRITE, incl INT96)
for k, v in [
    ("spark.sql.parquet.datetimeRebaseModeInRead",  "LEGACY"),
    ("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY"),
    ("spark.sql.parquet.int96RebaseModeInRead",     "LEGACY"),
    ("spark.sql.parquet.int96RebaseModeInWrite",    "LEGACY"),
    ("spark.sql.legacy.parquet.datetimeRebaseModeInRead",  "LEGACY"),
    ("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY"),
]:
    try:
        spark.conf.set(k, v)
    except Exception:
        pass

try:
    spark.conf.set("spark.network.timeout", "600s")
    spark.conf.set("spark.executor.heartbeatInterval", "30s")
    spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")
    spark.conf.set("spark.memory.fraction", "0.6")
    spark.conf.set("spark.memory.storageFraction", "0.3")
    spark.conf.set("spark.executor.memoryOverhead", "4096")
except Exception:
    pass

S3 = boto3.client("s3")


def list_tables_in_curated(bucket: str, curated_prefix: str):
    """Return the list of immediate child folders under curated/"""
    prefix = curated_prefix.strip("/") + "/"
    paginator = S3.get_paginator('list_objects_v2')
    tables = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            tables.append(cp["Prefix"].split("/")[-2])
    return tables


def bt(name: str) -> str:
    return f"`{name}`"


def read_dim_df(dim_name: str) -> DataFrame:
    return spark.table(f"{CATALOG}.{DB}.{dim_name}")


def resolve_nk(target_cols, source_table):
    if "id" in target_cols:
        return "id"
    alt = f"{source_table}_id"
    if alt in target_cols:
        return alt
    return None


def type_sql(dt: DataType) -> str:
    return dt.simpleString()

# ---- Clamp ancient Date/Timestamp values to NULL ----
def clamp_ancient_dt(df: DataFrame) -> DataFrame:
    if not CLAMP_ANCIENT_DATES_FOR_ALL:
        return df
    cutoff_date = F.to_date(F.lit(ANCIENT_DATE))
    cutoff_ts   = F.to_timestamp(F.lit(ANCIENT_TS))
    out = df
    for f in df.schema.fields:
        if isinstance(f.dataType, DateType):
            out = out.withColumn(f.name, F.when(F.col(f.name) < cutoff_date, F.lit(None).cast(DateType())).otherwise(F.col(f.name)))
        elif isinstance(f.dataType, TimestampType):
            out = out.withColumn(f.name, F.when(F.col(f.name) < cutoff_ts, F.lit(None).cast(TimestampType())).otherwise(F.col(f.name)))
    return out


def stage_df_from_raw(raw_df: DataFrame, dim_schema: StructType, source_table: str):
    target_cols = [f.name for f in dim_schema.fields]
    target_types = {f.name: f.dataType for f in dim_schema.fields}
    nk = resolve_nk(target_cols, source_table)

    exclude = set(BASE_SCD2_COLS) | {"surrogate_key"}
    sk = f"{source_table}_sk"
    if sk in target_cols: exclude.add(sk)
    business_cols = [c for c in target_cols if c not in exclude and c != nk]

    alias_map = RAW_DIM_ALIAS.get(source_table, {})  # curated->DIM map

    df = raw_df
    rp = REPARTITION_HINT.get(source_table)
    if rp and df.rdd.getNumPartitions() < rp:
        df = df.repartition(int(rp))

    # NK
    if nk and nk in df.columns:
        df = df.withColumn(nk, F.col(nk).cast(target_types[nk]))
    elif nk and "id" in df.columns:
        df = df.withColumn(nk, F.col("id").cast(target_types[nk]))
    elif nk:
        df = df.withColumn(nk, F.lit(None).cast(target_types[nk]))

    # Business columns (only those present in dim)
    for dim_col in business_cols:
        raw_col = None
        for rc, dc in alias_map.items():
            if dc == dim_col:
                raw_col = rc; break
        if raw_col is None:
            raw_col = dim_col  # same name case
        if raw_col in df.columns:
            df = df.withColumn(dim_col, F.col(raw_col).cast(target_types[dim_col]))
        else:
            df = df.withColumn(dim_col, F.lit(None).cast(target_types[dim_col]))

    if nk is None:
        return df.select(business_cols), nk, business_cols, target_cols, target_types

    return df.select([nk] + business_cols), nk, business_cols, target_cols, target_types


def nullsafe_equal_predicate(cols, left_alias="cur", right_alias="s"):
    if not cols:
        return "TRUE"
    return " AND ".join([f"{left_alias}.{bt(c)} <=> {right_alias}.{bt(c)}" for c in cols])


def build_sig_hash_expr(business_cols):
    if not COMPUTE_SIG_HASH or not business_cols:
        return "CAST(NULL AS BIGINT)"
    parts = ", ".join([f"CAST(s.{bt(c)} AS STRING)" for c in business_cols])
    return f"CAST(xxhash64({parts}) AS BIGINT)"

# ---- Integrity fixer: ensure one current row per NK (Spark/Iceberg-friendly MERGE) ----
def integrity_fix_one_current(dim_name: str, nk_col: str):
    try:
        spark.sql(f"""
          CREATE OR REPLACE TEMP VIEW d_{dim_name} AS
          SELECT {bt(nk_col)}, version
          FROM (
            SELECT {bt(nk_col)},
                   version,
                   ROW_NUMBER() OVER (PARTITION BY {bt(nk_col)} ORDER BY version DESC) AS rn
            FROM {CATALOG}.{DB}.{dim_name}
            WHERE current_flag = TRUE
          ) x
          WHERE rn > 1
        """)
        spark.sql(f"""
          MERGE INTO {CATALOG}.{DB}.{dim_name} AS t
          USING d_{dim_name} AS d
          ON t.{bt(nk_col)} = d.{bt(nk_col)} AND t.version = d.version
          WHEN MATCHED THEN UPDATE SET t.current_flag = FALSE
        """)
    except Exception as e:
        print(f"[{dim_name}] integrity fix skipped/failed: {e}")


def process_table(source_table: str, load_ts_str: str):
    if source_table in EXCLUDE_TABLES:
        print(f"[{source_table}] SKIP: excluded")
        return (source_table, 0, "excluded")

    dim_name = SRC_TO_DIM.get(source_table)
    if not dim_name:
        print(f"[{source_table}] SKIP: not mapped"); return (source_table, 0, "not_mapped")

    raw_path = f"s3://{BUCKET}/{L1_PREFIX}/{source_table}/"
    try:
        raw_df = (
            spark.read
                 .option("datetimeRebaseMode", "LEGACY")
                 .option("int96RebaseMode", "LEGACY")
                 .parquet(raw_path)
        )
    except Exception as e:
        print(f"[{source_table}] SKIP read fail: {e}")
        return (source_table, 0, "read_fail")

    # Clamp ancient datetimes BEFORE staging
    raw_df = clamp_ancient_dt(raw_df)

    try:
        dim_df = read_dim_df(dim_name)
    except Exception as e:
        print(f"[{source_table}] ERROR: dim {dim_name} missing: {e}")
        return (source_table, 0, "dim_missing")

    dim_schema = dim_df.schema

    try:
        stg_df, nk_col, business_cols, target_cols, target_types = stage_df_from_raw(raw_df, dim_schema, source_table)
    except Exception as e:
        print(f"[{source_table}] ERROR staging: {e}")
        return (source_table, 0, "staging_error")

    if nk_col is None:
        print(f"[{source_table}] ERROR: Could not determine NK (no 'id' or '{source_table}_id').")
        return (source_table, 0, "no_nk")

    # Views
    stg_view = f"stg__{source_table}"
    cur_view = f"cur__{source_table}"
    stg_df.createOrReplaceTempView(stg_view)
    dim_df.filter(F.col("current_flag") == True).select([nk_col] + business_cols + ["version"]) \
          .createOrReplaceTempView(cur_view)

    stg_cols_actual = set(spark.table(stg_view).columns)
    required_cols = {nk_col, *business_cols}
    missing = [c for c in required_cols if c not in stg_cols_actual]
    if missing:
        print(f"[{source_table}] ERROR: staging view missing columns {missing}")
        return (source_table, 0, "stg_missing_cols")

    eq_pred = nullsafe_equal_predicate(business_cols)
    has_sk = f"{source_table}_sk" in target_cols
    sk_col = f"{source_table}_sk" if has_sk else None
    start_sk = 0
    if has_sk:
        try:
            start_sk = dim_df.select(F.max(F.col(sk_col)).alias("m")).collect()[0]["m"] or 0
        except Exception:
            start_sk = 0

    # ================= MERGE #1: CLOSE CHANGED =================
    close_changed_sql = f"""
      MERGE INTO {CATALOG}.{DB}.{dim_name} AS tgt
      USING (
        SELECT cur.{bt(nk_col)} AS {bt(nk_col)}
        FROM {cur_view} cur
        JOIN {stg_view} s ON s.{bt(nk_col)} = cur.{bt(nk_col)}
        WHERE NOT ({eq_pred})
      ) diff
      ON  tgt.{bt(nk_col)} = diff.{bt(nk_col)} AND tgt.current_flag = TRUE
      WHEN MATCHED THEN UPDATE SET
        tgt.current_flag = FALSE,
        tgt.effective_to = to_timestamp('{load_ts_str}')
    """
    try:
        spark.sql(close_changed_sql)
    except Exception as e:
        print(f"[{source_table}] ERROR MERGE#1 (close changed): {e}")
        return (source_table, 0, "merge_close_error")

    # ================= Close DELETIONS (anti-join) =================
    close_deleted_sql = f"""
      UPDATE {CATALOG}.{DB}.{dim_name} AS tgt
      SET tgt.current_flag = FALSE,
          tgt.effective_to = to_timestamp('{load_ts_str}')
      WHERE tgt.current_flag = TRUE
        AND NOT EXISTS (SELECT 1 FROM {stg_view} s WHERE s.{bt(nk_col)} = tgt.{bt(nk_col)})
    """
    try:
        spark.sql(close_deleted_sql)
    except Exception as e:
        print(f"[{source_table}] ERROR closing deleted: {e}")
        return (source_table, 0, "close_deleted_error")

    # Integrity guard: ensure single current per NK
    integrity_fix_one_current(dim_name, nk_col)

    # ================= MERGE #2: INSERT NEW & CHANGED =================
    target_list_sql = ", ".join([bt(c) for c in target_cols])

    def col_select_sql(col_name: str) -> str:
        if col_name == nk_col:
            return f"CAST(`{nk_col}` AS {type_sql(target_types[nk_col])}) AS {bt(nk_col)}"
        if col_name in business_cols:
            return f"`{col_name}` AS {bt(col_name)}"
        if col_name == "sig_hash":
            if COMPUTE_SIG_HASH and business_cols:
                parts = ", ".join([f"CAST(`{c}` AS STRING)" for c in business_cols])
                return f"CAST(xxhash64({parts}) AS BIGINT) AS `sig_hash`"
            else:
                return "CAST(NULL AS BIGINT) AS `sig_hash`"
        if col_name == "effective_from":
            return f"to_timestamp('{load_ts_str}') AS `effective_from`"
        if col_name == "effective_to":
            return f"to_timestamp('{SENTINEL_TO}') AS `effective_to`"
        if col_name == "current_flag":
            return "TRUE AS `current_flag`"
        if col_name == "version":
            return "COALESCE(max_version, 0) + 1 AS `version`"
        if has_sk and col_name == sk_col:
            return f"CAST({start_sk} + __rn AS BIGINT) AS `{sk_col}`"
        return f"CAST(NULL AS {type_sql(target_types[col_name])}) AS `{col_name}`"

    select_payload_sql = ",\n          ".join([col_select_sql(c) for c in target_cols])

    sel_base_cols = ", ".join([f"s.{bt(nk_col)} AS {bt(nk_col)}"] + [f"s.{bt(c)} AS {bt(c)}" for c in business_cols])
    sel_base_sql = f"""
      WITH sel_base AS (
        SELECT
          {sel_base_cols},
          h.max_version,
          ROW_NUMBER() OVER (ORDER BY s.{bt(nk_col)}) AS __rn
        FROM {stg_view} s
        LEFT JOIN (
          SELECT {bt(nk_col)}, MAX(version) AS max_version
          FROM {CATALOG}.{DB}.{dim_name}
          GROUP BY {bt(nk_col)}
        ) h ON h.{bt(nk_col)} = s.{bt(nk_col)}
        LEFT JOIN {cur_view} cur ON cur.{bt(nk_col)} = s.{bt(nk_col)}
        WHERE cur.{bt(nk_col)} IS NULL OR NOT ({eq_pred})
      ),
      ins AS (
        SELECT
          {select_payload_sql}
        FROM sel_base
      )
      MERGE INTO {CATALOG}.{DB}.{dim_name} AS t
      USING ins
      ON 1 = 0
      WHEN NOT MATCHED THEN INSERT ({target_list_sql})
      VALUES ({", ".join([f"ins.{bt(c)}" for c in target_cols])})
    """
    try:
        spark.sql(sel_base_sql)
    except Exception as e:
        print(f"[{source_table}] ERROR MERGE#2 (insert new/changed): {e}\n--- MERGE#2 SQL ---\n{sel_base_sql}\n-------------------")
        return (source_table, 0, "merge_insert_error")

    # Final integrity pass (cheap)
    integrity_fix_one_current(dim_name, nk_col)

    print(f"[{source_table}] -> [{dim_name}] DONE")
    return (source_table, -1, "ok")

# ===================== Orchestration =====================
def main():
    # Runtime load timestamp (UTC)
    load_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    load_ts_str = load_ts.isoformat(sep=' ')

    print(f"Processing curated root: s3://{BUCKET}/{L1_PREFIX}")
    print(f"Mapped dims: {len(DIM_TO_SRC)} | sources: {len(SRC_TO_DIM)}")
    print(f"Target Iceberg: {CATALOG}.{DB} (warehouse: {args['warehouse_path']})")
    print(f"Load timestamp (UTC): {load_ts_str}")

    curated_tables = list_tables_in_curated(BUCKET, L1_PREFIX)
    tables = [t for t in curated_tables if t in SRC_TO_DIM]
    if LIMIT_TABLES:
        tables = [t for t in tables if t in LIMIT_TABLES]
    if not tables:
        print("No eligible mapped tables under curated/."); return

    print(f"Tables to process ({len(tables)}): {', '.join(sorted(tables))}")

    results = []
    max_workers = max(1, min(int(MAX_WORKERS), len(tables)))
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futs = [exe.submit(process_table, t, load_ts_str) for t in tables]
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                res = fut.result(); results.append(res)
                print(f"[{i}/{len(tables)}] {res}")
            except Exception as e:
                print(f"[{i}/{len(tables)}] worker failure: {e}\n{traceback.format_exc()}")

    ok = sum(1 for _, _, s in results if s == "ok")
    print(f"=== Run complete: {ok}/{len(tables)} dims updated ===")

if __name__ == "__main__":
    main()
    job.commit()
