# create_layer2_dims.py
#
# Bootstrap job: creates the Iceberg dim_<table> tables that
# build_layer2_dims_scd2.py merges into, if they don't already exist.
#
# For each mapped source table, reads the schema written by move_raw_tables.py
# under curated/<table>/, and issues CREATE TABLE IF NOT EXISTS ... USING iceberg
# with the source's business columns plus the fixed SCD2 columns
# (sig_hash, effective_from, effective_to, current_flag, version) and a
# per-table surrogate key (<table>_sk).
#
# Idempotent by design (IF NOT EXISTS) so it's safe to re-run, e.g. after adding
# a new table to MAPPING_TEXT. It does NOT alter existing tables — schema
# evolution for already-created dims is out of scope; drop and recreate (or
# hand-run an ALTER TABLE) if a dim's business columns need to change.
#
# Run manually, once, before the nightly SCD2 merge job is first pointed at a
# new table.

import sys, traceback

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import DataType

# ===================== Glue / Spark bootstrap =====================
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'curated_bucket',
    'curated_prefix',
    'dims_database',
    'dims_catalog_name',
    'warehouse_path',
])
sc = SparkContext()
glueContext = GlueContext(sc)

CATALOG = args['dims_catalog_name']
DB = args['dims_database']

spark = (
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

BUCKET = args['curated_bucket']
CURATED_PREFIX = args['curated_prefix'].strip('/')

BASE_SCD2_COLS = {"sig_hash", "effective_from", "effective_to", "current_flag", "version"}
STATIC_DIMS = {"dim_date", "dim_status"}  # pre-existing conformed dims, not derived from source tables

# ===================== FULL MAPPING (dim -> source) =====================
# Kept identical to build_layer2_dims_scd2.py's MAPPING_TEXT — the two scripts
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


def bt(name: str) -> str:
    return f"`{name}`"


def type_sql(dt: DataType) -> str:
    return dt.simpleString()


def resolve_nk(columns, source_table):
    if "id" in columns:
        return "id"
    alt = f"{source_table}_id"
    if alt in columns:
        return alt
    return None


def create_dim_table(dim_name: str, source_table: str):
    curated_path = f"s3://{BUCKET}/{CURATED_PREFIX}/{source_table}/"
    try:
        schema = spark.read.parquet(curated_path).schema
    except Exception as e:
        print(f"[{source_table}] SKIP: cannot read curated schema at {curated_path}: {e}")
        return (source_table, "read_fail")

    columns = [f.name for f in schema.fields]
    nk = resolve_nk(columns, source_table)
    if nk is None:
        print(f"[{source_table}] SKIP: no 'id' or '{source_table}_id' column found")
        return (source_table, "no_nk")

    business_cols = [f for f in schema.fields if f.name != nk]
    sk_col = f"{source_table}_sk"

    col_defs = [f"{bt(nk)} {type_sql(schema[nk].dataType)}"]
    col_defs += [f"{bt(f.name)} {type_sql(f.dataType)}" for f in business_cols]
    col_defs += [
        f"{bt(sk_col)} BIGINT",
        f"{bt('sig_hash')} BIGINT",
        f"{bt('effective_from')} TIMESTAMP",
        f"{bt('effective_to')} TIMESTAMP",
        f"{bt('current_flag')} BOOLEAN",
        f"{bt('version')} BIGINT",
    ]

    col_defs_sql = ",\n        ".join(col_defs)
    ddl = f"""
      CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.{dim_name} (
        {col_defs_sql}
      ) USING iceberg
      TBLPROPERTIES ('format-version' = '2')
    """
    try:
        spark.sql(ddl)
        print(f"[{source_table}] -> [{dim_name}] ensured ({len(col_defs)} cols, nk={nk})")
        return (source_table, "ok")
    except Exception as e:
        print(f"[{source_table}] ERROR creating {dim_name}: {e}\n--- DDL ---\n{ddl}\n-----------")
        return (source_table, "create_error")


def main():
    # The database itself is created by Terraform (aws_glue_catalog_database.layer2).
    results = []
    for dim_name, source_table in DIM_TO_SRC.items():
        try:
            results.append(create_dim_table(dim_name, source_table))
        except Exception as e:
            print(f"[{source_table}] worker failure: {e}")
            traceback.print_exc()
            results.append((source_table, "worker_error"))

    ok = sum(1 for _, s in results if s == "ok")
    print(f"=== Bootstrap complete: {ok}/{len(results)} dims ensured ===")


if __name__ == "__main__":
    main()
    job.commit()
