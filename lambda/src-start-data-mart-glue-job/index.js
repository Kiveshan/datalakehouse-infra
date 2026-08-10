const {
  GlueClient,
  StartJobRunCommand,
} = require("@aws-sdk/client-glue");

const glue = new GlueClient({});

// Generic: jobName is passed in by the Step Function, e.g. any of
// aws_glue_job.move_raw_tables / create_layer2_dims / build_layer2_dims_scd2 /
// build_layer2_facts / build_layer2_facts_ctas / publish_layer2_*_postgres /
// publish_etqa_facts_postgres (see infra/outputs.tf for the *_job_name outputs).
exports.handler = async (event) => {
  const { jobName } = event;

  if (!jobName) throw new Error("jobName was not provided in the event");

  const res = await glue.send(new StartJobRunCommand({ JobName: jobName }));
  return {
    jobName,
    jobRunId: res.JobRunId,
  };
};
