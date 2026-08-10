const {
  GlueClient,
  GetJobRunCommand,
} = require("@aws-sdk/client-glue");

const glue = new GlueClient({});

exports.handler = async (event) => {
  const { jobName, jobRunId } = event;

  if (!jobName) throw new Error("jobName was not provided in the event");
  if (!jobRunId) throw new Error("jobRunId was not provided in the event");

  const res = await glue.send(
    new GetJobRunCommand({
      JobName: jobName,
      RunId: jobRunId,
      PredecessorsIncluded: false,
    })
  );

  return {
    jobName,
    jobRunId,
    status: res.JobRun.JobRunState, // RUNNING, SUCCEEDED, FAILED, ...
  };
};
