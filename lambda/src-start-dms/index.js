const {
  DatabaseMigrationServiceClient,
  StartReplicationTaskCommand,
} = require("@aws-sdk/client-database-migration-service");

// REPLICATION_TASK_ARN should be set to this project's
// module.lms_dms.replication_task_arn output (infra/outputs.tf ->
// dms_replication_task_arn), not a hardcoded ARN from another account.
// Region is picked up automatically from the Lambda's AWS_REGION env var.
const dmsClient = new DatabaseMigrationServiceClient({});

exports.handler = async () => {
  const replicationTaskArn = process.env.REPLICATION_TASK_ARN;

  if (!replicationTaskArn) {
    throw new Error("REPLICATION_TASK_ARN environment variable is not set");
  }

  const command = new StartReplicationTaskCommand({
    ReplicationTaskArn: replicationTaskArn,
    StartReplicationTaskType: "reload-target",
  });

  const response = await dmsClient.send(command);

  return {
    replicationTaskArn,
    status: response.ReplicationTask?.Status,
  };
};
