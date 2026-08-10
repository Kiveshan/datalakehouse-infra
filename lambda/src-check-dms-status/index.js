const {
  DatabaseMigrationServiceClient,
  DescribeReplicationTasksCommand,
} = require("@aws-sdk/client-database-migration-service");

// Same REPLICATION_TASK_ARN as src-start-dms (this project's
// module.lms_dms.replication_task_arn) so both steps always point at the
// same task instead of drifting.
const dmsClient = new DatabaseMigrationServiceClient({});

exports.handler = async () => {
  const taskArn = process.env.REPLICATION_TASK_ARN;

  if (!taskArn) {
    throw new Error("REPLICATION_TASK_ARN environment variable is not set");
  }

  const command = new DescribeReplicationTasksCommand({
    Filters: [
      {
        Name: "replication-task-arn",
        Values: [taskArn],
      },
    ],
  });

  const result = await dmsClient.send(command);

  if (!result.ReplicationTasks || result.ReplicationTasks.length === 0) {
    throw new Error("No replication task found with the provided ARN");
  }

  return { taskArn, status: result.ReplicationTasks[0].Status };
};
