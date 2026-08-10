const { SNSClient, PublishCommand } = require("@aws-sdk/client-sns");

const sns = new SNSClient({});

// SNS_TOPIC_ARN needs a topic created in this project (the original account's
// etl-pipeline-notifications topic no longer applies to this account) --
// not created yet, wire it up alongside the Step Function.
exports.handler = async (event) => {
  const topicArn = process.env.SNS_TOPIC_ARN;

  if (!topicArn) throw new Error("SNS_TOPIC_ARN environment variable is not set");

  await sns.send(
    new PublishCommand({
      TopicArn: topicArn,
      Subject: "ETL Pipeline Failure",
      Message: JSON.stringify(event, null, 2),
    })
  );

  return { status: "alert-sent" };
};
