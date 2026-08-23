const { S3Client, ListObjectsV2Command } = require("@aws-sdk/client-s3");
const { GlueClient, GetTableCommand } = require("@aws-sdk/client-glue");
const { CloudWatchClient, PutMetricDataCommand } = require("@aws-sdk/client-cloudwatch");

const s3 = new S3Client({});
const glue = new GlueClient({});
const cloudwatch = new CloudWatchClient({});
const METRICS_NAMESPACE = "ETLPipeline";

// LAKEHOUSE_BUCKET -> aws_s3_bucket.lakehouse-bucket.id (storage.tf)
// RAW_DATABASE     -> aws_glue_catalog_database.raw.name (catalog.tf)
// RAW_PREFIX       -> where DMS lands tables under raw/, e.g. "raw/lms_src/"
//                     (raw/<dms_target_bucket_folder>/<dms_source_schema_name>/)
exports.handler = async () => {
  const bucket = process.env.LAKEHOUSE_BUCKET;
  const glueDatabase = process.env.RAW_DATABASE;
  const rawPrefix = process.env.RAW_PREFIX || "raw/lms_src/";

  if (!bucket) throw new Error("LAKEHOUSE_BUCKET environment variable is not set");
  if (!glueDatabase) throw new Error("RAW_DATABASE environment variable is not set");

  // Step 1: list all "folders" (table names) under the raw prefix.
  const listed = await s3.send(
    new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: rawPrefix,
      Delimiter: "/",
    })
  );

  if (!listed.CommonPrefixes || listed.CommonPrefixes.length === 0) {
    throw new Error(`No table folders found under s3://${bucket}/${rawPrefix}`);
  }

  const failures = [];

  for (const prefixObj of listed.CommonPrefixes) {
    const folderPrefix = prefixObj.Prefix;
    const tableName = folderPrefix.split("/").filter(Boolean).pop();

    // Step 2: check at least 1 file exists in the folder.
    const files = await s3.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: folderPrefix,
        MaxKeys: 1,
      })
    );

    if (!files.Contents || files.Contents.length === 0) {
      failures.push(`Empty S3 folder: ${folderPrefix}`);
      continue;
    }

    // Step 3: check the Glue table exists and has columns.
    try {
      const table = await glue.send(
        new GetTableCommand({ DatabaseName: glueDatabase, Name: tableName })
      );

      if (!table.Table || !table.Table.StorageDescriptor.Columns.length) {
        failures.push(`Glue table ${tableName} has no columns.`);
      }
    } catch (err) {
      failures.push(`Glue table missing: ${tableName}`);
    }
  }

  await publishValidationMetrics(listed.CommonPrefixes.length, failures.length);

  if (failures.length) {
    throw new Error(`Validation failed:\n${failures.join("\n")}`);
  }

  return {
    status: "valid",
    tableCount: listed.CommonPrefixes.length,
  };
};

// Reports table-level validation results (missing S3 data / missing or
// columnless Glue tables) to CloudWatch, not row-level data quality --
// this lambda checks structure, not row content.
async function publishValidationMetrics(tablesValidated, validationFailures) {
  try {
    await cloudwatch.send(
      new PutMetricDataCommand({
        Namespace: METRICS_NAMESPACE,
        MetricData: [
          { MetricName: "TablesValidated", Value: tablesValidated, Unit: "Count" },
          { MetricName: "ValidationFailures", Value: validationFailures, Unit: "Count" },
        ],
      })
    );
  } catch (err) {
    // Metrics are best-effort -- never fail validation over a CloudWatch hiccup.
    console.warn(`Could not publish validation metrics: ${err}`);
  }
}
