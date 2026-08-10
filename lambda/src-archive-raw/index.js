const {
  S3Client,
  ListObjectsV2Command,
  CopyObjectCommand,
} = require("@aws-sdk/client-s3");

const s3 = new S3Client({});

// LAKEHOUSE_BUCKET -> aws_s3_bucket.lakehouse-bucket.id (storage.tf)
// SOURCE_PREFIX     -> where DMS lands tables under raw/, e.g. "raw/lms_src/"
// Copies into the bucket's own archive/ prefix (storage.tf already creates it).
// Copy only, source is left in place -- DMS overwrites it on the next reload.
exports.handler = async () => {
  const bucket = process.env.LAKEHOUSE_BUCKET;
  const sourcePrefix = process.env.SOURCE_PREFIX || "raw/lms_src/";

  if (!bucket) throw new Error("LAKEHOUSE_BUCKET environment variable is not set");

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const archivePrefix = `archive/lms_src-${timestamp}/`;

  const listedObjects = await s3.send(
    new ListObjectsV2Command({ Bucket: bucket, Prefix: sourcePrefix })
  );

  if (!listedObjects.Contents || listedObjects.Contents.length === 0) {
    return { message: "No files to archive. Source folder is empty." };
  }

  for (const obj of listedObjects.Contents) {
    const srcKey = obj.Key;
    const destKey = srcKey.replace(sourcePrefix, archivePrefix);

    await s3.send(
      new CopyObjectCommand({
        Bucket: bucket,
        CopySource: `${bucket}/${srcKey}`,
        Key: destKey,
      })
    );
  }

  return {
    status: "archived",
    original: sourcePrefix,
    archiveLocation: archivePrefix,
    filesArchived: listedObjects.Contents.length,
  };
};
