# storage.tf
#
# The lakehouse S3 bucket: raw/curated/archive/layer2 prefixes and default
# encryption. Shared locals (name_prefix, common_tags) live in locals.tf.
#
# layer2/ is the Iceberg warehouse root for the SCD2 dim tables built from
# curated/ (see layer2_dims.tf) — kept separate from the plain-Parquet
# curated/<table>/ layout since Iceberg manages its own file layout under it.

locals {
  folders = ["archive", "raw", "curated", "layer2"]
}

resource "aws_s3_bucket" "lakehouse-bucket" {
  bucket = local.name_prefix
  tags = {
    Project   = "gov-skills-pipeline"
    ManagedBy = "terraform"
  }

}


resource "aws_s3_object" "lakehouse-folders" {
  for_each = toset(local.folders)
  bucket   = aws_s3_bucket.lakehouse-bucket.id
  key      = "${each.value}/" # The trailing slash indicates a folder
}


resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lakehouse-bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}