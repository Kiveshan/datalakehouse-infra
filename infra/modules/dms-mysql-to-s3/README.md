# DMS MySQL → S3 module

Account-agnostic version of the `lms-prod` → `s3-target` DMS setup, extracted from
account `123456789012` / `af-south-1` on 2026-08-04.

Drop this directory into another Terraform project and call it as a module. It
contains **no provider block** — the calling project supplies the provider, and
therefore the account and region.

## Usage

```hcl
module "lms_dms" {
  source = "./modules/dms-mysql-to-s3"

  # Source (Azure MySQL)
  source_server_name = "prod-source.mysql.database.azure.com"
  source_username    = "report_user"
  source_password    = var.lms_prod_password # never hardcode

  # Target (S3 in THIS account)
  target_bucket_name = "my-new-bucket-name"
  dms_s3_role_arn    = aws_iam_role.dms_s3.arn

  # Networking in THIS account's VPC
  replication_subnet_group_id = aws_dms_replication_subnet_group.this.id
  vpc_security_group_ids      = [aws_security_group.dms.id]

  tags = {
    description = "Migration from Azure MySQL to S3"
  }
}
```

Supply the password out of band, never in a committed file:

```bash
export TF_VAR_lms_prod_password='...'
```

## Before your first apply

This module creates the DMS resources only. Four things must exist or happen
outside it:

1. **The S3 bucket.** Bucket names are globally unique, so the original
   bucket name cannot be reused. Create a new one in the target
   account and pass its name.

2. **The DMS S3 IAM role.** Needs a trust policy for `dms.<region>.amazonaws.com`
   and `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`, `s3:GetObject` on the
   bucket. It usually belongs next to the bucket, not in here.

3. **A replication subnet group and security group** in the target account's VPC.
   The security group must allow egress to the source MySQL host on port 3306.

4. **Azure firewall allowlisting.** A new replication instance gets a **new public
   IP**. Until it is added to the Azure MySQL server's firewall rules, the task
   cannot connect. Read it from the `replication_instance_public_ips` output after
   the instance is created. This is the step most likely to bite you, and it is
   entirely outside Terraform.

## Differences from the original

These are deliberate, not oversights:

| Item | Original | Here | Why |
|---|---|---|---|
| `ssl_mode` (source) | `none` | `require` | The original sent credentials to Azure unencrypted over the internet. A fresh deploy is the right moment to fix it. Set `source_ssl_mode = "none"` to reproduce the old behaviour. |
| `availability_zone` | `af-south-1c` | unset | AZ *names* are shuffled per account, so the name does not identify the same physical AZ elsewhere. DMS picks from the subnet group. |
| `kms_key_arn` | CMK in `123456789012` | `null` | That key does not exist in another account. Null means the AWS-managed `aws/dms` key. Pass a target-account key ARN to use a CMK. |
| `prevent_destroy` | set | not set | It would block `terraform destroy` for the entire calling project. See the comment in `replication_task.tf` to add it back. |
| `start_replication_task` | unset | unset (`null`) | Terraform does not manage run state, so an apply can never start or stop a migration as a side effect. |

## Warning: `DROP_AND_CREATE`

`FullLoadSettings.TargetTablePrepMode` is `DROP_AND_CREATE`, carried over from the
original task. Running the task overwrites whatever already exists under
`s3://<bucket>/<bucket_folder>/`. Check the prefix before the first run.
