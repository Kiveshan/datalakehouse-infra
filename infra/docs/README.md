# Deploy permissions

`terraform-user-policy.json` is the set of permissions
`arn:aws:iam::123456789012:user/Terraform` needs in order to apply `dms.tf`.
Attach it as an inline or customer-managed policy **alongside** the user's
existing S3 and state permissions — it does not include those.

Without it the plan fails at the first EC2 call:

```
Error: reading EC2 VPC: ... UnauthorizedOperation: ... not authorized to
perform: ec2:DescribeVpcs
```

## Notes on scope

- **`dms:*`** is deliberately broad. DMS spreads a single `terraform apply`
  across create, describe, tag and modify calls on five resource types, and the
  enumerated-action version is long enough that a missing entry surfaces as a
  failed apply halfway through. Narrow it if your account requires it.

- **`iam:PassRole`** is the non-obvious one. Creating the S3 target endpoint
  hands `gov-skills-*-dms-s3` to DMS, and that is a `PassRole` on top of the
  permissions to create the role itself. It is conditioned on
  `iam:PassedToService = dms.amazonaws.com` so the role cannot be passed
  anywhere else.

- The **security group** statements cannot be resource-scoped: the group's ARN
  is not known until `CreateSecurityGroup` returns, so the create call has to be
  allowed on `*`.

## Avoiding the EC2 read permissions

If EC2 describe permissions cannot be granted, set `dms_vpc_id` and
`dms_subnet_ids` explicitly in `prod.tfvars`. Terraform then skips the default
VPC lookup entirely. The `ManageDmsSecurityGroup` statement is still required —
the security group has to be created either way.
