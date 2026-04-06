# S3 Backup & Restore

Back up and restore TAPIR clusters to Amazon S3 or S3-compatible services
(MinIO, Ceph RGW, etc.) by specifying an `s3://` path.

## Prerequisites

- AWS credentials configured via environment variables, `~/.aws/credentials`,
  IAM role, or any method supported by the
  [AWS SDK credential chain](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html)

## Usage

The S3 backend activates automatically when the path starts with `s3://`:

```bash
# Full backup to S3:
tapictl backup cluster \
  --shard-manager-url http://127.0.0.1:9001 \
  --output s3://my-bucket/backups/2025-01-15/

# Incremental backup (same path, existing cluster.json detected):
tapictl backup cluster \
  --shard-manager-url http://127.0.0.1:9001 \
  --output s3://my-bucket/backups/2025-01-15/

# Restore from S3:
tapictl restore cluster \
  --shard-manager-url http://127.0.0.1:9001 \
  --admin-addrs 10.0.1.1:9000,10.0.1.2:9000,10.0.1.3:9000 \
  --input s3://my-bucket/backups/2025-01-15/

# List backups in S3:
tapictl get backups --dir s3://my-bucket/backups/

# Solo mode works too:
tapictl solo backup --admin-addrs 127.0.0.1:9000 --output s3://my-bucket/solo-backup/
tapictl solo restore --admin-addrs 10.0.1.1:9000 --input s3://my-bucket/solo-backup/
```

## S3-Specific Flags

| Flag | Description |
|------|-------------|
| `--s3-region` | AWS region (overrides SDK default) |
| `--s3-endpoint` | Custom endpoint URL for S3-compatible services |

When `--s3-endpoint` is set, path-style access is enabled automatically
(required by MinIO and most S3-compatible services).

## Local Testing with MinIO

Test S3 backups locally using MinIO in Docker:

```bash
# Start MinIO:
docker run -d --name tapi-minio -p 9100:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

# Create a test bucket (no system-wide AWS CLI needed):
docker run --rm --network host \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \
  amazon/aws-cli --endpoint-url http://localhost:9100 s3 mb s3://tapi-test

# Run backup:
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  tapictl backup cluster \
  --shard-manager-url http://127.0.0.1:9001 \
  --output s3://tapi-test/my-backup/ \
  --s3-endpoint http://localhost:9100

# Clean up:
docker rm -f tapi-minio
```

## IAM Policy

Minimum permissions for the backup bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:HeadObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket",
        "arn:aws:s3:::my-bucket/*"
      ]
    }
  ]
}
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `NoSuchBucket` | Bucket doesn't exist | Create the bucket first |
| `InvalidAccessKeyId` | Bad credentials | Check `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` |
| `SignatureDoesNotMatch` | Endpoint/region mismatch | Verify `--s3-region` and `--s3-endpoint` |
| Timeout on MinIO | Wrong port or network | Ensure MinIO is reachable at the endpoint URL |
