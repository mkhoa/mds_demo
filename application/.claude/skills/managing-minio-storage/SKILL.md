---
name: managing-minio-storage
description: Use to inspect or manage MinIO object storage — listing buckets and objects, checking sizes, or uploading/downloading files in the data lake. Use when a task involves the raw landing zone or s3://dwhfilesystem.
---

# Managing MinIO Storage

The `mc` client is not in this container. Run it as a one-shot container on
`ndsnet`, configuring an alias from MinIO's own environment:

    docker run --rm --network mds_demo_ndsnet --entrypoint sh minio/mc -c '
      mc alias set local http://minio:9000 "$MINIO_ADMIN" "$MINIO_PWD" >/dev/null &&
      mc ls local/'

`MINIO_ADMIN` / `MINIO_PWD` are in `/workspace/.env`; pass them with
`-e MINIO_ADMIN=... -e MINIO_PWD=...` or read them from that file first.

## Common operations

- List buckets: `mc ls local/`
- List a bucket: `mc ls --recursive local/dwhfilesystem/`
- Object info: `mc stat local/dwhfilesystem/<path>`
- Bucket size: `mc du local/dwhfilesystem/`

## Buckets

`dwhfilesystem` is the primary data lake (Delta + Iceberg + parquet);
`dwhfilesystem/landing_area/` is the raw file drop zone.

## Rules

Listing and stat are always safe. Do NOT delete buckets or run recursive
`mc rm` — destructive deletion is out of scope. Confirm any single-object
removal with the user first.
