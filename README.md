# Litestream Prometheus Exporter

A sidecar container that monitors Litestream LTX files (local and remote) and exposes Prometheus metrics.

## Metrics

| Metric | Type | Description |
| --- | --- | --- |
| `ops_litestream_local_ltx_files` | Gauge | Total number of LTX files in local metadata directory |
| `ops_litestream_local_ltx_bytes` | Gauge | Total size of LTX files in local metadata directory |
| `ops_litestream_local_ltx_files_by_level` | Gauge | Number of LTX files by compaction level (local) |
| `ops_litestream_local_ltx_bytes_by_level` | Gauge | Size of LTX files by compaction level (local) |
| `ops_litestream_remote_ltx_files` | Gauge | Total number of LTX files in remote replica (S3/R2) |
| `ops_litestream_remote_ltx_bytes` | Gauge | Total size of LTX files in remote replica (S3/R2) |
| `ops_litestream_remote_ltx_files_by_level` | Gauge | Number of LTX files by compaction level (remote) |
| `ops_litestream_remote_ltx_bytes_by_level` | Gauge | Size of LTX files by compaction level (remote) |
| `ops_litestream_last_scrape_timestamp_seconds` | Gauge | Unix timestamp of last successful scrape |
| `ops_litestream_scrape_duration_seconds` | Gauge | Duration of last metrics scrape |
| `ops_litestream_scrape_errors` | Gauge | Total scrape errors by source |

### Compaction Levels

Litestream uses compaction levels to organize LTX files:

| Level | Name | Description |
| --- | --- | --- |
| 0 | L0 (raw) | Raw LTX files, no compaction |
| 1 | L1 (30s) | 30-second compaction |
| 2 | L2 (5min) | 5-minute compaction |
| 3 | L3 (hourly) | Hourly compaction |
| 9 | L9 (snapshot) | Full database snapshots |

## Configuration

Environment variables:

| Variable | Required | Description |
| --- | --- | --- |
| `LOCAL_LTX_DIR` | No | Path to local LTX directory (e.g., `/data/.omni.db-litestream/ltx`) |
| `S3_BUCKET` | No | S3/R2 bucket name |
| `S3_PREFIX` | No | S3 prefix/path (e.g., `omni`) |
| `S3_ENDPOINT_URL` | No | S3-compatible endpoint URL (required for Cloudflare R2) |
| `LITESTREAM_ACCESS_KEY_ID` | No | AWS/R2 access key ID |
| `LITESTREAM_SECRET_ACCESS_KEY` | No | AWS/R2 secret access key |
| `METRICS_PORT` | No | Port to expose metrics (default: `9090`) |
| `SCRAPE_INTERVAL_SECONDS` | No | How often to refresh metrics (default: `60`) |

## Usage

### Docker Compose

```yaml
services:
  litestream-exporter:
    build: ./scripts/litestream_exporter
    environment:
      LOCAL_LTX_DIR: /data/.omni.db-litestream/ltx
      S3_BUCKET: my-bucket
      S3_PREFIX: omni
      S3_ENDPOINT_URL: https://ACCOUNT_ID.r2.cloudflarestorage.com
      LITESTREAM_ACCESS_KEY_ID: ${LITESTREAM_ACCESS_KEY_ID}
      LITESTREAM_SECRET_ACCESS_KEY: ${LITESTREAM_SECRET_ACCESS_KEY}
      METRICS_PORT: "9090"
      SCRAPE_INTERVAL_SECONDS: "60"
    volumes:
      - data:/data:ro
    ports:
      - "9090:9090"

volumes:
  data:
```

### Kubernetes Sidecar

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: app-with-litestream
spec:
  containers:
    - name: app
      # ... your app container
      volumeMounts:
        - name: data
          mountPath: /data
    
    - name: litestream
      image: litestream/litestream
      # ... litestream config
      volumeMounts:
        - name: data
          mountPath: /data
    
    - name: litestream-exporter
      image: your-registry/litestream-exporter:latest
      env:
        - name: LOCAL_LTX_DIR
          value: /data/.omni.db-litestream/ltx
        - name: S3_BUCKET
          value: my-bucket
        - name: S3_PREFIX
          value: omni
        - name: S3_ENDPOINT_URL
          value: https://ACCOUNT_ID.r2.cloudflarestorage.com
        - name: LITESTREAM_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: litestream-secrets
              key: access-key-id
        - name: LITESTREAM_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: litestream-secrets
              key: secret-access-key
      ports:
        - containerPort: 9090
          name: metrics
      volumeMounts:
        - name: data
          mountPath: /data
          readOnly: true
  
  volumes:
    - name: data
      emptyDir: {}
```

### Local Development

TODO

## Prometheus Scrape Config

```yaml
scrape_configs:
  - job_name: 'litestream'
    static_configs:
      - targets: ['litestream-exporter:9090']
    scrape_interval: 60s
```
