# GCP Permissions & Resource Setup for dbt Medallion Architecture

## Project & Datasets

**GCP Project:** `acn-uki-ds-data-ai-project`  
**Region:** `EU`

### BigQuery Datasets to Create

Run these once before first `dbt build`:

```bash
bq --location=EU mk --dataset acn-uki-ds-data-ai-project:silver
bq --location=EU mk --dataset acn-uki-ds-data-ai-project:gold
bq --location=EU mk --dataset acn-uki-ds-data-ai-project:ops
```

The source dataset `credit_card_synt` already exists (created by credit_card_synthetic.py).

---

## IAM Roles Required

The service account (or user) running dbt needs these roles:

| Resource | Required Role | Why |
|---|---|---|
| `credit_card_synt` dataset | `roles/bigquery.dataViewer` | Read Bronze source tables |
| `silver` dataset | `roles/bigquery.dataEditor` | Create/replace Silver tables |
| `gold` dataset | `roles/bigquery.dataEditor` | Create/replace Gold tables |
| `ops` dataset | `roles/bigquery.dataEditor` | Create/replace ops tables (seeds, reports) |
| Project level | `roles/bigquery.jobUser` | Execute BigQuery jobs |

### Grant via gcloud (replace `SA_EMAIL` with your service account):

```bash
# Read access to Bronze source
bq add-iam-policy-binding \
  --member="serviceAccount:SA_EMAIL" \
  --role="roles/bigquery.dataViewer" \
  acn-uki-ds-data-ai-project:credit_card_synt

# Write access to Silver, Gold, Ops
for dataset in silver gold ops; do
  bq add-iam-policy-binding \
    --member="serviceAccount:SA_EMAIL" \
    --role="roles/bigquery.dataEditor" \
    acn-uki-ds-data-ai-project:$dataset
done

# Job execution at project level
gcloud projects add-iam-policy-binding acn-uki-ds-data-ai-project \
  --member="serviceAccount:SA_EMAIL" \
  --role="roles/bigquery.jobUser"
```

### For interactive OAuth (local dev — your own account):

```bash
gcloud auth application-default login
gcloud config set project acn-uki-ds-data-ai-project
```

Your account (`sooraj.t.r@accenture.com`) needs the same roles above granted at dataset level.

---

## Authentication in profiles.yml

**Option 1 — OAuth (local dev, uses your logged-in gcloud account):**
```yaml
method: oauth
```

**Option 2 — Service account key (CI/CD):**
```yaml
method: service-account
keyfile: /path/to/sa_key.json
```

**Option 3 — Workload Identity / ADC (GCP-hosted runners):**
```yaml
method: oauth
```
No keyfile needed — ADC picks up the environment automatically.

---

## dbt Installation & First Run

```bash
# Install dbt with BigQuery adapter
pip install dbt-bigquery

# Copy profiles.yml to the dbt home directory
cp profiles.yml ~/.dbt/profiles.yml

# Navigate to project
cd dbt_project/

# Install dbt packages (dbt_utils, dbt_expectations)
dbt deps

# Verify connection
dbt debug

# Seed the mapping tables into ops dataset
dbt seed

# Build everything: seed + run + test in dependency order
dbt build

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

---

## Quota & Cost Notes

- `gold_transaction_360` is partitioned by `transaction_date` — queries on date ranges will only scan the relevant partitions.
- `gold_transaction_360` is clustered by `customer_id, merchant_id` — further reduces bytes scanned on filtered queries.
- For large-scale incremental runs, switch Silver/Gold to `incremental` materialization in `dbt_project.yml` (add `unique_key` configs) to avoid full table rebuilds.
- The `medallion_reconciliation_report` runs 32 subqueries across all layers — run it on a schedule rather than every build to manage slot usage.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Access Denied: Dataset acn-uki-ds-data-ai-project:silver` | Dataset doesn't exist or no WRITE permission | Run `bq mk` + grant `dataEditor` |
| `Access Denied: Table credit_card_synt.customers` | No READ permission on source | Grant `dataViewer` on `credit_card_synt` |
| `Not found: Dataset acn-uki-ds-data-ai-project:ops` | ops dataset not created | Run `bq mk --dataset acn-uki-ds-data-ai-project:ops` |
| `Could not serialize access` | Concurrent DML on same partition | Reduce `--threads` in profiles.yml |
| `dbt debug` fails with auth error | ADC not configured | Run `gcloud auth application-default login` |
