# Modern-Data-Pipeline

## Initial:
1. Create GitHub repo & connect locally
2. Create folder structure (dbt_project/, airflow_dags/, docker-compose.yml, README.md)

## Environment Setup
1. Create GCP projects: dev and prod
2. Create datasets on BigQuery (in both projects): `mdp_raw`, `mdp_stg`, `mdp_int`, `mdp_fct`, `mdp_dim`, `mdp_rep` 
3. Create service accounts fror both projects
    - give relevant access to the account while creating it
4. Generate JSON keys and download them
5. Create `.env` file with paths and IDs
5. Create `.gitignore` file

## Documentation Coverage (dbt-coverage)

We use [`dbt-coverage`](https://github.com/slidoapp/dbt-coverage) to measure how many models/columns have a `description` in schema.yml. It is **not a dbt package** — it doesn't appear in `packages.yml` or `dbt_packages/`. It's a standalone Python CLI tool, installed separately into the project's Python environment (`dbt-env`):

```
pip install dbt-coverage
```

It works by reading two files dbt already generates — it never parses `.sql` or `.yml` directly:
- `target/catalog.json` (from `dbt docs generate`) — the **real, physical columns** that exist in BigQuery right now. This is the source of truth for "what columns exist."
- `target/manifest.json` (from `dbt parse`) — the columns/descriptions declared in schema.yml.

A column counts as a documentation gap only if it physically exists in BigQuery (per catalog.json) but has no described entry in schema.yml (per manifest.json). This also means it won't catch the opposite problem — a schema.yml entry for a column that no longer exists in the model's SQL (stale/renamed columns) — since those never appear in catalog.json at all.

### How to run it

```
dbt docs generate --no-partial-parse
dbt-coverage compute doc --model-path-exclusion-filter models/edr/
```

Two flags matter here, both worth keeping:
- `--no-partial-parse` — dbt's partial-parse cache (`target/partial_parse.msgpack`) has a known edge case where editing one model's schema.yml "patch" can cause *sibling* models in the same YAML file to lose their parsed descriptions/columns in the manifest, even though the YAML on disk is untouched and correct. Forcing a full reparse avoids reporting a false coverage drop.
- `--model-path-exclusion-filter models/edr/` — excludes the `elementary` package's own models (`dq_tests_elementary` schema) from the report. Elementary isn't part of our documented model layer, so it shouldn't count toward coverage.

Add `--cov-report coverage.json` to save the report as a file instead of (or in addition to) printing it. That file is regenerated on demand and gitignored — it's a point-in-time report, not something to commit.

## 