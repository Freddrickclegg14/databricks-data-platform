# Talent Analytics Lakehouse (Databricks)

A medallion-architecture (Bronze → Silver → Gold) lakehouse built on Databricks Free Edition, using a public LinkedIn job-postings dataset to answer one real question about hiring trends. The third and smallest project in a three-part data engineering portfolio — see `batch-data-platform` (Fannie Mae + yfinance, GCP/BigQuery/dbt) and `streaming-fraud-platform` (Kafka/PyFlink fraud detection) for the other two.

Deliberately scoped down: this is a single-platform showcase to demonstrate Databricks (Delta Lake, Unity Catalog, DABs) with reusable PySpark skills, not a third mega-project.

## Core question

**Does salary rise consistently with seniority level, and do required skills cluster differently across seniority and salary bands?**

## Core finding

Average salary rises monotonically with seniority across every level in the dataset:

| Seniority level | Postings | Avg. annual min salary | Avg. annual max salary |
|---|---|---|---|
| Internship | 370 | $50,055 | $61,577 |
| Entry level | 9,114 | $58,164 | $73,567 |
| Associate | 3,891 | $72,302 | $91,450 |
| (unlabeled) | 8,117 | $88,777 | $125,355 |
| Mid-Senior level | 12,904 | $97,511 | $129,495 |
| Director | 1,268 | $146,554 | $198,016 |
| Executive | 382 | $173,645 | $229,933 |

This is based on **36,046 of 123,849 postings (29.1%)** — the subset with usable salary data.

**Skill clustering is a weaker signal than seniority/salary.** The dataset's skill taxonomy is only 35 broad categories, and Information Technology, Sales, and Management dominate the top-3 skills across nearly every seniority level and salary band — there's no sharp, distinct clustering by level at this granularity. The one real, consistent trend: Manufacturing postings are heavily front-loaded at `<60k` (~3,200) and fall off sharply at every band above it, while Engineering starts low at `<60k` (~550), rises to peak at `100-150k` (~1,650), then dips again at `150k+`. The two lines cross around `60-100k`. So skill mix shifts away from Manufacturing and toward Engineering/IT as pay rises, up to a point — a real but modest finding, not the sharp clustering the question originally hoped for. Full breakdowns in `gold_layer.gold_skills_by_seniority` (245 rows) and `gold_layer.gold_skills_by_salary_band` (140 rows).

## Dataset

[LinkedIn Job Postings](https://www.kaggle.com/datasets/arshkon/linkedin-job-postings) (Kaggle, arshkon) — 123,849 postings plus companion tables for companies, job skills, and salaries. No actual Hugo data was used, matching the boundary set on the other two portfolio projects.

## Architecture

```
Kaggle CSVs (postings.csv, jobs/job_skills.csv, jobs/salaries.csv, mappings/skills.csv)
        │
        ▼  Unity Catalog Volume (raw landing)
   Bronze layer  — raw ingest, as-is, Delta tables
        │
        ▼
   Silver layer  — typed, deduplicated, validated, quality-flagged
        │
        ▼
   Gold layer    — 3 aggregate tables answering the core question
        │
        ▼
   Databricks SQL dashboard
```

| Table | Layer | Purpose |
|---|---|---|
| `bronze_postings` | Bronze | Raw job postings, as ingested |
| `bronze_job_skills` | Bronze | Raw job-to-skill-code mapping |
| `bronze_salaries` | Bronze | Raw salary records |
| `silver_postings` | Silver | Deduplicated on `job_id`, epoch timestamps cast properly |
| `silver_job_skills` | Silver | Deduplicated, joined to skill names |
| `silver_salaries` | Silver | Annualized salary figures, quality-flagged |
| `gold_skills_by_seniority` | Gold | Skill frequency per seniority level |
| `gold_skills_by_salary_band` | Gold | Skill frequency per salary band |
| `gold_seniority_salary_summary` | Gold | Avg. salary per seniority level (the headline finding) |

## Platform & tech stack

- **Databricks Free Edition** (not the retired Community Edition) — free, no credit card, supports Git-integrated Repos and Workflows
- **Delta Lake + Unity Catalog** for storage, with separate `bronze_layer` / `silver_layer` / `gold_layer` schemas in the `data-engineering` catalog
- **PySpark** for all transforms, run as notebooks on Databricks' serverless compute (Free Edition has no classic/custom clusters — no `node_type_id` or pinned Spark version)
- **Databricks Asset Bundles (DABs)** for orchestration-as-code — `databricks.yml` + `resources/jobs.yml` define the three-task pipeline as a reviewable file, not a UI-only click-through
- **Databricks SQL dashboard** off the Gold tables
- **Databricks Repos**, connected to this GitHub repo, notebooks saved in Source format (`.py`) so they diff and render properly on GitHub

## Known data-quality findings (disclosed, not hidden)

- **Multi-line CSV parsing bug (caught before trusting any output):** Spark's default CSV reader splits on every literal newline, including ones inside quoted multi-line `description` text. Initial `bronze_postings` load produced 1,842,544 rows instead of the correct 123,849 — fixed with `.option("multiLine", "true").option("escape", '"')` and re-verified against a pandas-based row count.
- **Sparse salary data:** `postings.csv`'s own salary columns are 70–95% null. The `jobs/salaries.csv` companion table is used instead (40,785 populated records), with `postings.normalized_salary` as a documented fallback where no match exists.
- **Mixed min/max/med population:** Some `salaries.csv` rows only populate `med_salary`, leaving `min_salary`/`max_salary` null. Annualization uses `coalesce(min_salary, med_salary)` / `coalesce(max_salary, med_salary)` so these rows aren't silently dropped.
- **31 mislabeled salary records:** 31 of 40,785 salary rows have `pay_period = HOURLY` with a `min_salary` in the tens or hundreds of thousands (e.g. $240,000) — plainly an annual figure mislabeled as hourly in the source. Left uncorrected, the ×2080 hourly-to-annual multiplier produced an average of $831,867/year for Internship postings and a non-monotonic salary curve across seniority levels. These 31 rows are flagged (`salary_quality_flag = 'implausible_hourly_rate'`) in `silver_salaries` and excluded from Gold aggregates — flagged, not silently dropped, so the record is traceable.
- **Salary coverage is partial:** only 29.1% of postings have usable salary data. The salary-based Gold tables reflect that subset, not the full dataset — stated here explicitly rather than left implicit in the numbers.

## Deliberately out of scope

- No cloud deployment beyond Databricks itself (no separate Terraform-provisioned VM, unlike the other two projects)
- No CI/CD pipeline
- No dead-letter/reliability engineering — not a streaming system
- No multi-rule comparison or precision/recall evaluation — one clear, verified finding is the goal
- Testing limited to sanity checks (row counts, null checks, key uniqueness) at each layer transition — no full pytest suite

## Running it

```
databricks bundle validate --profile <your-profile>
databricks bundle deploy --profile <your-profile>
databricks bundle run medallion_etl_job --profile <your-profile>
```