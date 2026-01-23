# EDGAR Pipeline

**A reference implementation for building your own SEC EDGAR data pipeline.**

This is a blueprint demonstrating how to orchestrate SEC filing metadata ingestion using Airflow, S3, DuckDB, and dbt. It is not a turnkey SaaS product-it's a starting point for teams that want to understand and control their SEC data infrastructure.

---

## What This Project Is (and Isn't)

**This is:**
- A working example of ELT pipeline architecture for SEC data
- A learning resource for Airflow/dbt patterns with real data
- A foundation you can extend for your specific needs

**This is not:**
- A production-ready service you can deploy without modification
- A replacement for commercial APIs if you need parsed financials
- The only (or necessarily best) way to access SEC data

---

## What the SEC Already Provides (Free)

Before using this pipeline, understand what the SEC offers directly at no cost:

| SEC Resource | What It Provides | URL |
|--------------|------------------|-----|
| **Daily Index Files** | Metadata for all filings each day | `sec.gov/Archives/edgar/daily-index/` |
| **submissions.zip** | All companies' filing history (~3GB) | `sec.gov/Archives/edgar/daily-index/bulkdata/` |
| **companyfacts.zip** | XBRL financial facts for all filers (~15GB) | `sec.gov/Archives/edgar/daily-index/xbrl/` |
| **Company API** | JSON for any company's submissions/facts | `data.sec.gov/submissions/CIK*.json` |

**Rate limits:** ~10 requests/second with required User-Agent header.

**This pipeline builds on the daily index files.** The SEC already provides excellent structured data—this project adds orchestration, storage, and transformation layers on top.

---

## Should You Use This Pipeline?

**Decision framework:**

```
1. Do you need structured financial statements (revenue, EPS, balance sheet)?
   └─ YES → Use SEC's companyfacts API, a commercial XBRL API, or build an XBRL parser
   └─ NO → Continue

2. Do you need real-time filing notifications (<1 hour latency)?
   └─ YES → Use sec-api.io webhooks, AlphaSense, or similar streaming service
   └─ NO → Continue

3. Do you have Airflow/dbt expertise in-house?
   └─ NO → Use a commercial API or the simpler script alternative below
   └─ YES → Continue

4. Are you building something requiring full control over your data pipeline?
   └─ YES → This pipeline is worth exploring
   └─ NO → Existing solutions will likely serve you better
```

---

## The Simplest Alternative

Before investing in this pipeline, consider whether a simple script meets your needs:

```python
# download_submissions.py - 10 lines to get all SEC filing history
import requests
import zipfile
import io

url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
headers = {"User-Agent": "YourName your@email.com"}
resp = requests.get(url, headers=headers)
with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
    z.extractall("./submissions/")
print(f"Extracted {len(z.namelist())} company files")
```

**This gives you:** Every public company's complete filing history in JSON format.

**This pipeline is for when:** That script needs production orchestration—scheduling, retries, monitoring, transformations, data quality checks, and auditability.

---

## Honest Cost Comparison

| Approach | Monthly Cost | Engineering Time | Best For |
|----------|--------------|------------------|----------|
| **Commercial API** (sec-api.io) | $29–$299/mo | Minimal | Quick integration, no infrastructure |
| **This Pipeline** | ~$5–20 S3 + compute | Setup complete already, ongoing maintenance | Full control, custom transformations |
| **Simple Script + Cron** | ~$0–5 | 2–4 hrs setup | Occasional batch downloads |

**Reality check:** Engineering time has real cost. A $150k/year engineer costs ~$12,500/month. Building on and maintaining this pipeline only makes economic sense if:
- You're learning infrastructure patterns
- You need customizations commercial APIs don't offer
- You're processing at scale where per-request API costs become prohibitive
- You require complete data lineage for compliance/audit

---

## Competitive Landscape

### Commercial APIs

| Provider | Pricing | Strengths | Limitations |
|----------|---------|-----------|-------------|
| **sec-api.io** | Free tier, $29–$299/mo paid | Full-text search, XBRL parsing, webhooks | Per-request limits, vendor dependency |
| **Intrinio** | $75–$500+/mo | SEC + market data bundle | Complex pricing, bundled products |
| **Polygon.io** | $29–$199/mo | Strong market data, SEC secondary | SEC not primary focus |
| **FactSet/Refinitiv** | $10k–$25k+/yr | Enterprise-grade, full integration | Requires institutional budget |

### Open-Source Alternatives

| Project | GitHub Stars | Last Updated | Focus |
|---------|--------------|--------------|-------|
| **sec-edgar-downloader** | ~500+ | Active | Simple download library |
| **edgartools** | ~200+ | Active | Pythonic API wrapper |
| **OpenEDGAR** | ~100+ | Less active | Academic research framework |
| **edgar-crawler** | ~100+ | Active | NLP/text extraction |

**When open-source libraries are better:** If you just need to download filings programmatically, `sec-edgar-downloader` or `edgartools` are simpler and better maintained than building a full pipeline.

**When this pipeline adds value:** If you need orchestration, incremental loading, data quality validation, and transformation layers that integrate with existing Airflow/dbt infrastructure.

---

## What This Pipeline Provides

| Capability | Description |
|------------|-------------|
| **Orchestration** | Airflow DAG with retry logic and backfill support |
| **Raw Storage** | S3 archival of original .idx files |
| **Structured Loading** | Parsed records in DuckDB with idempotent upserts |
| **Transformations** | dbt models for deduplication and validation |
| **Data Quality** | Automated tests and post-load verification |
| **Lineage** | Every record traces to source file and load timestamp |

---

## What This Pipeline Does NOT Provide (currently)

| Limitation | Implication | Alternative |
|------------|-------------|-------------|
| **No parsed financials** | Only filing metadata, not revenue/EPS/balance sheet | Use companyfacts.zip or commercial XBRL API |
| **No document text** | Indexes filings but doesn't download 10-K content | Add document fetcher using `filename` field |
| **No real-time streaming** | Batch processing, typically daily | Use webhook services for sub-hour latency |
| **No concurrent access** | DuckDB is embedded, single-writer | Migrate to cloud DWH for multi-user |
| **No professional support** | Open-source, self-maintained | Commercial APIs include support |

---

## Target Users (Honest Assessment)

### ✅ Good Fit

**Data Engineering Learners**
- Want hands-on experience with Airflow/dbt patterns (this project was my genesis into Data Engineering!)
- Learning ELT architecture with real-world data

**Fintech Startups (Early Stage)**
- Need cost-controlled infrastructure foundation
- Have engineering capacity to maintain
- Building products requiring filing metadata

**Academic Researchers**
- Need bulk historical data without API costs
- Can invest setup time for long-term research
- Require reproducible data pipelines

### ⚠️ Questionable Fit

**Compliance Teams**
- Typically buy SaaS solutions, don't build pipelines
- Need vendor support and audit trails that commercial products provide
- May not have engineering resources for maintenance

**Quant Funds**
- Usually use specialized data platforms (Bloomberg, FactSet)
- Need parsed financials, not just metadata
- Require real-time or near-real-time data

**Enterprise Users**
- Generally prefer vendor solutions with SLAs
- Need multi-user concurrent access
- Require professional support contracts

---

## Technical Architecture

### Data Flow

```
SEC EDGAR (daily .idx) → S3 (raw archive) → DuckDB (parsed) → dbt (transformed)
```

### Pipeline Tasks

| Task | Purpose |
|------|---------|
| `fetch_filings_to_s3` | Download SEC index with retry logic |
| `load_duckdb` | Parse and insert records (idempotent) |
| `verify_post_load` | Validate counts and freshness |
| `run_dbt_models` | Execute transformations |
| `run_dbt_tests` | Data quality assertions |
| `run_smoke_query` | Final verification |

### Why DuckDB

**Advantages:**
- Zero cost, embedded database
- Fast analytical queries
- Simple backup (single file)
- No server management

**When to migrate to cloud database:**
- Multiple users need concurrent access
- Data exceeds local storage capacity
- Need enterprise SLAs and managed backups

The dbt models port directly—change adapter from `dbt-duckdb` to `dbt-snowflake`/`dbt-redshift`.

---

## Future Roadmap: XBRL Financial Data

**Planned Improvement (if I have the time to get around to it):** Parsing structured financial statements from SEC's XBRL data.

### Current State (Metadata Only)
- Knows *when* Apple files a 10-K, but not what's in it
- Tracks filing patterns, not financial performance
- Useful for monitoring, not financial analysis

### With XBRL Parsing (Future)
- **Structured financials**: Revenue, EPS, assets as queryable tables
- **Cross-company comparison**: Compare Apple vs Microsoft metrics
- **Time-series analysis**: Track performance quarterly/yearly

### Why XBRL is more challenging
- Complex XML with concepts, contexts, units
- Company-specific taxonomy extensions
- ~15GB daily (companyfacts.zip)
- Inline XBRL embedded in HTML filings

### Implementation Options
```python
# Option 1: SEC's companyfacts API (easiest)
# Already provides parsed XBRL in JSON
resp = requests.get("https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json")

# Option 2: Bulk companyfacts.zip (medium)  
# ~15GB nightly, all companies' facts

# Option 3: Parse raw filings (hardest)
# Download HTML, extract inline XBRL, handle custom taxonomies
```

---

## Getting Started

### Prerequisites
- Docker Desktop
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)
- AWS credentials with S3 access

### Setup

```bash
git clone https://github.com/mrbt03/edgar-pipeline.git
cd edgar-pipeline

export EDGAR_S3_BUCKET=your-bucket-name
export AWS_DEFAULT_REGION=us-east-1
export DUCKDB_PATH=/data/edgar.duckdb

astro dev start
```

Access Airflow at http://localhost:8080 (credentials: `admin`/`admin`)

### Run Tests

```bash
astro dev bash -s --scheduler
pytest tests/ -v
```

---

## Data Schema

### raw.edgar_master

| Column | Type | Description |
|--------|------|-------------|
| `cik` | VARCHAR | Central Index Key |
| `company_name` | VARCHAR | Legal entity name |
| `form_type` | VARCHAR | SEC form type (10-K, 8-K, etc.) |
| `date_filed` | VARCHAR | Filing date |
| `filename` | VARCHAR | Path to document on SEC servers |
| `loaded_at` | TIMESTAMP | Pipeline load timestamp |
| `index_file_date` | VARCHAR | Source index file date |

---

## Acknowledgments

This pipeline is a personal project I built entirely on the SEC's public data infrastructure. It was my data engineering project. The SEC provides free, programmatic access to all company filings through EDGAR—this project adds orchestration and transformation layers to make that data pipeline-ready. It will be maintained by me, the creator, mrbt03.

**The SEC's data offerings are genuinely excellent.** Before building infrastructure, explore what they provide directly.

---

## License

MIT
