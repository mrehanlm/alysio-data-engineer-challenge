# Overview
This ETL pipeline processes and loads sales-related data from CSV and JSON files into a database using SQLAlchemy and Pandas.

NOTE: While reading data from raw files, it ensures the use of Pandas `chunksize` to efficiently handle large datasets and optimize memory usage.

Next, the pipeline cleans, validates, and inserts records for companies, contacts, opportunities, and activities while handling sub-entities like industries, products, and statuses. The pipeline ensures data integrity by mapping foreign keys, filtering duplicates, and logging validation errors. Bulk operations optimize performance for large datasets. The process runs end-to-end with error handling, logging, and automated validation reporting.

Please have a look at [more detailed documentation on ETL process](ETL.md).

## Getting Started

### 1. Run ETL Pipeline

#### Prerequisites:
1. Linux / MacOS machine – As `Makefile` targets are written to only support these platforms for now. Windows support can be added in future.
2. Install `make` CLI – This is required in order to be able to use make targets from `Makefile`
      - MacOS: `brew install make`
      - Linux: `sudo apt-get install build-essential`
3. Install Python (>= 3.10)

Now, ETL pipeline can be run via single command (i.e. `make run-etl`). It will:

1. Setup virtual environment
2. Install dependencies
3. Setup database schema
4. Apply DB migrations through alembic
5. Run ETL pipeline

```bash
❯ make run-etl
# Output:
...
Running ETL pipeline
venv/bin/python -m etl.pipeline
INFO:__main__:
 ETL Pipeline has been successfully completed.
 There were validation errors. Please check them out at: data/errors/
Done
...
```

Any sort of data / validation errors encountered during the ETL pipeline execution are written in `data/errors/` directory for further inspection.

### 2. Database (SQLite for now)
The schema is defined declaratively via SQLAlchemy and upon updating the schema, migrations need to be generated and applied via alembic as seen below:


#### Generate alembic revisions
```bash
# Generates migrations in under `alembic/versions/` directory
❯ make generate-revision NAME=<enter you message here>
```

#### Alembic apply DB migrations

```bash
# Apply DB migrations
❯ make alembic-upgrade
```

#### Alembic downgrade by 1 revision
```bash
# Un-Apply a DB migration
❯ make alembic-downgrade
```

### 3. Unit Tests
Pytest has been used to write Unit tests for ETL pipeline. The unit tests coverage is covering:

1. The incremental behavior of the pipeline
2. Data Validation / Cleaning,  Transformation and Ingestion into the DB

#### Run Test Cases
Unit tests can be run by:

```bash
❯ make tests
```

### 4. Sample Queries
The sample queries can be found at: `/src/sample_queries.py`

#### Run Sample Queries / Generate SQLs
```bash
❯ make run-sample-queries

# Output:

...
-------------------------------------------------
 Companies that are Customers with Revenue > 1M (LIMIT 10)
-------------------------------------------------

 --------- SQL Start ---------

 SELECT companies.id AS companies_id,
       companies.industry_id AS companies_industry_id,
       companies.name AS companies_name,
       companies.domain AS companies_domain,
       companies.size AS companies_size,
       companies.country AS companies_country,
       companies.created_date AS companies_created_date,
       companies.is_customer AS companies_is_customer,
       companies.annual_revenue AS companies_annual_revenue
FROM companies
WHERE companies.is_customer IS 1
  AND companies.annual_revenue > ?
LIMIT ?
OFFSET ?

--------- SQL End ---------


OUTPUT:

INFO:__main__:Company: COMPANY 89 - Revenue: 7978590.0
INFO:__main__:Company: COMPANY 48 - Revenue: 2910568.0
INFO:__main__:Company: COMPANY 73 - Revenue: 8084089.0
INFO:__main__:Company: COMPANY 57 - Revenue: 2650252.0
INFO:__main__:Company: COMPANY 100 - Revenue: 8691825.0
INFO:__main__:Company: COMPANY 19 - Revenue: 9031441.0
INFO:__main__:Company: COMPANY 71 - Revenue: 9095220.0
INFO:__main__:Company: COMPANY 83 - Revenue: 3651406.0
INFO:__main__:Company: COMPANY 60 - Revenue: 1108707.0
INFO:__main__:Company: COMPANY 22 - Revenue: 5018233.0

 ... so on ...
```

### 5. Misc – Help
You can `make help` for more convenient make targets which are helpful during development.
```bash
❯ make help

Usage:
  make <target>

Targets:

 check-lint-and-formatting  Execute check of lint and formatting using existing pre-commit hooks
 alembic-upgrade            Upgrade to the latest migration (head)
 alembic-downgrade          Downgrade to the previous migration
 alembic-history            Show the migration history
 alembic-current            Show the current migration
 generate-revision          Generate a new migration. Example usage: `make generate-revision NAME="add_new_table"`
 alembic-downgrade-all      Downgrade to the base migration
 run-etl                    Run ETL pipeline
 docs                       Build and serve docs

```
