# Financial Pipeline (BigQuery + dbt + Python)

A complete end-to-end data pipeline for ingesting, transforming, and modeling financial market data (stocks & ETFs).

## Architecture

- **Python ingestion** → loads raw Yahoo Finance data into BigQuery (`raw.raw_prices`)
- **dbt staging** → cleans and standardizes raw data (`stg_prices`)
- **dbt marts** → produces analytics-ready models (`prices_latest`)
- **Tests** → ensures data quality (not_null, unique)
- **GitHub** → full version control and portfolio visibility

## Models

### `stg_prices`
Standardized staging model for raw price data.

### `prices_latest`
One row per ticker with the most recent available price.