# Project Overview

This dbt project is designed to transform raw data into a structured data warehouse.

## Models

### Fact Sales

The `fact_sales` model contains sales transaction data, including:

- Opportunity ID
- Geographic ID
- Client ID
- Employee ID
- Date ID
- Probability of Win
- Total value in TND
- Total value in base currency

### Dimension Tables

The project includes several dimension tables:

- **dim_employee:** Employee information
- **dim_date:** Date information
- **dim_geographic:** Geographic information
- **dim_client:** Client information

## Data Lineage

![Data Lineage Diagram](data_lineage.png)
