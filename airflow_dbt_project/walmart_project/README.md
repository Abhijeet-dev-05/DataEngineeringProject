# walmart_project — dbt Project

This is the dbt project component of the Walmart Data Engineering Pipeline. It handles all data transformations from Bronze through Silver to Gold layers inside a Databricks Unity Catalog (`walmart` catalog).

## Running the Project

```bash
# Run all models
dbt run

# Run a specific layer
dbt run --select silver_t
dbt run --select silver_b
dbt run --select gold/ephermeral
dbt run --select gold/fact

# Run snapshots (SCD Type 2 dimensions)
dbt snapshot

# Run tests
dbt test
dbt test --select silver_t

# Check source freshness
dbt source freshness

# Clean compiled artifacts
dbt clean
```

## Layer Overview

| Layer | Schema | Materialization | Description |
|---|---|---|---|
| Bronze | `walmart.bronze` | Source (external) | CDC-loaded raw tables from Databricks job |
| Silver Technical | `walmart.silver_t` | incremental | One model per entity, CDC watermark merge |
| Silver Business | `walmart.silver_b` | table | One Big Table — all 6 entities joined |
| Gold Ephemeral | — | ephemeral | De-duplicated entity views, compiled inline |
| Gold Dimensions | `walmart.gold` | snapshot (SCD2) | 5 SCD Type 2 dimension tables |
| Gold Fact | `walmart.gold` | table | Grain-level `fact_orders` table |

## Resources

- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt Slack Community](https://community.getdbt.com/)
- [dbt-databricks adapter docs](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)
