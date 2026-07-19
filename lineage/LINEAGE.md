# Lineage & Data Catalog

> FILE SINH TỰ ĐỘNG từ `metadata/` — đừng sửa tay. Sinh lại: `python -m dataplatform.cli write`.

## 1. Sơ đồ dòng chảy dữ liệu

```mermaid
flowchart LR
  bank_alerts_fraud_alerts["bank.alerts.fraud_alerts"] -->|es-sink-fraud-alerts| es_fraud_alerts["es:fraud-alerts"]
  bank_metric_breakdown["bank.metric.breakdown"] -->|ch-kafka-engine| clickhouse_metrics_breakdown["clickhouse:metrics.breakdown"]
  bank_metric_kpi["bank.metric.kpi"] -->|ch-kafka-engine| clickhouse_metrics_kpi["clickhouse:metrics.kpi"]
  bank_metric_timeseries["bank.metric.timeseries"] -->|ch-kafka-engine| clickhouse_metrics_timeseries["clickhouse:metrics.timeseries"]
  bank_metric_topn["bank.metric.topn"] -->|ch-kafka-engine| clickhouse_metrics_topn["clickhouse:metrics.topn"]
  bank_public_accounts["bank.public.accounts"] -->|es-sink-accounts| es_accounts["es:accounts"]
  bank_public_accounts["bank.public.accounts"] -->|s3-sink-cdc| s3_data_lake_bronze["s3:data-lake-bronze"]
  bank_public_accounts["bank.public.accounts"] -->|spark| silver_enriched_transactions["silver:enriched_transactions"]
  bank_public_customers["bank.public.customers"] -->|es-sink-customers| es_customers["es:customers"]
  bank_public_customers["bank.public.customers"] -->|s3-sink-cdc| s3_data_lake_bronze["s3:data-lake-bronze"]
  bank_public_customers["bank.public.customers"] -->|spark| silver_enriched_transactions["silver:enriched_transactions"]
  bank_public_transactions["bank.public.transactions"] -->|flink| bank_alerts_fraud_alerts["bank.alerts.fraud_alerts"]
  bank_public_transactions["bank.public.transactions"] -->|flink| bank_metric_breakdown["bank.metric.breakdown"]
  bank_public_transactions["bank.public.transactions"] -->|flink| bank_metric_kpi["bank.metric.kpi"]
  bank_public_transactions["bank.public.transactions"] -->|flink| bank_metric_timeseries["bank.metric.timeseries"]
  bank_public_transactions["bank.public.transactions"] -->|flink| bank_metric_topn["bank.metric.topn"]
  bank_public_transactions["bank.public.transactions"] -->|es-sink-transactions| es_transactions["es:transactions"]
  bank_public_transactions["bank.public.transactions"] -->|s3-sink-cdc| s3_data_lake_bronze["s3:data-lake-bronze"]
  bank_public_transactions["bank.public.transactions"] -->|spark| silver_enriched_transactions["silver:enriched_transactions"]
  bank_public_transfers["bank.public.transfers"] -->|es-sink-transfers| es_transfers["es:transfers"]
  bank_public_transfers["bank.public.transfers"] -->|s3-sink-cdc| s3_data_lake_bronze["s3:data-lake-bronze"]
  silver_enriched_transactions["silver:enriched_transactions"] -->|spark| gold_customer_lifetime_metrics["gold:customer_lifetime_metrics"]
  silver_enriched_transactions["silver:enriched_transactions"] -->|spark| gold_daily_transaction_summary["gold:daily_transaction_summary"]
  silver_enriched_transactions["silver:enriched_transactions"] -->|spark| gold_high_risk_transactions["gold:high_risk_transactions"]
  silver_enriched_transactions["silver:enriched_transactions"] -->|spark| iceberg_lakehouse_silver_enriched_transactions["iceberg:lakehouse.silver.enriched_transactions"]
```


## 2. Data catalog — ai sở hữu, PII ở đâu

| Dataset | Layer | Owner | Cột PII | Tags |
|---|---|---|---|---|
| `bank.alerts.fraud_alerts` | alert | team-fraud | — | fraud, alert, generated |
| `bank.metric.breakdown` | metric | team-analytics | — | metric, realtime, dashboard |
| `bank.metric.kpi` | metric | team-analytics | — | metric, realtime, dashboard |
| `bank.metric.timeseries` | metric | team-analytics | — | metric, realtime, dashboard |
| `bank.metric.topn` | metric | team-analytics | — | metric, realtime, dashboard |
| `bank.public.accounts` | oltp | team-core-banking | account_number | banking, dimension |
| `bank.public.customers` | oltp | team-core-banking | full_name, email, phone | banking, dimension, pii |
| `bank.public.transactions` | oltp | team-core-banking | — | banking, fact, high-throughput |
| `bank.public.transfers` | oltp | team-core-banking | — | banking, fact, lifecycle |

## 3. PII chảy tới đâu

| Dataset PII | Cột | Chảy tới |
|---|---|---|
| `bank.public.accounts` | account_number | es:accounts, s3:data-lake-bronze, silver:enriched_transactions |
| `bank.public.customers` | full_name, email, phone | es:customers, s3:data-lake-bronze, silver:enriched_transactions |

## 4. Lineage cột (Flink metric)

| Cột đầu ra | Từ cột nguồn | Biểu thức |
|---|---|---|
| `bank.metric.breakdown.tx_type` | `bank.public.transactions.transaction_type` | ``after`.transaction_type` |
| `bank.metric.breakdown.tx_count` | — (không cột nguồn cụ thể) | `COUNT(*)` |
| `bank.metric.breakdown.total_value` | `bank.public.transactions.amount` | `SUM(CAST(`after`.amount AS DECIMAL(19, 4)))` |
| `bank.metric.breakdown.success_count` | `bank.public.transactions.status` | `COUNT(*) FILTER (WHERE `after`.status = 'completed')` |
| `bank.metric.breakdown.failed_count` | `bank.public.transactions.status` | `COUNT(*) FILTER (WHERE `after`.status = 'failed')` |
| `bank.metric.kpi.total_count` | — (không cột nguồn cụ thể) | `COUNT(*)` |
| `bank.metric.kpi.total_value` | `bank.public.transactions.amount` | `SUM(CAST(`after`.amount AS DECIMAL(19, 4)))` |
| `bank.metric.kpi.success_count` | `bank.public.transactions.status` | `COUNT(*) FILTER (WHERE `after`.status = 'completed')` |
| `bank.metric.kpi.failed_count` | `bank.public.transactions.status` | `COUNT(*) FILTER (WHERE `after`.status = 'failed')` |
| `bank.metric.kpi.success_rate` | `bank.public.transactions.status` | `CAST(COUNT(*) FILTER (WHERE `after`.status = 'completed') * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5, 2))` |
| `bank.metric.kpi.active_users` | `bank.public.transactions.account_id` | `COUNT(DISTINCT `after`.account_id)` |
| `bank.metric.timeseries.tx_type` | `bank.public.transactions.transaction_type` | ``after`.transaction_type` |
| `bank.metric.timeseries.tx_count` | — (không cột nguồn cụ thể) | `COUNT(*)` |
| `bank.metric.timeseries.total_amount` | `bank.public.transactions.amount` | `SUM(CAST(`after`.amount AS DECIMAL(19, 4)))` |
| `bank.metric.topn.account_id` | `bank.public.transactions.account_id` | ``after`.account_id` |
| `bank.metric.topn.tx_count` | — (không cột nguồn cụ thể) | `COUNT(*)` |
| `bank.metric.topn.total_value` | `bank.public.transactions.amount` | `SUM(CAST(`after`.amount AS DECIMAL(19, 4)))` |
