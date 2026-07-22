#!/usr/bin/env bash
# FILE SINH TỰ ĐỘNG - đừng sửa tay. Nguồn: metadata/datasets/*.yaml + generators/dlq.py + hằng số hạ tầng trong generators/topic_manifest.py. Sinh lại: python -m dataplatform.cli write
#
# Tạo mọi topic mà hệ thống cần, idempotent. Chạy trước khi tắt
# auto.create.topics (xem ADR-0020). Vd:
#   docker exec bigdata-kafka bash /opt/bitnami/kafka/create-topics.sh
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"

# --- Topic dữ liệu (sinh từ dataset contract) ---
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic bankdb.public.accounts --partitions 1 --replication-factor 1   # dataset:bank.public.accounts
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic bankdb.public.customers --partitions 1 --replication-factor 1   # dataset:bank.public.customers
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic bankdb.public.transactions --partitions 1 --replication-factor 1   # dataset:bank.public.transactions
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic bankdb.public.transfers --partitions 1 --replication-factor 1   # dataset:bank.public.transfers
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic fraud-alerts --partitions 1 --replication-factor 1   # dataset:bank.alerts.fraud_alerts
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic metrics.breakdown --partitions 1 --replication-factor 1   # dataset:bank.metric.breakdown
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic metrics.kpi --partitions 1 --replication-factor 1   # dataset:bank.metric.kpi
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic metrics.timeseries --partitions 1 --replication-factor 1   # dataset:bank.metric.timeseries
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic metrics.topn --partitions 1 --replication-factor 1   # dataset:bank.metric.topn
# --- Topic dead-letter (một cái mỗi sink connector) ---
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic dlq.es-sink-accounts --partitions 1 --replication-factor 1   # dlq:es-sink-accounts
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic dlq.es-sink-customers --partitions 1 --replication-factor 1   # dlq:es-sink-customers
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic dlq.es-sink-fraud-alerts --partitions 1 --replication-factor 1   # dlq:es-sink-fraud-alerts
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic dlq.es-sink-transactions --partitions 1 --replication-factor 1   # dlq:es-sink-transactions
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic dlq.es-sink-transfers --partitions 1 --replication-factor 1   # dlq:es-sink-transfers
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic dlq.s3-sink-cdc --partitions 1 --replication-factor 1   # dlq:s3-sink-cdc
# --- Topic pipeline nội bộ ---
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic dlq.events --partitions 1 --replication-factor 1   # pipeline:dlq-processor
# --- Topic nội bộ Kafka Connect (compacted) ---
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic __debezium-heartbeat.bankdb --partitions 1 --replication-factor 1   # infra:debezium-heartbeat
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic _connect_configs --partitions 1 --replication-factor 1 --config cleanup.policy=compact  # infra:kafka-connect
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic _connect_offsets --partitions 25 --replication-factor 1 --config cleanup.policy=compact  # infra:kafka-connect
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic _connect_status --partitions 5 --replication-factor 1 --config cleanup.policy=compact  # infra:kafka-connect
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic _schemas --partitions 1 --replication-factor 1 --config cleanup.policy=compact  # infra:schema-registry

echo "Đã đảm bảo tồn tại 21 topic."
