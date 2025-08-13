#!/usr/bin/env bash
set -euo pipefail
BROKER="${KAFKA_BROKER:-localhost:9092}"
topics=("news_headlines" "price_ticks" "impact_scores")
for t in "${topics[@]}"; do
  echo "Creating topic $t (if not exists) ..."
  docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic "$t" --replication-factor 1 --partitions 1 || true
done
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
