#!/bin/bash

set -e

PROJECT_ROOT=$(cd "$(dirname "$0")" && pwd)

echo "=========================================="
echo "Starting Streaming Pipelines"
echo "=========================================="

source "$PROJECT_ROOT/.venv/bin/activate"

echo "Starting Bronze Pipeline..."
python "$PROJECT_ROOT/src/spark/bronze_spark_pipeline.py" &
BRONZE_PID=$!

sleep 10

echo "Starting Silver Pipeline..."
python "$PROJECT_ROOT/src/spark/silver_spark_pipeline.py" &
SILVER_PID=$!

sleep 10

echo "Starting Gold Events Pipeline..."
python "$PROJECT_ROOT/src/spark/gold/gold_events_per_minute.py" &
GOLD_EVENTS_PID=$!

sleep 10

echo "Starting Gold Category Pipeline..."
python "$PROJECT_ROOT/src/spark/gold/gold_category_metrics.py" &
GOLD_CATEGORY_PID=$!


echo ""
echo "=========================================="
echo "All pipelines started"
echo "=========================================="

echo "Bronze PID        : $BRONZE_PID"
echo "Silver PID        : $SILVER_PID"
echo "Gold Events PID   : $GOLD_EVENTS_PID"
echo "Gold Category PID : $GOLD_CATEGORY_PID"

wait