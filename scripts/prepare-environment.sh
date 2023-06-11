#!/bin/bash

docker-compose --log-level WARNING -f scripts/docker-compose-$1.yml up -d

ORACLE_NAME=gemini-oracle
TEST_NAME=gemini-test

echo "Waiting for ${ORACLE_NAME} to start"
until docker logs ${ORACLE_NAME} 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 2; done
echo "Waiting for ${TEST_NAME} to start"
until docker logs ${TEST_NAME} 2>&1 | grep "Starting listening for CQL clients" > /dev/null; do sleep 2; done
