#!/bin/bash

set -e

ORACLE_NAME=gemini-oracle
TEST_NAME=gemini-test

ORACLE_IP=$(docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' ${ORACLE_NAME})
TEST_IP=$(docker inspect --format='{{ .NetworkSettings.Networks.gemini.IPAddress }}' ${TEST_NAME})

GEMINI_CMD=gemini
SEED=$(date +%s%N)
ABS_GEMINI_PATH=$(whereis ${GEMINI_CMD})
ABS_GEMINI_PATH=${ABS_GEMINI_PATH//gemini: /}

GOBIN="$(dirname $ABS_GEMINI_PATH)"
$GEMINI_CMD \
	--duration=10m \
	--fail-fast \
	--seed=${SEED} \
	--dataset-size=small \
	--test-cluster=${TEST_IP} \
	--oracle-cluster=${ORACLE_IP} \
	"$@"
exit $?
