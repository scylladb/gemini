#!/bin/sh

test_gemini() {
    echo -n "Compile gemini"
    ./scripts/compile-and-install-gemini.sh || exit 1
    oracle="scylla"
    if [ "$1" = "cassandra" ]; then
      oracle="cassandra"
      shift
    fi

    echo -n "Preparing environment with $oracle as oracle"
    ./scripts/prepare-environment.sh $oracle || exit 1

    echo -n "Running test with $oracle as oracle for 'gemini $@' ... "
    ./scripts/run-gemini-test.sh --drop-schema -c 8 $@
    if [ $? -eq 0 ]; then
       echo "OK"
    else
       echo "FAILED"
    fi

    exit $?
}

test_gemini $@
