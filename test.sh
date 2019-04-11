#!/bin/sh

launcher_cmd=./scripts/gemini-launcher

test_gemini() {
    echo -n "Running test for 'gemini $@' ... "
    $launcher_cmd --duration 1s --drop-schema $@ > /dev/null
    if [ $? -eq 0 ]
    then
       echo "OK"
    else
       echo "FAILED"
    fi
    exit $?
}

test_gemini "--non-interactive"
