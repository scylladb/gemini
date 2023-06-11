#!/bin/bash

function quit () {
    echo $2
    exit $1
}

go mod download || quit $? "Downloading dependencies failed"
go install ./... || quit $? "Compilation failed"
