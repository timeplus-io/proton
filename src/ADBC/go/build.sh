#!/bin/bash

# Set the Go environment variables
export GO111MODULE=on
export CGO_ENABLED=1

# Build the Go ADBC Interface as a C shared object
go build -o adbc_driver.so -buildmode=c-shared adbc_driver.go
