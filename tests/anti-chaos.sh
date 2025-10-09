#!/bin/bash


while true
do
    ./programs/proton server --config /code/cluster/config-no-logs-$1.yaml

    echo "killed, and respawning"
    sleep 300
done
