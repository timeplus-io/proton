#!/bin/bash
i=0

while true
do
    processes=`ps -ef | grep "proton server" | grep -v grep | awk '{print $2}'`

    to_kill=$((i % 3))
    echo "killing index=$to_kill"

    j=0

    for p in $processes
    do
        if [[ $j == $to_kill ]]; then
            kill $p
            echo "killed $p"
        fi

        ((++j))
    done

    ((++i))

    echo "kill round $i"

    sleep 350
done
