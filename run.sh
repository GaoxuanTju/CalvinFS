#!/bin/bash
bin/scripts/cluster --command="kill"
cd src
make -j
cd ..
bin/scripts/cluster --command="start" --experiment=12 --clients=40 --max_active=1000 --max_running=100 --local_percentage=100 