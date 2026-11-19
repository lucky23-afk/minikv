#!/usr/bin/env bash
set -e

echo "Starting miniKV 3-node cluster + coordinator..."

# Create directories
mkdir -p bin data/node-1 data/node-2 data/node-3

# Build binaries
go build -o bin/minikv-node ./cmd/minikv-node
go build -o bin/minikv-coord ./cmd/minikv-coord

# Launch node-1
./bin/minikv-node --id node-1 --addr 127.0.0.1:7000 --http-addr 127.0.0.1:7001 --data-dir ./data/node-1 &
PID1=$!

# Launch node-2
./bin/minikv-node --id node-2 --addr 127.0.0.1:7010 --http-addr 127.0.0.1:7011 --data-dir ./data/node-2 &
PID2=$!

# Launch node-3
./bin/minikv-node --id node-3 --addr 127.0.0.1:7020 --http-addr 127.0.0.1:7021 --data-dir ./data/node-3 &
PID3=$!

sleep 1

# Launch coordinator
./bin/minikv-coord --addr :8080 --nodes "node-1=127.0.0.1:7000=127.0.0.1:7001,node-2=127.0.0.1:7010=127.0.0.1:7011,node-3=127.0.0.1:7020=127.0.0.1:7021" &
PID_COORD=$!

echo "miniKV cluster started!"
echo "Dashboard accessible at http://localhost:8080"
echo "Press CTRL+C to stop all services."

trap "kill $PID1 $PID2 $PID3 $PID_COORD; exit 0" SIGINT SIGTERM
wait
