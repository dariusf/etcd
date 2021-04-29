#!/bin/bash

set -x

build_traces() {
  set -e
  go build traces.go
  ./traces
}

build() {
  set -e
  go build -o raftexample
  ./raftexample $PROJECT_DIR/full.json
  #--id 1 --cluster http://127.0.0.1:12379 --port 12380
  echo "rc: $?"
}

cluster1() {
  ./raftexample --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 12380
}

cluster2() {
  ./raftexample --id 2 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 22380
}

cluster3() {
  ./raftexample --id 3 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 32380
}

sanity() {
  curl -L http://127.0.0.1:12380/my-key1 -XPUT -d hello
  sleep 1
  curl -L http://127.0.0.1:12380/my-key1
}

trace() {
  java -cp $TLA_TOOLS -XX:+UseParallelGC tlc2.TLC $PROJECT_DIR/tla/raft.tla > /tmp/raft.log
  java -jar $TLA2JSON -d /tmp/raft.log > /tmp/raft.json
  ./convert.py raft.json > /tmp/raft1.json
}

if [ "$1" = "1" ]; then
  build
  # build_traces
elif [ -n "$1" ]; then
  $1
else
  fd . | entr -c -r ./run.sh 1
fi
