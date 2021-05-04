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
  rm -rf raftexample-*
  # ./raftexample -nodes 2 -file $TRACES_DIR/first-leader/full.json "$@"
  # ./raftexample -nodes 3 -file $TRACES_DIR/double-leader/full.json "$@"
  ./raftexample -nodes 3 -file $TRACES_DIR/first-commit/full.json "$@"

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

if [ -n "$1" ]; then
  fn="$1"
  shift
  "$fn" "$@"
else
  fd . | entr -c -r ./run.sh build "$@"
fi
