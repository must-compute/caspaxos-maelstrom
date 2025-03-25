#!/bin/bash

MAELSTROM="./maelstrom/maelstrom"
BINARY="./target/debug/cas-paxos"

if [ "$1" = "lin-kv" ]; then
  cargo build && $MAELSTROM test -w lin-kv --bin $BINARY --time-limit 35 --log-stderr --node-count 3 --concurrency 2n --rate 10 # --latency 120
elif [ "$1" = "lin-kv-nemesis" ]; then
  cargo build && $MAELSTROM test -w lin-kv --bin $BINARY --time-limit 15 --log-stderr --node-count 3 --concurrency 4n --rate 100 --nemesis partition --nemesis-interval 4 # --latency 120
else
  echo "unknown command"
fi
