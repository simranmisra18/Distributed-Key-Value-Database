#!/bin/sh
DIR=$(dirname "$0")
HOSTNAME=$(hostname)
mkdir -p "$DIR/logs"
cd "$DIR/target" || exit 1
./server server-hostnames.txt "$HOSTNAME" 2> "../logs/$HOSTNAME-stderr.log"
