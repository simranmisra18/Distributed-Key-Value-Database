#!/bin/sh
DIR=$(dirname "$0")
cd "$DIR/target" || exit 1
./client "$@"
