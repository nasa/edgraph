#!/bin/bash

# wait-for-it.sh script to wait for Neo4j to be ready
host="$1"
port="$2"
shift 2
cmd="$@"

while ! nc -z $host $port; do
  echo "Waiting for Neo4j to be ready..."
  sleep 1
done

exec $cmd

