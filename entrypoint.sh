#!/usr/bin/env bash

echo 'Starting up streams aggregator...' >&2

if [[ $STARTUP_SLEEP_TIMEOUT ]]; then
  echo "sleeping for $STARTUP_SLEEP_TIMEOUT seconds" >&2
  sleep $STARTUP_SLEEP_TIMEOUT
fi

exec ./bin/kafka-dvs-streams
