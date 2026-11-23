#!/usr/bin/env sh
# The NexusMQ command-line tool, e.g.: ./nexusmq.sh --url http://localhost:5680 stats
DIR="$(dirname "$0")/cli"
if [ -x "$DIR/nexusmq" ]; then exec "$DIR/nexusmq" "$@"; fi
exec dotnet "$DIR/nexusmq.dll" "$@"
