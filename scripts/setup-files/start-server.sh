#!/usr/bin/env sh
# Starts NexusMQ in this terminal. Dashboard: http://localhost:5680  (Ctrl+C stops it)
# Another address: NEXUSMQ_URLS=http://0.0.0.0:5680 ./start-server.sh
# Settings: server/appsettings.json (access keys, data folder, cluster). Data is kept in server/data.
cd "$(dirname "$0")/server" || exit 1
URLS="${NEXUSMQ_URLS:-http://localhost:5680}"
if [ -x ./NexusMQ.Server ]; then exec ./NexusMQ.Server --urls "$URLS" "$@"; fi
exec dotnet NexusMQ.Server.dll --urls "$URLS" "$@"
