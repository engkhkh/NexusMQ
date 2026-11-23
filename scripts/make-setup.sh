#!/usr/bin/env bash
# Builds a ready-to-run NexusMQ server setup in the project's setup/ folder.
#   ./scripts/make-setup.sh                 # portable: needs the .NET 10 runtime on the machine
#   ./scripts/make-setup.sh linux-x64       # self-contained (no .NET needed); also win-x64, osx-arm64, ...
#   OUTPUT=/tmp/setup ./scripts/make-setup.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUNTIME="${1:-}"
OUT="${OUTPUT:-$ROOT/setup}"
EXTRA=(-p:UseAppHost=false)   # portable: run with dotnet, no platform launcher
[[ -n "$RUNTIME" ]] && EXTRA=(-r "$RUNTIME" --self-contained true -p:PublishSingleFile=true)
[[ -n "${VERSION:-}" ]] && EXTRA+=("-p:Version=$VERSION")   # release builds: stamp the tag's version
rm -rf "$OUT/server" "$OUT/cli"
dotnet publish "$ROOT/src/NexusMQ.Server" -c Release -o "$OUT/server" "${EXTRA[@]}"
dotnet publish "$ROOT/src/NexusMQ.Cli" -c Release -o "$OUT/cli" "${EXTRA[@]}"
cp "$ROOT"/scripts/setup-files/* "$OUT/"
perl -pi -e 's/\r?\n/\r\n/' "$OUT"/*.cmd   # batch files: Windows line endings, even when built on Linux
chmod +x "$OUT"/*.sh
echo "NexusMQ setup ready in $OUT  (./start-server.sh, then http://localhost:5680)"
