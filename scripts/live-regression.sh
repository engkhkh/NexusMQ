#!/usr/bin/env bash
# shellcheck disable=SC2046  # cluster_env output is intentionally word-split into VAR=value args
# NexusMQ live regression: starts real servers and checks every feature end to end
# (samples, CLI, access keys, rate limits, audit, message browser, HTTPS, metrics/dashboard,
#  primary/standby failover, Raft cluster leader kill, membership changes, minority refusal, benchmarks).
#
#   ./scripts/live-regression.sh            # everything (~6-8 minutes)
#   ONLY="cluster ha" ./scripts/live-regression.sh
#   OLD_SERVER=/path/to/older/NexusMQ.Server.dll ./scripts/live-regression.sh   # adds the upgrade checks
#
# Needs: .NET 10 SDK, curl, python3, openssl. Optional: node (dashboard JS check), actionlint, docker.
# Exit code = number of failed checks.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${WORK:-/tmp/nexusmq-regression}"
rm -rf "$WORK"; mkdir -p "$WORK"
PASSES=0; FAILS=0; WARNS=0; SKIPS=0
RESULTS=()
PIDS=()

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
pass() { PASSES=$((PASSES+1)); RESULTS+=("PASS  $*"); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
fail() { FAILS=$((FAILS+1));   RESULTS+=("FAIL  $*"); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }
warn() { WARNS=$((WARNS+1));   RESULTS+=("WARN  $*"); printf '  \033[33mWARN\033[0m %s\n' "$*"; }
skip() { SKIPS=$((SKIPS+1));   RESULTS+=("SKIP  $*"); printf '  SKIP %s\n' "$*"; }
check() { local name="$1"; shift; if "$@"; then pass "$name"; else fail "$name"; fi; }
want() { [[ -z "${ONLY:-}" || " $ONLY " == *" $1 "* ]]; }

cleanup() { for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done; }
trap cleanup EXIT

SRV="$ROOT/src/NexusMQ.Server/bin/Release/net10.0/NexusMQ.Server.dll"
CLI_DLL="$ROOT/src/NexusMQ.Cli/bin/Release/net10.0/nexusmq.dll"
CHAOS_DLL="$ROOT/tools/NexusMQ.ChaosClient/bin/Release/net10.0/nexusmq-chaos.dll"
cli() { dotnet "$CLI_DLL" "$@"; }

# start <name> <port> [VAR=value ...]   → server PID in $LAST_PID, waits until healthy
start() {
  local name="$1" port="$2"; shift 2
  local scheme="http"; [[ " $* " == *"HTTPS=1"* ]] && scheme="https"
  local args=(); for a in "$@"; do [[ "$a" == HTTPS=1 ]] || args+=("$a"); done
  local urls="$scheme://localhost:$port"
  setsid env NexusMQ__DataDirectory="$WORK/$name-data" "${args[@]}" dotnet "$SRV" --urls "$urls" >> "$WORK/$name.log" 2>&1 < /dev/null &
  LAST_PID=$!; PIDS+=("$LAST_PID"); echo "$LAST_PID" > "$WORK/$name.pid"
  for _ in $(seq 1 60); do curl -ksf "$urls/api/health" >/dev/null 2>&1 && return 0; sleep 0.5; done
  echo "server $name did not start; log:"; tail -20 "$WORK/$name.log"; return 1
}
stop() { local pid; pid=$(cat "$WORK/$1.pid" 2>/dev/null) && kill -9 "$pid" 2>/dev/null; sleep 0.5; }
leader_port() { curl -s "localhost:$1/api/health" | python3 -c "import json,sys;print((json.load(sys.stdin).get('leader') or '').rsplit(':',1)[-1])"; }
json_get() { python3 -c "import json,sys;d=json.load(sys.stdin);print(eval(sys.argv[1]))" "$1"; }
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

# ------------------------------------------------------------------------------------------ build
say "Build (Release, whole solution)"
if dotnet build "$ROOT/NexusMQ.sln" -c Release > "$WORK/build.log" 2>&1; then pass "solution builds"
elif grep -q "NU1301" "$WORK/build.log"; then
  # No access to nuget.org (offline / locked-down machine): build everything that needs no packages.
  warn "nuget.org unreachable; building all projects except the xUnit test project"
  OK=1
  for p in "$ROOT"/src/*/*.csproj "$ROOT"/samples/*/*.csproj "$ROOT"/tools/*/*.csproj; do
    dotnet build "$p" -c Release >> "$WORK/build.log" 2>&1 || { OK=0; echo "  build failed: $p"; }
  done
  if [[ $OK == 1 ]]; then pass "all non-test projects build"; else fail "projects build (see $WORK/build.log)"; exit 1; fi
else fail "solution builds (see $WORK/build.log)"; exit 1; fi
WARNINGS=$(grep -cE "warning CS" "$WORK/build.log" || true)
[[ "$WARNINGS" == 0 ]] && pass "no compiler warnings" || warn "$WARNINGS compiler warnings"

# ------------------------------------------------------------------------------------------ samples
if want samples; then
  say "Samples: producer + consumer on one server (queues, topics, exchanges, scheduling, dead-lettering)"
  start single 5700
  dotnet "$ROOT/samples/Sample.Producer/bin/Release/net10.0/Sample.Producer.dll" http://localhost:5700 > "$WORK/producer.log" 2>&1
  check "producer ran" grep -q "Dashboard" "$WORK/producer.log"
  timeout -s INT 25 dotnet "$ROOT/samples/Sample.Consumer/bin/Release/net10.0/Sample.Consumer.dll" http://localhost:5700 > "$WORK/consumer.log" 2>&1
  check "consumer processed 11 orders (10 + 1 scheduled)" test "$(grep -c '^\[orders\]' "$WORK/consumer.log")" -eq 11
  check "topic subscription with filter got 10 order events" test "$(grep -c 'events/billing' "$WORK/consumer.log")" -eq 10
  check "topic exchange routed *.error to errors queue" grep -q "\[errors\] payment gateway timeout" "$WORK/consumer.log"
  check "poison message dead-lettered after MaxDeliveryCount" grep -q "\[dead-letter\] poison-1: MaxDeliveryCountExceeded" "$WORK/consumer.log"

  say "Sample.WebApp: embedded broker, DI handlers, SQL filter, ordered sessions"
  setsid env NexusMQ__Url=http://localhost:5701 NexusMQ__DataDirectory="$WORK/webapp-data" \
    dotnet "$ROOT/samples/Sample.WebApp/bin/Release/net10.0/Sample.WebApp.dll" --urls http://localhost:5701 > "$WORK/webapp.log" 2>&1 < /dev/null &
  PIDS+=("$!"); WEBAPP_PID=$!
  for _ in $(seq 1 60); do curl -sf localhost:5701/api/health >/dev/null && break; sleep 0.5; done
  for i in 1 2; do curl -s -X POST localhost:5701/orders -H 'Content-Type: application/json' -d "{\"orderId\":\"R-$i\",\"customer\":\"c$i\",\"total\":$((i*70))}" >/dev/null; done
  sleep 4
  curl -s localhost:5701/orders > "$WORK/webapp-orders.json"
  check "typed handler processed both orders" python3 -c "import json;d=json.load(open('$WORK/webapp-orders.json'));assert all('processed' in d[k] for k in ['R-1','R-2'])"
  check "SQL-filtered subscription flagged only the large order" python3 -c "import json;d=json.load(open('$WORK/webapp-orders.json'));assert any('billing' in e for e in d['R-2']) and not any('billing' in e for e in d['R-1'])"
  check "session steps handled in order with session state" python3 -c "
import json;d=json.load(open('$WORK/webapp-orders.json'))
for k in ['R-1','R-2']:
  s=[e for e in d[k] if e.startswith('shipment')]
  assert s==['shipment: start -> picked','shipment: picked -> packed','shipment: packed -> shipped'],s"
  kill -9 "$WEBAPP_PID" 2>/dev/null
fi

# ------------------------------------------------------------------------------------------ CLI + message browser
if want cli; then
  say "CLI: entities, send/peek/receive, dead letters, resend, delete-message, audit"
  [[ -f "$WORK/single.pid" ]] || start single 5700
  C=(--url http://localhost:5700)
  check "create queue" cli "${C[@]}" create queue clitest --max-delivery 1
  check "send with properties" bash -c "echo '{\"id\":1}' | dotnet '$CLI_DLL' --url http://localhost:5700 send clitest --subject order.created --prop region=eu >/dev/null"
  check "peek --json shows the property" bash -c "dotnet '$CLI_DLL' --url http://localhost:5700 peek clitest --json | grep -q '\"region\":\"eu\"'"
  cli "${C[@]}" receive clitest --abandon > /dev/null
  DLSEQ=$(cli "${C[@]}" peek 'clitest/$deadletter' --json | head -1 | json_get "d['sequenceNumber']")
  check "abandoned message moved to dead-letter queue" test -n "$DLSEQ"
  check "resend from dead-letter back to source" cli "${C[@]}" resend 'clitest/$deadletter' "$DLSEQ"
  SEQ=$(cli "${C[@]}" peek clitest --json | head -1 | json_get "d['sequenceNumber']")
  check "delete single message" cli "${C[@]}" delete-message clitest "$SEQ"
  check "deleting it again fails cleanly" bash -c "! dotnet '$CLI_DLL' --url http://localhost:5700 delete-message clitest $SEQ 2>/dev/null"
  check "audit log recorded the actions" bash -c "dotnet '$CLI_DLL' --url http://localhost:5700 audit | grep -q 'messages/resend'"
  check "unknown command exits with 2" bash -c "dotnet '$CLI_DLL' bogus >/dev/null 2>&1; test \$? -eq 2"
  check "metrics endpoint exposes counters" bash -c "curl -s localhost:5700/api/metrics | grep -q 'nexusmq_messages_sent_total{entity=\"clitest\"}'"
  check "dashboard is served" bash -c "curl -s localhost:5700/ | grep -q '<title>NexusMQ Dashboard</title>'"
  if command -v node >/dev/null; then
    curl -s localhost:5700/ | python3 -c "import re,sys;print(re.search(r'<script>(.*)</script>',sys.stdin.read(),re.S).group(1))" > "$WORK/dash.js"
    check "dashboard JavaScript is valid" node --check "$WORK/dash.js"
  else skip "dashboard JavaScript check (node not installed)"; fi

  say "CLI as a .NET tool from the NuGet package"
  dotnet pack "$ROOT/src/NexusMQ.Cli" -c Release -o "$WORK/pkg" -p:Version=0.0.0-regression > "$WORK/pack.log" 2>&1
  dotnet tool install --tool-path "$WORK/tool" NexusMQ.Cli --version 0.0.0-regression --add-source "$WORK/pkg" --ignore-failed-sources > "$WORK/toolinstall.log" 2>&1
  check "tool installs and runs" "$WORK/tool/nexusmq" --url http://localhost:5700 stats
fi

# ------------------------------------------------------------------------------------------ access keys
if want keys; then
  say "Access keys, runtime keys, rotation, rate limits, audit of denials"
  start keys 5702 NexusMQ__ApiKey=master \
    NexusMQ__AccessKeys__0__Name=sender NexusMQ__AccessKeys__0__Key=send-key NexusMQ__AccessKeys__0__Rights=Send NexusMQ__AccessKeys__0__Entities__0=orders \
    NexusMQ__AccessKeys__1__Name=prom NexusMQ__AccessKeys__1__Key=mon-key NexusMQ__AccessKeys__1__Rights=Monitor
  K=localhost:5702
  code -X PUT -H 'X-Api-Key: master' $K/api/queues/orders >/dev/null
  check "no key → 401" test "$(code $K/api/stats)" = 401
  check "send key can send to its queue" test "$(code -X POST -H 'X-Api-Key: send-key' -H 'Content-Type: application/json' -d '[{"body":"x"}]' $K/api/send/orders)" = 200
  check "send key cannot receive → 403" test "$(code -X POST -H 'X-Api-Key: send-key' $K/api/receive/orders)" = 403
  check "send key cannot touch other queues → 403" test "$(code -X POST -H 'X-Api-Key: send-key' -H 'Content-Type: application/json' -d '[{"body":"x"}]' $K/api/send/other)" = 403
  check "monitor key reads metrics via ?apiKey=" test "$(code "$K/api/metrics?apiKey=mon-key")" = 200
  check "monitor key cannot manage → 403" test "$(code -X PUT -H 'X-Api-Key: mon-key' $K/api/queues/x)" = 403
  SECRET=$(cli --url http://$K --key master keys create app --rights Send --entity orders 2>/dev/null)
  check "runtime key works" test "$(code -X POST -H "X-Api-Key: $SECRET" -H 'Content-Type: application/json' -d '[{"body":"x"}]' $K/api/send/orders)" = 200
  NEW=$(cli --url http://$K --key master keys rotate app --grace 60 2>/dev/null)
  check "old secret still works during grace period" test "$(code -X POST -H "X-Api-Key: $SECRET" -H 'Content-Type: application/json' -d '[{"body":"x"}]' $K/api/send/orders)" = 200
  check "new secret works" test "$(code -X POST -H "X-Api-Key: $NEW" -H 'Content-Type: application/json' -d '[{"body":"x"}]' $K/api/send/orders)" = 200
  check "secret is not stored in plain text" bash -c "! grep -q '$NEW' '$WORK/keys-data/access-keys.json'"
  cli --url http://$K --key master keys limits app --rps 2 --burst 2 > /dev/null
  LIMITED=0; for _ in $(seq 1 8); do [[ "$(code -X POST -H "X-Api-Key: $NEW" -H 'Content-Type: application/json' -d '[{"body":"x"}]' $K/api/send/orders)" == 429 ]] && LIMITED=$((LIMITED+1)); done
  check "rate limit answers 429 when exceeded ($LIMITED of 8 limited)" test "$LIMITED" -ge 4
  check "audit log records denied requests" bash -c "dotnet '$CLI_DLL' --url http://$K --key master audit --result denied | grep -q denied"
  stop keys
fi

# ------------------------------------------------------------------------------------------ HTTPS
if want https; then
  say "HTTPS: Kestrel certificate, HTTP + WebSocket (wss) through the CLI"
  openssl req -x509 -newkey rsa:2048 -nodes -keyout "$WORK/key.pem" -out "$WORK/cert.pem" -days 2 -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost" >/dev/null 2>&1
  openssl pkcs12 -export -out "$WORK/tls.pfx" -inkey "$WORK/key.pem" -in "$WORK/cert.pem" -passout pass:pw >/dev/null 2>&1
  start tls 5703 HTTPS=1 Kestrel__Certificates__Default__Path="$WORK/tls.pfx" Kestrel__Certificates__Default__Password=pw
  check "untrusted certificate is refused with a clear message" bash -c "dotnet '$CLI_DLL' --url https://localhost:5703 stats 2>&1 | grep -q 'not trusted'"
  cli --url https://localhost:5703 --insecure create queue tls >/dev/null
  cli --url https://localhost:5703 --insecure send tls "over TLS" >/dev/null
  timeout -s INT 8 dotnet "$CLI_DLL" --url https://localhost:5703 --insecure listen tls > "$WORK/tls-listen.log" 2>/dev/null
  check "push delivery over wss" grep -q "over TLS" "$WORK/tls-listen.log"
  stop tls
fi

# ------------------------------------------------------------------------------------------ primary / standby
if want ha; then
  say "High availability: primary + standby, hard-kill the primary under traffic"
  start ha-a 5710 NexusMQ__ApiKey=master
  start ha-b 5711 NexusMQ__ApiKey=master NexusMQ__Replication__Role=Standby NexusMQ__Replication__PrimaryUrl=http://localhost:5710 \
    NexusMQ__Replication__PrimaryKey=master NexusMQ__Replication__AutoFailover=true NexusMQ__Replication__FailoverAfterSeconds=5
  HA="http://localhost:5710,http://localhost:5711"
  cli --url "$HA" --key master create queue chaos >/dev/null
  cli --url "$HA" --key master create queue keep >/dev/null; cli --url "$HA" --key master send keep "survives failover" >/dev/null
  APP=$(cli --url "$HA" --key master keys create app --rights Send,Listen --entity chaos 2>/dev/null)
  sleep 2
  check "standby refuses clients with 503" test "$(code -H 'X-Api-Key: master' localhost:5711/api/stats)" = 503
  ( sleep 8; kill -9 "$(cat "$WORK/ha-a.pid")" ) &
  dotnet "$CHAOS_DLL" "$HA" "$APP" 25 chaos > "$WORK/ha-chaos.log" 2>&1; RC=$?
  grep SUMMARY "$WORK/ha-chaos.log" | sed 's/^/    /'
  if [[ $RC == 0 ]]; then pass "no confirmed message lost or duplicated across the failover"
  elif [[ $RC == 3 ]]; then warn "no loss, but duplicates after failover (allowed: at-least-once)"
  else fail "confirmed messages lost across the failover"; fi
  check "standby became primary" bash -c "curl -s localhost:5711/api/health | grep -q '\"role\":\"Primary\"'"
  check "data from before the failover survived" bash -c "dotnet '$CLI_DLL' --url http://localhost:5711 --key master peek keep | grep -q 'survives failover'"
  check "runtime key was replicated" test "$(code -X POST -H "X-Api-Key: $APP" -H 'Content-Type: application/json' -d '[{"body":"x"}]' localhost:5711/api/send/chaos)" = 200
  stop ha-b
fi

# ------------------------------------------------------------------------------------------ Raft cluster
cluster_env() { # cluster_env <id>
  echo NexusMQ__ApiKey=master NexusMQ__Cluster__NodeId="$1" NexusMQ__Cluster__Secret=regression-secret \
    NexusMQ__Cluster__Nodes__0__Id=a NexusMQ__Cluster__Nodes__0__Url=http://localhost:5720 \
    NexusMQ__Cluster__Nodes__1__Id=b NexusMQ__Cluster__Nodes__1__Url=http://localhost:5721 \
    NexusMQ__Cluster__Nodes__2__Id=c NexusMQ__Cluster__Nodes__2__Url=http://localhost:5722
}
id_of_port() { case "$1" in 5720) echo a;; 5721) echo b;; 5722) echo c;; 5723) echo d;; esac; }
port_of_id() { case "$1" in a) echo 5720;; b) echo 5721;; c) echo 5722;; d) echo 5723;; esac; }
wait_leader() { for _ in $(seq 1 40); do for p in "$@"; do L=$(leader_port "$p" 2>/dev/null); [[ -n "$L" ]] && { echo "$L"; return 0; }; done; sleep 0.5; done; return 1; }

if want cluster; then
  say "Raft cluster: 3 servers, hard-kill the leader under traffic"
  # shellcheck disable=SC2046
  start a 5720 $(cluster_env a); start b 5721 $(cluster_env b); start c 5722 $(cluster_env c)
  CL="http://localhost:5720,http://localhost:5721,http://localhost:5722"
  LP=$(wait_leader 5720 5721 5722); check "a leader was elected" test -n "$LP"
  cli --url "$CL" --key master create queue chaos >/dev/null
  cli --url "$CL" --key master create queue keep >/dev/null; cli --url "$CL" --key master send keep "survives everything" >/dev/null
  APP=$(cli --url "$CL" --key master keys create app --rights Send,Listen --entity chaos 2>/dev/null)
  LID=$(id_of_port "$LP")
  ( sleep 8; kill -9 "$(cat "$WORK/$LID.pid")" ) &
  dotnet "$CHAOS_DLL" "$CL" "$APP" 25 chaos > "$WORK/cluster-chaos.log" 2>&1; RC=$?
  grep SUMMARY "$WORK/cluster-chaos.log" | sed 's/^/    /'
  if [[ $RC == 0 ]]; then pass "leader killed: no confirmed message lost or duplicated"
  elif [[ $RC == 3 ]]; then warn "leader killed: no loss, some duplicates (allowed: at-least-once)"
  else fail "leader killed: confirmed messages lost"; fi
  NEWLP=$(wait_leader 5720 5721 5722); check "a new leader took over" test -n "$NEWLP" -a "$NEWLP" != "$LP"
  check "leader reports how recently each live peer answered" bash -c "curl -s -H 'X-Api-Key: master' localhost:$NEWLP/api/replication/status | python3 -c \"
import json,sys
c=json.load(sys.stdin)['cluster']; ms=c['peerLastContactMs']
alive=[k for k,v in ms.items() if v is not None and v < 2000]
assert len(alive) >= 1, ms\""
  check "dashboard has the Servers panel" bash -c "curl -s localhost:$NEWLP/ | grep -q 'id=\"clusterSection\"'"

  say "Raft cluster: restarted server catches up"
  # shellcheck disable=SC2046
  start "$LID" "$LP" $(cluster_env "$LID")
  sleep 4
  STATUS=$(cli --url "$CL" --key master replication status)
  echo "$STATUS" | sed 's/^/    /'
  check "all three servers have the same committed log" bash -c "echo '$STATUS' | grep -o 'applied=[0-9]*' | sort -u | wc -l | grep -qx 1"

  say "Raft cluster: add a server and remove the leader, under traffic"
  LP=$(wait_leader 5720 5721 5722); LID=$(id_of_port "$LP")
  (
    sleep 5
    setsid env NexusMQ__DataDirectory="$WORK/d-data" NexusMQ__ApiKey=master NexusMQ__Cluster__NodeId=d NexusMQ__Cluster__Secret=regression-secret \
      NexusMQ__Cluster__Join=true dotnet "$SRV" --urls http://localhost:5723 >> "$WORK/d.log" 2>&1 < /dev/null &
    echo $! > "$WORK/d.pid"
    sleep 4; dotnet "$CLI_DLL" --url "$CL" --key master cluster add d http://localhost:5723 >> "$WORK/membership.log" 2>&1
    sleep 2; dotnet "$CLI_DLL" --url "http://localhost:$LP" --key master cluster remove "$LID" >> "$WORK/membership.log" 2>&1
    sleep 3; kill -9 "$(cat "$WORK/$LID.pid")"
  ) &
  dotnet "$CHAOS_DLL" "$CL,http://localhost:5723" "$APP" 25 chaos > "$WORK/membership-chaos.log" 2>&1; RC=$?
  PIDS+=("$(cat "$WORK/d.pid" 2>/dev/null)")
  grep SUMMARY "$WORK/membership-chaos.log" | sed 's/^/    /'
  if [[ $RC == 0 ]]; then pass "membership changes: no confirmed message lost or duplicated"
  elif [[ $RC == 3 ]]; then warn "membership changes: no loss, some duplicates (allowed: at-least-once)"
  else fail "membership changes: confirmed messages lost"; fi
  MEMBERS=$(cli --url "$CL,http://localhost:5723" --key master cluster members)
  echo "$MEMBERS" | sed 's/^/    /'
  check "new server d is a member and the old leader is gone" bash -c "echo '$MEMBERS' | grep -q '^d ' && ! echo '$MEMBERS' | grep -q '^$LID '"
  check "first message survived everything" bash -c "dotnet '$CLI_DLL' --url '$CL,http://localhost:5723' --key master peek keep | grep -q 'survives everything'"

  say "Raft cluster: a minority refuses writes"
  ALIVE=(); for id in a b c d; do [[ "$id" == "$LID" ]] && continue; ALIVE+=("$id"); done
  stop "${ALIVE[0]}"; stop "${ALIVE[1]}"
  SURV="${ALIVE[2]}"; SP=$(port_of_id "$SURV")
  sleep 3
  check "the last server refuses a write" bash -c "! dotnet '$CLI_DLL' --url http://localhost:$SP --key master send keep 'must not exist' 2>/dev/null"
  # shellcheck disable=SC2046
  if [[ "${ALIVE[0]}" == d ]]; then
    setsid env NexusMQ__DataDirectory="$WORK/d-data" NexusMQ__ApiKey=master NexusMQ__Cluster__NodeId=d NexusMQ__Cluster__Secret=regression-secret \
      NexusMQ__Cluster__Join=true dotnet "$SRV" --urls http://localhost:5723 >> "$WORK/d.log" 2>&1 < /dev/null & PIDS+=("$!")
  else start "${ALIVE[0]}" "$(port_of_id "${ALIVE[0]}")" $(cluster_env "${ALIVE[0]}"); fi
  sleep 5
  check "with a majority back, writes work again" cli --url "$CL,http://localhost:5723" --key master send keep "after recovery"
  check "the refused message never appeared" bash -c "! dotnet '$CLI_DLL' --url '$CL,http://localhost:5723' --key master peek keep | grep -q 'must not exist'"
fi

# ------------------------------------------------------------------------------------------ upgrade from an older version
if want upgrade; then
  say "Upgrade: data written by an older server, read by this one (single server and mixed-version cluster)"
  if [[ -z "${OLD_SERVER:-}" || ! -f "${OLD_SERVER:-}" ]]; then
    skip "upgrade checks (set OLD_SERVER to an older NexusMQ.Server.dll)"
  else
    head -c 20000 /dev/urandom > "$WORK/big.bin"
    python3 -c "import base64,json,sys;print(json.dumps([{'subject':'big','bodyBase64':base64.b64encode(open(sys.argv[1],'rb').read()).decode()}]))" "$WORK/big.bin" > "$WORK/big.json"
    WANT=$(sha256sum "$WORK/big.bin" | cut -c1-16)
    big_sha() { curl -s "${@:2}" "$1/api/peek/up?max=50" | python3 -c "
import json,sys,base64,hashlib
for m in json.load(sys.stdin):
  if m.get('subject')=='big': print(hashlib.sha256(base64.b64decode(m['bodyBase64'])).hexdigest()[:16])"; }
    # single server: old writes, new reads (and converts the files)
    setsid env NexusMQ__DataDirectory="$WORK/up-single" dotnet "$OLD_SERVER" --urls http://localhost:5730 >> "$WORK/up-old.log" 2>&1 < /dev/null &
    for _ in $(seq 1 60); do curl -sf localhost:5730/api/health >/dev/null && break; sleep 0.5; done
    code -X PUT localhost:5730/api/queues/up >/dev/null
    for i in 1 2 3; do code -X POST -H 'Content-Type: application/json' -d "[{\"body\":\"msg $i\"}]" localhost:5730/api/send/up >/dev/null; done
    code -X POST -H 'Content-Type: application/json' --data-binary @"$WORK/big.json" localhost:5730/api/send/up >/dev/null
    pkill -9 -f "urls http://localhost:5730"; sleep 1
    start up-new 5731 NexusMQ__DataDirectory="$WORK/up-single"
    check "messages written by the older server are all there" bash -c "curl -s localhost:5731/api/stats | grep -q '\"active\":4'"
    check "binary body survived the upgrade byte for byte" test "$(big_sha http://localhost:5731)" = "$WANT"
    check "journal converted to the current format" bash -c "head -c 8 '$WORK/up-single/entities/up.journal' | grep -q NMQBIN1"
    stop up-new
  fi
fi

# ------------------------------------------------------------------------------------------ benchmarks
if want bench; then
  say "Benchmarks (informational)"
  BENCH="$ROOT/samples/Sample.Benchmark/bin/Release/net10.0/Sample.Benchmark.dll"
  [[ -f "$WORK/single.pid" ]] && kill -0 "$(cat "$WORK/single.pid")" 2>/dev/null || start single 5700
  echo "    single server:"; dotnet "$BENCH" http://localhost:5700 3000 2>&1 | tail -3 | sed 's/^/      /'
  for p in 5720 5721 5722 5723; do kill -9 "$(cat "$WORK/$(id_of_port $p).pid" 2>/dev/null)" 2>/dev/null; done
  rm -rf "$WORK"/a-data "$WORK"/b-data "$WORK"/c-data "$WORK"/d-data
  # shellcheck disable=SC2046
  start a 5720 $(cluster_env a); start b 5721 $(cluster_env b); start c 5722 $(cluster_env c); wait_leader 5720 5721 5722 >/dev/null
  echo "    3-node cluster:"; dotnet "$BENCH" "http://localhost:5720,http://localhost:5721,http://localhost:5722" 3000 master 2>&1 | tail -3 | sed 's/^/      /'
  pass "benchmarks ran"
fi

# ------------------------------------------------------------------------------------------ tooling
if want tooling; then
  say "Workflows and container"
  if command -v actionlint >/dev/null; then check "GitHub Actions workflows are valid (actionlint)" actionlint "$ROOT"/.github/workflows/*.yml
  else skip "actionlint not installed"; fi
  if docker info >/dev/null 2>&1; then
    check "Docker image builds" docker build -q -t nexusmq:regression "$ROOT"
  else skip "Docker image build (no Docker daemon)"; fi
fi

# ------------------------------------------------------------------------------------------ summary
say "Summary"
printf '%s\n' "${RESULTS[@]}" | sed 's/^/  /'
printf '\n  %d passed, %d failed, %d warnings, %d skipped\n' "$PASSES" "$FAILS" "$WARNS" "$SKIPS"
exit "$FAILS"
