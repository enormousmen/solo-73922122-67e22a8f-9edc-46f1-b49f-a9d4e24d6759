#!/bin/sh
# Demonstration of the digest-verified download API.
#
#   1. publish a digest from the current upstream (demo helper)
#   2. download through /api/verified-download -> 200 only after a match
#   3. a wrong digest -> 422 with JSON only (no artifact bytes)
#   4. an upstream 404 -> 502 (never its error body)
#   5. switching mid-verification -> old generation drains after 5s
#
# Requires the compose stack (see README).  Relay URL: $RELAY (default
# http://localhost:8080).
set -eu

RELAY=${RELAY:-http://localhost:8080}
SEED=${SEED:-demo}
SIZE=${SIZE:-1048576}
WORK=${WORK:-/tmp/relay-verified-demo}

mkdir -p "$WORK"

json() { python3 -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]])' "$1"; }

echo "== current target =="
curl -sS "$RELAY/api/status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["current"])'

echo "== publishing the expected digest =="
DIGEST=$(curl -sS "$RELAY/verified/manifest?seed=$SEED&bytes=$SIZE" | json sha256)
echo "sha256=$DIGEST  bytes=$SIZE"
curl -sS "$RELAY/verified/manifest?seed=$SEED&bytes=$SIZE" | python3 -m json.tool

echo "== verified download =="
curl -sS -D "$WORK/ok.hdr" -o "$WORK/ok.bin" \
  "$RELAY/api/verified-download?path=%2Fverified%2Fartifact%3Fseed%3D$SEED%26bytes%3D$SIZE&sha256=$DIGEST"
grep -i '^HTTP\|^x-relay\|^content-length' "$WORK/ok.hdr" || true
ACTUAL=$(python3 -c "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$WORK/ok.bin")
echo "local sha256=$ACTUAL"
[ "$ACTUAL" = "$DIGEST" ] && echo "OK: bytes match the published digest" || { echo "MISMATCH"; exit 1; }

echo "== wrong digest is rejected without artifact bytes =="
WRONG=$(python3 -c 'print("0"*64)')
curl -sS -D "$WORK/bad.hdr" -o "$WORK/bad.json" \
  "$RELAY/api/verified-download?path=%2Fverified%2Fartifact%3Fseed%3D$SEED%26bytes%3D100&sha256=$WRONG"
grep -i '^HTTP\|^content-type' "$WORK/bad.hdr" || true
cat "$WORK/bad.json"; echo

echo "== upstream non-200 is not forwarded =="
curl -sS -D "$WORK/404.hdr" -o "$WORK/404.json" \
  "$RELAY/api/verified-download?path=%2Fno-such-artifact&sha256=$DIGEST"
grep -i '^HTTP\|^content-type' "$WORK/404.hdr" || true
cat "$WORK/404.json"; echo

echo "== switch mid-verification (slow upstream; old generation drains in 5s) =="
OTHER=$([ "$(curl -sS "$RELAY/api/targets" | json current)" = blue ] && echo green || echo blue)
# 8 MiB in 64 KiB chunks with a 0.1s pause => ~13s of staging, so the 5s
# drain window necessarily expires while the request is still unverified.
SLOW_SIZE=8388608
SLOW_DIGEST=$(curl -sS "$RELAY/verified/manifest?seed=$SEED&bytes=$SLOW_SIZE" | json sha256)
curl -sS -o "$WORK/slow.json" -w '%{http_code}\n' \
  "$RELAY/api/verified-download?path=%2Fverified%2Fartifact%3Fseed%3D$SEED%26bytes%3D$SLOW_SIZE%26chunk%3D65536%26delay%3D0.1&sha256=$SLOW_DIGEST" > "$WORK/slow.code" &
SLOW_PID=$!
sleep 1
curl -sS -X POST "$RELAY/api/switch" -H 'content-type: application/json' \
  -d "{\"target\":\"$OTHER\",\"wait\":true,\"wait_timeout\":6}" | python3 -m json.tool
START=$(date +%s)
if wait "$SLOW_PID"; then
  echo "slow verified download ended with HTTP $(cat "$WORK/slow.code") $(( $(date +%s) - START ))s after the switch"
  cat "$WORK/slow.json"; echo
else
  echo "slow verified download interrupted, $(( $(date +%s) - START ))s after the switch"
fi

echo "== staging directory is empty (nothing left behind) =="
if command -v docker >/dev/null 2>&1; then
  docker compose exec -T relay sh -c 'ls -la /var/tmp/relay-staging 2>/dev/null || true' || true
else
  echo "(docker not available here; check the relay's staging dir on the host)"
fi

echo "== final status =="
curl -sS "$RELAY/api/status" | python3 -m json.tool
