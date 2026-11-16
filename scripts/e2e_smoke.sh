#!/usr/bin/env bash
# End-to-end smoke test over real HTTP against a running uvicorn instance.
set -uo pipefail
BASE="${1:-http://127.0.0.1:8011}"
M="M-E2E-$$"
pass=0; fail=0

check() { # desc expected actual
  if [ "$2" = "$3" ]; then echo "PASS: $1 ($3)"; pass=$((pass+1));
  else echo "FAIL: $1 expected=$2 actual=$3"; fail=$((fail+1)); fi
}

echo "== 1. register two touching segments =="
code=$(curl -s -o /tmp/e2e_r1.json -w "%{http_code}" -X POST "$BASE/ownership/register" -H 'Content-Type: application/json' -d "{\"meter_no\":\"$M\",\"valid_from\":\"2026-01-01T00:00:00Z\",\"valid_to\":\"2026-01-01T12:00:00Z\",\"line\":\"LINE-A\"}")
check "register segment 1" 201 "$code"; cat /tmp/e2e_r1.json; echo
code=$(curl -s -o /tmp/e2e_r2.json -w "%{http_code}" -X POST "$BASE/ownership/register" -H 'Content-Type: application/json' -d "{\"meter_no\":\"$M\",\"valid_from\":\"2026-01-01T12:00:00Z\",\"valid_to\":\"2026-01-02T00:00:00Z\",\"line\":\"LINE-B\"}")
check "register segment 2 (touching)" 201 "$code"

echo "== 2. overlapping registration must be 409 =="
code=$(curl -s -o /tmp/e2e_overlap.json -w "%{http_code}" -X POST "$BASE/ownership/register" -H 'Content-Type: application/json' -d "{\"meter_no\":\"$M\",\"valid_from\":\"2026-01-01T06:00:00Z\",\"valid_to\":\"2026-01-01T18:00:00Z\",\"line\":\"LINE-X\"}")
check "overlap rejected" 409 "$code"; cat /tmp/e2e_overlap.json; echo

echo "== 3. ingest readings (covered + uncovered) =="
code=$(curl -s -o /tmp/e2e_ing.json -w "%{http_code}" -X POST "$BASE/readings/ingest" -H 'Content-Type: application/json' -d "{\"readings\":[{\"reading_uid\":\"E2E-$$-1\",\"meter_no\":\"$M\",\"ts\":\"2026-01-01T11:59:00Z\",\"wh\":100},{\"reading_uid\":\"E2E-$$-2\",\"meter_no\":\"$M\",\"ts\":\"2026-01-01T12:00:00Z\",\"wh\":200}]}")
check "ingest covered readings" 201 "$code"; cat /tmp/e2e_ing.json; echo
code=$(curl -s -o /tmp/e2e_nocov.json -w "%{http_code}" -X POST "$BASE/readings/ingest" -H 'Content-Type: application/json' -d "{\"readings\":[{\"reading_uid\":\"E2E-$$-3\",\"meter_no\":\"$M\",\"ts\":\"2026-01-03T00:00:00Z\",\"wh\":9}]}")
check "uncovered reading rejected" 422 "$code"

echo "== 4. get current version =="
ver=$(curl -s "$BASE/meters/$M" | python3 -c "import sys,json;print(json.load(sys.stdin)['version'])")
echo "current version = $ver"

echo "== 5. stale correction must be 409 =="
code=$(curl -s -o /tmp/e2e_stale.json -w "%{http_code}" -X POST "$BASE/ownership/correct" -H 'Content-Type: application/json' -d "{\"meter_no\":\"$M\",\"valid_from\":\"2026-01-01T03:00:00Z\",\"valid_to\":\"2026-01-01T04:00:00Z\",\"new_line\":\"LINE-Z\",\"base_version\":$((ver+50))}")
check "stale correction conflict" 409 "$code"

echo "== 6. correct window [06:00,18:00) to LINE-C =="
code=$(curl -s -o /tmp/e2e_corr.json -w "%{http_code}" -X POST "$BASE/ownership/correct" -H 'Content-Type: application/json' -d "{\"meter_no\":\"$M\",\"valid_from\":\"2026-01-01T06:00:00Z\",\"valid_to\":\"2026-01-01T18:00:00Z\",\"new_line\":\"LINE-C\",\"base_version\":$ver}")
check "correction accepted" 200 "$code"; cat /tmp/e2e_corr.json; echo

echo "== 7. segments after correction =="
curl -s "$BASE/ownership/$M" | python3 -c "import sys,json;[print(s['line'],s['valid_from'],'->',s['valid_to']) for s in json.load(sys.stdin)]"

echo "== 8. readings by line (re-linked) =="
for line in LINE-A LINE-B LINE-C; do
  echo "-- $line --"
  curl -s "$BASE/readings/by-line/$line?start=2026-01-01T00:00:00Z&end=2026-01-03T00:00:00Z" | python3 -c "import sys,json;[print(' ',r['reading_uid'],r['ts'],r['wh'],'via seg',r['ownership_id']) for r in json.load(sys.stdin) if r['meter_no']=='$M']"
done

echo
echo "RESULT: pass=$pass fail=$fail"
exit $fail
