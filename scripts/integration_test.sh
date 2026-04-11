#!/usr/bin/env bash
set -euo pipefail

BIN=${1:-./target/release/parquet2json}
FIXTURE=tests/fixtures/sample.parquet
HTTP_URL=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
S3_URL=s3://parquet2json-tests/tests/fixtures/sample.parquet
S3_REGION=us-east-1

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; exit 1; }

assert_eq() {
  local desc=$1 actual=$2 expected=$3
  if [ "$actual" = "$expected" ]; then
    pass "$desc"
  else
    fail "$desc — expected '$expected', got '$actual'"
  fi
}

echo ""
echo "=== Local: $FIXTURE ==="

COUNT=$("$BIN" "$FIXTURE" rowcount)
assert_eq "rowcount == 1" "$COUNT" "1"

"$BIN" "$FIXTURE" schema > /dev/null
pass "schema exits 0"

LINES=$("$BIN" "$FIXTURE" cat | wc -l | tr -d ' ')
assert_eq "cat outputs 1 line" "$LINES" "1"

LINES=$("$BIN" "$FIXTURE" cat --offset=-1 | wc -l | tr -d ' ')
assert_eq "cat --offset=-1 outputs 1 line" "$LINES" "1"

LINES=$("$BIN" "$FIXTURE" cat --columns dt | wc -l | tr -d ' ')
assert_eq "cat --columns dt outputs 1 line" "$LINES" "1"

echo ""
echo "=== HTTP: $HTTP_URL ==="

COUNT=$("$BIN" "$HTTP_URL" rowcount)
[ "$COUNT" -gt 0 ] || fail "rowcount > 0"
pass "rowcount ($COUNT) > 0"

LINES=$("$BIN" "$HTTP_URL" cat --limit 5 | wc -l | tr -d ' ')
assert_eq "cat --limit 5 outputs 5 lines" "$LINES" "5"

echo ""

has_aws_credentials() {
  [ -n "${AWS_ACCESS_KEY_ID:-}" ] && return 0
  command -v aws >/dev/null 2>&1 && aws sts get-caller-identity >/dev/null 2>&1
}

if ! has_aws_credentials; then
  echo "=== S3: skipped (no AWS credentials found) ==="
else
  echo "=== S3: $S3_URL ==="

  COUNT=$(AWS_DEFAULT_REGION=$S3_REGION "$BIN" "$S3_URL" rowcount)
  assert_eq "rowcount == 1" "$COUNT" "1"

  AWS_DEFAULT_REGION=$S3_REGION "$BIN" "$S3_URL" schema > /dev/null
  pass "schema exits 0"

  LINES=$(AWS_DEFAULT_REGION=$S3_REGION "$BIN" "$S3_URL" cat | wc -l | tr -d ' ')
  assert_eq "cat outputs 1 line" "$LINES" "1"
fi

echo ""
echo "All integration tests passed."
