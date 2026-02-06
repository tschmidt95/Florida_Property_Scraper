#!/usr/bin/env bash
set -euo pipefail

curl_json_or_fail() {
  local url="$1"
  local out="$2"
  shift 2

  local tmp_hdr
  tmp_hdr="$(mktemp)"

  local http_status
  http_status="$(curl -sS -D "$tmp_hdr" -o "$out" -w "%{http_code}" "$@" "$url" || true)"

  local content_type
  content_type="$(awk 'BEGIN{IGNORECASE=1} /^content-type:/{print $2}' "$tmp_hdr" | tail -n 1 | tr -d '\r')"

  if [[ ! "$http_status" =~ ^2 ]]; then
    echo "FAIL: http_status=$http_status"
    echo "content_type=${content_type:-unknown}"
    echo "body_head_200:"
    head -c 200 "$out" 2>/dev/null || true
    echo
    rm -f "$tmp_hdr"
    return 2
  fi

  if ! test -s "$out"; then
    echo "FAIL: http_status=$http_status"
    echo "content_type=${content_type:-unknown}"
    echo "body_head_200:"
    head -c 200 "$out" 2>/dev/null || true
    echo
    rm -f "$tmp_hdr"
    return 2
  fi

  rm -f "$tmp_hdr"
  return 0
}
