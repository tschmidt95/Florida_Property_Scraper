# Build Proofs (Expected Output Format)

This document describes the expected output format for proof scripts. Do not paste real run output here.

## Proof: Seminole search via UI proxy

Script: scripts/proof_search_seminole_via_ui_proxy.sh

Expected format:

```
status=200
returned_count=<int>
total_count=<int>
field_stats_keys=<comma-separated keys>
```

Failure conditions:
- Non-200 HTTP status should print the raw response body.
- ok=false should print "FAIL: ok=false" and exit non-zero.

## Proof: Seminole search direct (8000)

Script: scripts/proof_search_seminole_direct_8000.sh

Expected format:

```
status=200
raw_response=/tmp/<file>
returned_count=<int>
total_count=<int>
```

Failure conditions:
- Non-200 HTTP status should print the raw response body.
- ok=false should print "FAIL: ok=false" and exit non-zero.