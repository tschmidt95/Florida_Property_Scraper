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

## Proof: Backend stays up

Script: scripts/proof_backend_stays_up.sh

Expected format:

```
PASS attempt=<n> pid=<pid>
...
PASS backend_stays_up pid=<pid>
```

Failure conditions:
- Any request fails (non-200 or curl error).
- /api/debug/process PID changes during the loop.

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

## Proof: Providers catalog (Seminole)

Script: scripts/proof_providers_catalog_seminole.sh

Expected format:

```
{...}
```

Failure conditions:
- Missing provider `seminole_official_records`.
- Provider status is not `implemented`.

## Proof: Enrich (Seminole fixture)

Script: scripts/proof_enrich_seminole_fixture.sh

Expected format:

```
{...}
```

Failure conditions:
- `ok != true`.
- No evidence rows.
- Missing `last_sale_date` in merged fields for `SEM-0001`.

## Proof: Trigger evaluate (Seminole fixture)

Script: scripts/proof_trigger_eval_seminole.sh

Expected format:

```
{...}
```

Failure conditions:
- `ok != true`.
- No triggers fired.
- Missing `deed_transfer_recent` trigger key.
- Missing `new_recording` trigger key.

## Proof: Web build ok

Script: scripts/proof_web_build_ok.sh

Expected format:

```
PASS web_build_ok
```

Failure conditions:
- `npm run build` fails.

## Proof: Provider evidence persists

Script: scripts/proof_provider_evidence_persist.sh

Expected format:

```
{...}
```

Failure conditions:
- `ok != true`.
- `count <= 0`.