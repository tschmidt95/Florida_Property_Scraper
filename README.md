# Florida_Property_Scraper
Scan properties and look up owner information in Florida

---

## Post-publish smoke check (automated)

We run a post-publish smoke check after a release is published that pulls the published GHCR image and performs a minimal import test to ensure the package is present and importable.

- How to re-run manually: open the `Post-publish smoke check` workflow and use **Run workflow** (workflow_dispatch) with the `tag` input set to the tag you want to validate.
- Notes on timing: releases sometimes publish slightly before the registry is fully populated. The workflow includes a retry/backoff and a delayed retry that waits and re-checks automatically; if the pull ultimately fails the action prints a useful link to the publish workflows filtered by tag:

  https://github.com/<owner>/<repo>/actions/runs?query=tag%3A<your-tag>

Replace `<owner>/<repo>` and `<your-tag>` with the repository and tag to inspect the publish job logs.

If you want, you can also dispatch the workflow manually with the `tag` of a known-published image for immediate verification.

### Scheduled image monitoring

A daily scheduled job runs at 02:00 UTC and checks `ghcr.io/tschmidt95/florida-scraper:latest` by default (it can be dispatched manually with a specific `tag`). This provides ongoing verification that published images remain importable.

---
