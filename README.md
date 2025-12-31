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

If the scheduled monitor fails repeatedly (default threshold: 3 consecutive failures), an automatic GitHub Issue will be created (label `monitor-failure`) to notify maintainers and centralize triage.

### Optional: External notifications (template)

We provide a **template** workflow that can send an external notification (Slack, Microsoft Teams, webhook endpoints) when an issue labeled `monitor-failure` is opened.

- File: `.github/workflows/notify_placeholder.yml`
- How to enable: add a repository secret named `ALERT_WEBHOOK_URL` with your webhook URL (Slack, Teams, or custom). If you want to select provider-specific payloads, also add `ALERT_PROVIDER` with one of `slack`, `teams`, or `generic` (defaults to `slack`).
- Notes: the template is intentionally non-invasive (it exits if `ALERT_WEBHOOK_URL` is not configured). When you add the secret, the workflow will start posting notifications on new `monitor-failure` issues.

Testing:
1. Add secrets `ALERT_WEBHOOK_URL` (required) and optionally `ALERT_PROVIDER` (`slack` or `teams`).
2. Create a test issue with label `monitor-failure` to trigger the notification.
3. The workflow will send a JSON payload appropriate for the selected provider and log the webhook response status and a short response body excerpt in the action logs (helps with debugging).

---
