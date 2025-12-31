#!/usr/bin/env bash
set -euo pipefail

echo "Validating generic payload..."
generic_file=/tmp/generic_payload.json
jq -n --arg title "Test title" --arg url "https://example.com/issue/1" --arg body "Test body" '{alert_type: "image_monitor_failure", title: $title, url: $url, body: $body}' > "$generic_file"

# Validate against JSON Schema using ajv (npx)
echo "Validating generic payload against schema..."
npx --yes ajv-cli validate -s .github/schemas/generic_payload.json -d "$generic_file" || (echo "Generic payload failed schema validation"; cat "$generic_file"; exit 1)

echo "Validating slack payload..."
slack_file=/tmp/slack_payload.json
jq -n --arg title "Test title" --arg url "https://example.com/issue/1" --arg body_raw "Test body" --arg repo "owner/repo" '{blocks: [ {type: "section", text: {type: "mrkdwn", text: (":rotating_light: *" + $title + "*\n" + ($body_raw | tostring))}}, {type: "section", fields: [{type: "mrkdwn", text: ("*Repo:* " + $repo)}, {type: "mrkdwn", text: ("*Issue:* <" + $url + "|" + $title + ">")} ]}, {type: "actions", elements: [{type: "button", text: {type: "plain_text", text: "View issue"}, url: $url}]} ] }' > "$slack_file"

# Validate against schema
npx --yes ajv-cli validate -s .github/schemas/slack_payload.json -d "$slack_file" || (echo "Slack payload failed schema validation"; cat "$slack_file"; exit 1)

echo "Validating generic payload..."
generic_file=/tmp/generic_payload.json
jq -n --arg title "Test title" --arg url "https://example.com/issue/1" --arg body "Test body" '{alert_type: "image_monitor_failure", title: $title, url: $url, body: $body}' > "$generic_file"
npx --yes ajv-cli validate -s .github/schemas/generic_payload.json -d "$generic_file" || (echo "Generic payload failed schema validation"; cat "$generic_file"; exit 1)
# Validate Teams payload
echo "Validating teams payload..."
teams_file=/tmp/teams_payload.json
jq -n --arg title "Test title" --arg url "https://example.com/issue/1" --arg body_raw "Test body" --arg repo "owner/repo" '{"@type": "MessageCard", "@context": "https://schema.org/extensions", "themeColor": "0078D7", "summary": $title, "sections": [{"activityTitle": $title, "activitySubtitle": $repo, "text": $body_raw}, {"potentialAction": [{"@type": "OpenUri", "name": "View issue", "targets": [{"os": "default", "uri": $url}]}]}]}' > "$teams_file"

npx --yes ajv-cli validate -s .github/schemas/teams_payload.json -d "$teams_file" || (echo "Teams payload failed schema validation"; cat "$teams_file"; exit 1)

# (Removed intentional failing check) - CI will validate real payloads; unit tests cover negative cases.

echo "Validating teams payload..."
teams=$(jq -n --arg title "Test title" --arg url "https://example.com/issue/1" --arg body_raw "Test body" '{title: $title, text: $body_raw, potentialAction: [{"@type": "OpenUri", name: "View issue", targets: [{os: "default", uri: $url}]}]}')
# Check presence of title and text
echo "$teams" | jq -e 'has("title") and has("text")' >/dev/null || { echo "Teams payload invalid"; echo "$teams"; exit 1; }

echo "All payloads validated ✅"
