#!/usr/bin/env bash
set -euo pipefail

echo "Validating generic payload..."
generic=$(jq -n --arg title "Test title" --arg url "https://example.com/issue/1" --arg body "Test body" '{alert_type: "image_monitor_failure", title: $title, url: $url, body: $body}')
# Ensure required keys exist
echo "$generic" | jq -e 'has("alert_type") and has("title") and has("url") and has("body")' >/dev/null || { echo "Generic payload missing required fields"; echo "$generic"; exit 1; }

echo "Validating slack payload..."
slack=$(jq -n --arg title "Test title" --arg url "https://example.com/issue/1" --arg body_raw "Test body" --arg repo "owner/repo" '{blocks: [ {type: "section", text: {type: "mrkdwn", text: (":rotating_light: *" + $title + "*\n" + ($body_raw | tostring))}}, {type: "section", fields: [{type: "mrkdwn", text: ("*Repo:* " + $repo)}, {type: "mrkdwn", text: ("*Issue:* <" + $url + "|" + $title + ">")} ]}, {type: "actions", elements: [{type: "button", text: {type: "plain_text", text: "View issue"}, url: $url}]} ] }')
# Check that blocks array exists and first block has a section
echo "$slack" | jq -e '.blocks and (.blocks | length) > 0 and .blocks[0].type == "section"' >/dev/null || { echo "Slack payload invalid"; echo "$slack"; exit 1; }

echo "Validating teams payload..."
teams=$(jq -n --arg title "Test title" --arg url "https://example.com/issue/1" --arg body_raw "Test body" '{title: $title, text: $body_raw, potentialAction: [{"@type": "OpenUri", name: "View issue", targets: [{os: "default", uri: $url}]}]}')
# Check presence of title and text
echo "$teams" | jq -e 'has("title") and has("text")' >/dev/null || { echo "Teams payload invalid"; echo "$teams"; exit 1; }

echo "All payloads validated ✅"
