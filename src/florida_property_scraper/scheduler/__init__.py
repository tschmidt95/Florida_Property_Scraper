"""Saved searches + alerts scheduler.

This package provides a deterministic, offline-friendly scheduler tick that:
- Refreshes saved-search membership
- Runs trigger connectors per county
- Rebuilds parcel trigger rollups
- Syncs alerts_inbox for each saved search

CLI entrypoint is wired via `python -m florida_property_scraper scheduler run`.
"""
