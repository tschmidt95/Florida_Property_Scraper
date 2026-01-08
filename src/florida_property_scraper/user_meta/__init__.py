"""User-managed metadata for parcels.

Non-negotiable: user meta never mutates PA-extracted fields.
"""

from .storage import UserMetaSQLite

__all__ = ["UserMetaSQLite"]
