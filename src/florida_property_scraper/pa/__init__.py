"""Property Appraiser (PA)-only canonical schema + storage + comps.

Core rule: attributes come only from PA-extracted data.
Missing attributes must be present with zero/empty defaults.
"""

from .schema import PAProperty
from .normalize import apply_defaults

__all__ = ["PAProperty", "apply_defaults"]
