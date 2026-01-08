from __future__ import annotations

from typing import List, Protocol

from ..models import ComparableListing, SubjectProperty


class ListingProvider(Protocol):
    """Pluggable comparable listing provider.

    Providers must avoid scraping paywalled sources.
    """

    name: str

    def search(self, subject: SubjectProperty) -> List[ComparableListing]:
        ...
