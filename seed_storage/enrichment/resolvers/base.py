"""Abstract base class for all content resolvers."""

from __future__ import annotations

from abc import ABC, abstractmethod

from seed_storage.enrichment.models import ResolvedContent


class BaseResolver(ABC):
    """Abstract base for all content resolvers.

    Subclasses must implement ``can_handle`` and ``resolve``.
    ``can_handle`` is called synchronously by the dispatcher; ``resolve``
    is async to allow non-blocking HTTP I/O via httpx.
    """

    @abstractmethod
    def can_handle(self, url: str) -> bool:
        """Return True if this resolver can handle the given URL.

        Must be synchronous — called in the dispatcher routing loop.
        Should use URL pattern matching (scheme, host, path) and never
        perform network I/O.
        """

    @abstractmethod
    async def resolve(self, url: str) -> ResolvedContent:
        """Fetch and extract content for *url*.

        Must be async. Should never raise — catch all exceptions and
        return ``ResolvedContent.error_result(url, str(exc))`` on failure.
        """
