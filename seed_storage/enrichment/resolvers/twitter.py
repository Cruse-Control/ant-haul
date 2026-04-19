"""Twitter/X resolver — TODO stub.

Real X/Twitter content extraction is out of scope for Phase A.
Returns an error_result for all twitter.com and x.com URLs.
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

from seed_storage.enrichment.models import ResolvedContent
from seed_storage.enrichment.resolvers.base import BaseResolver

logger = logging.getLogger(__name__)

_TWITTER_HOSTS = {"twitter.com", "www.twitter.com", "x.com", "www.x.com", "mobile.twitter.com"}


class TwitterResolver(BaseResolver):
    """TODO stub — returns error_result for all Twitter/X URLs.

    Real content extraction is out of scope for Phase A. This resolver
    exists to prevent fallback to the generic WebpageResolver, which would
    be blocked by Twitter's bot detection.
    """

    def can_handle(self, url: str) -> bool:
        parsed = urlparse(url)
        return (parsed.hostname or "").lower() in _TWITTER_HOSTS

    async def resolve(self, url: str) -> ResolvedContent:
        logger.debug("TwitterResolver: stub — returning error_result for %s", url)
        return ResolvedContent.error_result(
            url,
            "Twitter/X content extraction is not yet implemented (Phase A stub)",
        )
