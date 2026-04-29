"""URL classifier — routes URLs to the correct pipeline by platform."""

import re
from enum import Enum
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


class Platform(str, Enum):
    INSTAGRAM = "instagram"
    INSTAGRAM_IMAGE = "instagram_image"
    YOUTUBE = "youtube"
    X_TWITTER = "x_twitter"
    GITHUB = "github"
    AUDIBLE = "audible"
    WEB = "web"
    PLAIN_TEXT = "plain_text"
    DISCORD_LINK = "discord_link"
    MEDIA_LINK = "media_link"


# Order matters — first match wins.
_PATTERNS: list[tuple[re.Pattern, Platform]] = [
    (re.compile(r"https?://(www\.)?instagram\.com/(reel|reels)/"), Platform.INSTAGRAM),
    (re.compile(r"https?://(www\.)?instagram\.com/p/"), Platform.INSTAGRAM_IMAGE),
    (re.compile(r"https?://(www\.)?(youtube\.com/watch|youtu\.be/)"), Platform.YOUTUBE),
    (re.compile(r"https?://(www\.)?(youtube\.com/shorts/)"), Platform.YOUTUBE),
    (re.compile(r"https?://(www\.)?(x\.com|twitter\.com)/\w+/status/"), Platform.X_TWITTER),
    (re.compile(r"https?://(www\.)?github\.com/[\w\-]+/[\w\-]+"), Platform.GITHUB),
    # Audible book URLs (audible.com product pages + shortened amzn.to links).
    (re.compile(r"https?://(www\.)?audible\.com/pd/"), Platform.AUDIBLE),
    (re.compile(r"https?://amzn\.to/"), Platform.AUDIBLE),
    # Discord invite/channel links — don't scrape, just capture context.
    (re.compile(r"https?://(www\.)?discord\.(com|gg)/"), Platform.DISCORD_LINK),
    # Media platforms where scraping gives garbage — capture context only.
    (re.compile(r"https?://(open\.)?spotify\.com/"), Platform.MEDIA_LINK),
    (re.compile(r"https?://(www\.)?tiktok\.com/"), Platform.MEDIA_LINK),
    (re.compile(r"https?://(music\.)?apple\.com/"), Platform.MEDIA_LINK),
]

# Tracking params to strip for cleaner dedup.
_TRACKING_PARAMS = {"igsh", "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
                     "si", "ref", "fbclid", "gclid", "mc_cid", "mc_eid"}


def classify(url: str) -> Platform:
    """Return the platform for a URL, defaulting to WEB."""
    for pattern, platform in _PATTERNS:
        if pattern.search(url):
            return platform
    return Platform.WEB


def clean_url(url: str) -> str:
    """Strip tracking parameters for cleaner dedup."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=False)
    cleaned = {k: v for k, v in params.items() if k.lower() not in _TRACKING_PARAMS}
    clean_query = urlencode(cleaned, doseq=True) if cleaned else ""
    # Strip trailing ) that sometimes gets captured from markdown.
    path = parsed.path.rstrip(")")
    # Strip .git suffix from GitHub URLs (causes API 404).
    if parsed.netloc in ("github.com", "www.github.com") and path.endswith(".git"):
        path = path[:-4]
    return urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, clean_query, ""))


# Regex to extract all http(s) URLs from a message.
URL_RE = re.compile(r"https?://[^\s<>\"']+")


def extract_urls(text: str) -> list[str]:
    """Extract all URLs from a Discord message, cleaned of tracking params."""
    raw = URL_RE.findall(text)
    return [clean_url(u) for u in raw]
