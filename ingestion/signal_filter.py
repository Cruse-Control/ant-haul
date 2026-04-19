"""Signal filter — drop noise before staging.

Deterministic regex-based deny-list. No LLM calls.
URLs always pass (they are seed information).
"""

import re

# Messages matching any of these patterns are noise.
_NOISE_PATTERNS = [
    re.compile(r"^\[NOTIFY\]", re.IGNORECASE),
    re.compile(r"^QUEEN_ANT", re.IGNORECASE),
    re.compile(r"^⚠️?\s*(task|daemon|credential)", re.IGNORECASE),
    re.compile(r"^\[?(ERROR|WARN|INFO|DEBUG)\]?", re.IGNORECASE),
    re.compile(r"^(ok|thanks|thx|ty|lol|lmao|haha|nice|wow|yep|yea|nah|np|gg|brb|afk)$", re.IGNORECASE),
]

MIN_CONTENT_LENGTH = 5


def is_noise(text: str) -> bool:
    """Return True if the message is noise that should not be staged.

    URLs are never noise — they are seed information. This function
    should only be called on the text portion; the watcher should
    check for URLs separately and always stage those.
    """
    text = text.strip()

    if len(text) < MIN_CONTENT_LENGTH:
        return True

    for pattern in _NOISE_PATTERNS:
        if pattern.search(text):
            return True

    return False
