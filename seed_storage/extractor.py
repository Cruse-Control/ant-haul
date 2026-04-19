"""Content extraction pipeline — fetches and cleans content from URLs."""

from __future__ import annotations

import httpx
from readability import Document
from bs4 import BeautifulSoup


async def extract_url(url: str, timeout: float = 30) -> dict:
    """Fetch a URL and extract its readable content.

    Returns dict with: title, content (clean text), url, word_count.
    """
    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
        resp = await client.get(url, headers={"User-Agent": "SeedStorage/1.0"})
        resp.raise_for_status()

    doc = Document(resp.text)
    html_content = doc.summary()
    title = doc.title()

    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    return {
        "title": title,
        "content": text,
        "url": str(resp.url),
        "word_count": len(text.split()),
    }


async def extract_youtube_transcript(video_id: str) -> dict:
    """Placeholder for YouTube transcript extraction.
    Will integrate with existing youtube-transcript skill.
    """
    # TODO: integrate with /home/flynn-cruse/.claude/skills youtube-transcript
    return {
        "title": f"YouTube video {video_id}",
        "content": "",
        "url": f"https://youtube.com/watch?v={video_id}",
        "word_count": 0,
    }
