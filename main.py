"""Scirpt to download data from https://commoncrawl.org/."""

import json
from pathlib import Path
from sys import stdout

import bcp47
import requests
from bs4 import BeautifulSoup
from loguru import logger
from warcio.archiveiterator import ArchiveIterator

CERTIFICATE_PATH: Path = Path.cwd() / ".certificates"

# The Common Crawl index you want to query
INDEX_NAME: str = "CC-MAIN-2026-04"

# It’s advisable to use a descriptive User-Agent string when developing your own applications.
# This practice aligns with the conventions outlined in RFC 7231. Let's use this simple one:
AGENT_NAME: str = (
    "cc-get-started/1.0 (Example data retrieval script; yourname@example.com)"
)

NON_GERMAN_BCP47_CODES: set[str] = {
    bcp47_code
    for bcp47_name, bcp47_code in bcp47.languages.items()
    if "German" not in bcp47_name
    # Skip the short code like 'ar', because they would filter too many records!
    if len(bcp47_code) >= 5
}


def is_marked_as_german(html: bytes) -> bool:
    """Check if HTML contains <meta name="language" content="de"/>."""
    soup = BeautifulSoup(markup=html, features="html.parser")

    if language_meta_tag := soup.find(name="meta", attrs={"name": "language"}):
        match language_meta_tag.attrs.get("content"):
            case "de":
                return True
            case None:
                logger.warning("HTML language meta-tag doesn't contain 'content'.")
                return False
            case _ as other_language:
                logger.debug(f"Page is not marked as German, but {other_language}.")
                return False

    logger.warning("No language-tag found!")

    return False


def fetch_index() -> list[dict[str, str]]:
    """Search the Common Crawl Index."""
    response = requests.get(
        url=f"https://index.commoncrawl.org/{INDEX_NAME}-index",
        params={"url": "*.de", "output": "json"},
        headers={"user-agent": AGENT_NAME},
        verify=str(CERTIFICATE_PATH / "index.commoncrawl.org.pem"),
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.critical(f"Failed to fetch index: {response.status_code}")

        return []
    else:
        records = [json.loads(record) for record in response.text.strip().split("\n")]

        logger.info(f"Received {len(records):_d} records from Common Crawl")

        return records


def fetch_page(file_name: str, offset: int, length: int) -> bytes | None:
    """Fetch content from Common Crawl."""
    # We need to check the file_name for any hints that it is not worth downloading
    # it (because that would take too much time). Therefore, ...
    #
    # - ignore robots.txt, as they are not for humans but for search engines.
    if file_name.endswith("robots.txt"):
        logger.debug("Skipping robots.txt")

        return None
    #
    # - ignore URLs that contain a non-German language code in the URL.
    for bcp47_code in NON_GERMAN_BCP47_CODES:
        if bcp47_code in file_name:
            logger.debug(f"Skipping non-German site (i.e., {bcp47_code=})")

            return None

    url = f"https://data.commoncrawl.org/{file_name}"
    logger.info(f"Download from {url}")

    response = requests.get(
        url=url,
        headers={
            "user-agent": AGENT_NAME,
            "Range": f"bytes={offset}-{offset + length - 1}",
        },
        verify=str(CERTIFICATE_PATH / "data.commoncrawl.org.pem"),
        stream=True,  # Get a raw byte stream that is gzip-compressed!
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.critical(f"Failed to fetch data: {response.status_code}")

        return None
    else:
        if response.status_code == 206:
            for warc_record in ArchiveIterator(fileobj=response.raw):
                if warc_record.rec_type == "response":
                    return warc_record.content_stream().read()

        logger.info("No valid WARC record found in the given records")

        return None


def main() -> None:
    logger.remove()
    logger.add(
        sink=stdout,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS Z}</green> <red>|</red> "
            "<level>{level: <8}</level> <red>|</red> "
            "<level>{extra}</level> <red>|</red> "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>"
            " - <level>{message}</level>"
        ),
        level="TRACE",
    )

    for record in fetch_index()[:20]:
        with logger.contextualize(urlkey=record["urlkey"]):
            content = fetch_page(
                file_name=record["filename"],
                offset=int(record["offset"]),
                length=int(record["length"]),
            )

            if not content:
                continue

            # In theory that could work, but it is not mandatory for an HTML site to
            # declare the language. So it will certainly filter more sites than
            # intended.
            if not is_marked_as_german(html=content):
                continue

            logger.success("Now what?")


if __name__ == "__main__":
    main()
