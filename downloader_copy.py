from app.annas_api import Annas_API, Book_Query_Parameters, Book_DTO
from prefect import flow, task, get_run_logger
from prefect.tasks import exponential_backoff
import asyncio
import json
import re
import aiohttp
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager


# =============================================================================
# Utility Functions
# =============================================================================

def normalize(s: str) -> str:
    """Normalize string for comparison: lowercase, remove special chars."""
    return re.sub(r'[^a-z0-9]+', ' ', s.lower()).strip()


def title_matches(api_title: str, user_title: str) -> bool:
    """Check if user title matches API title (substring or word match)."""
    api_n = normalize(api_title)
    user_n = normalize(user_title)

    if user_n in api_n:
        return True

    pattern = r'\b' + r'\s+'.join(re.escape(w) for w in user_n.split()) + r'\b'
    return re.search(pattern, api_n) is not None


def author_matches(api_author: str, user_author: str) -> bool:
    """Check if any user author token appears in API author."""
    api_tokens = set(normalize(api_author).split())
    user_tokens = set(normalize(user_author).split())
    return len(api_tokens & user_tokens) > 0


def find_matching_book(
    api_books: list[Book_DTO],
    user_title: str,
    user_author: str
) -> Optional[Book_DTO]:
    """Find first book that matches both title and author criteria."""
    for api_book in api_books:
        if title_matches(api_book.title, user_title) and author_matches(api_book.author, user_author):
            return api_book
    return None


# =============================================================================
# Resource Management
# =============================================================================

DEFAULT_ANNAS_API_URL = "https://annas-archive-api.p.rapidapi.com"


@asynccontextmanager
async def create_api_client(api_key: str, api_url: str = DEFAULT_ANNAS_API_URL):
    """Context manager that creates and cleans up API client and session."""
    logger = get_run_logger()

    annas_api = Annas_API(api_url, api_key, logger=logger)
    await annas_api.ensure_api_session()

    async with aiohttp.ClientSession() as session:
        try:
            yield annas_api, session
        finally:
            await annas_api.disconnect_session()


# =============================================================================
# Tasks
# =============================================================================

@task(
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    retry_jitter_factor=0.5,
    name="search_book"
)
async def search_book_task(
    annas_api: Annas_API,
    title: str,
    author: str = "",
    ext: str = "pdf"
) -> list[Book_DTO]:
    """Search for books via Anna's Archive API with retry logic."""
    logger = get_run_logger()
    annas_api.set_logger(logger)

    query = Book_Query_Parameters(q=title, author=author, ext=ext.lower())
    return await annas_api.search_book(query)


@task(
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    retry_jitter_factor=0.5,
    name="get_download_links"
)
async def get_download_links_task(
    annas_api: Annas_API,
    book_md5: str
) -> list[str]:
    """Get download links for a book with retry logic."""
    logger = get_run_logger()
    annas_api.set_logger(logger)
    return await annas_api.download_book(book_md5)


@task(name="download_file")
async def download_file_task(
    session: aiohttp.ClientSession,
    download_links: list[str],
    save_path: str
) -> bool:
    """Try downloading from multiple links until one succeeds."""
    logger = get_run_logger()
    path = Path(save_path)

    for i, link in enumerate(download_links):
        logger.info(f"Trying link {i + 1}/{len(download_links)}: {link[:80]}...")
        try:
            async with session.get(link, timeout=aiohttp.ClientTimeout(total=300)) as res:
                res.raise_for_status()
                content = await res.read()
                path.write_bytes(content)
                logger.info(f"Downloaded successfully to {path}")
                return True
        except Exception as e:
            logger.warning(f"Link {i + 1} failed: {e}")
            continue

    logger.error(f"All {len(download_links)} download links failed for {path.name}")
    return False


# =============================================================================
# Flows - Each is self-contained and deployable
# =============================================================================

@flow(name="download_single_book")
async def download_single_book_flow(
    api_key: str,
    title: str,
    author: str = "",
    ext: str = "pdf",
    output_dir: str = "./downloads",
    api_url: str = DEFAULT_ANNAS_API_URL
) -> bool:
    """
    Download a single book by title and author.

    Self-contained flow that can be deployed independently.
    """
    logger = get_run_logger()
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Searching for: '{title}' by {author}")

    async with create_api_client(api_key, api_url) as (annas_api, session):
        # Search for books
        try:
            api_books = await search_book_task(annas_api, title, author, ext)
        except Exception as e:
            logger.error(f"Search failed for '{title}' after retries: {e}")
            return False

        if not api_books:
            logger.warning(f"No results found for '{title}'")
            return False

        # Find matching book
        matched_book = find_matching_book(api_books, title, author)
        if not matched_book:
            logger.warning(f"No matching book found for '{title}' by {author}")
            logger.debug(f"API returned: {[b.title for b in api_books[:5]]}")
            return False

        logger.info(f"Found match: '{matched_book.title}' (md5: {matched_book.md5})")

        # Get download links
        try:
            download_links = await get_download_links_task(annas_api, matched_book.md5)
        except Exception as e:
            logger.error(f"Failed to get download links after retries: {e}")
            return False

        if not download_links:
            logger.warning(f"No download links available for '{title}'")
            return False

        logger.info(f"Found {len(download_links)} download links")

        # Build save path
        safe_title = normalize(matched_book.title).replace(" ", "_")[:50]
        save_path = output_path / f"{safe_title}.{matched_book.format.lower()}"

        if save_path.exists():
            logger.info(f"Already exists: {save_path}")
            return True

        return await download_file_task(session, download_links, str(save_path))


@flow(name="download_bundle")
async def download_bundle_flow(
    api_key: str,
    bundle: dict,
    output_dir: str = "./BUNDLES",
    max_concurrent: int = 3,
    api_url: str = DEFAULT_ANNAS_API_URL
) -> dict:
    """
    Download all books in a bundle.

    Args:
        api_key: API key for Anna's Archive API
        bundle: Dict with "bundle_metadata" (title, publisher) and "books" list
        output_dir: Base directory for downloads
        max_concurrent: Max concurrent book downloads
        api_url: Base URL for the API (optional)
    """
    logger = get_run_logger()
    base_path = Path(output_dir)

    metadata = bundle["bundle_metadata"]
    title = metadata.get("title", "Unknown")
    publisher = metadata.get("publisher", "Unknown")

    # Create bundle directory
    dir_name = f"{normalize(title).replace(' ', '_')}_{publisher}"
    bundle_dir = base_path / dir_name
    bundle_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Processing bundle: {title} ({len(bundle['books'])} books)")
    logger.info(f"Output directory: {bundle_dir}")

    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_book(book_info: dict) -> bool:
        async with semaphore:
            return await download_single_book_flow(
                api_key=api_key,
                title=book_info["title"],
                author=book_info.get("author", ""),
                ext=book_info.get("format", "pdf"),
                output_dir=str(bundle_dir),
                api_url=api_url
            )

    tasks = [process_book(book) for book in bundle["books"]]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Summarize
    successful = sum(1 for r in results if r is True)
    failed = len(results) - successful

    logger.info(f"Bundle '{title}' complete: {successful}/{len(results)} successful")

    return {
        "bundle": title,
        "total": len(bundle["books"]),
        "successful": successful,
        "failed": failed,
    }


@flow(name="download_all_bundles")
async def download_all_bundles_flow(
    api_key: str,
    bundles_file: str = "bundles_data.json",
    output_dir: str = "./BUNDLES",
    max_concurrent_per_bundle: int = 3,
    api_url: str = DEFAULT_ANNAS_API_URL
) -> list[dict]:
    """
    Download all bundles from a JSON file.

    Args:
        api_key: API key for Anna's Archive API
        bundles_file: Path to JSON file with bundle data
        output_dir: Base directory for downloads
        max_concurrent_per_bundle: Max concurrent downloads per bundle
        api_url: Base URL for the API (optional)
    """
    logger = get_run_logger()

    # Load bundles
    with open(bundles_file, "r") as f:
        bundle_data = json.load(f)

    logger.info(f"Loaded {len(bundle_data)} bundles from {bundles_file}")

    results = []
    for bundle in bundle_data:
        result = await download_bundle_flow(
            api_key=api_key,
            bundle=bundle,
            output_dir=output_dir,
            max_concurrent=max_concurrent_per_bundle,
            api_url=api_url
        )
        results.append(result)

    # Final summary
    logger.info("=" * 60)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 60)
    for r in results:
        logger.info(f"  {r['bundle']}: {r['successful']}/{r['total']} successful")

    total_success = sum(r['successful'] for r in results)
    total_books = sum(r['total'] for r in results)
    logger.info(f"TOTAL: {total_success}/{total_books} books downloaded")

    return results


# =============================================================================
# Entry point for local runs
# =============================================================================

# if __name__ == "__main__":
#     from dotenv import load_dotenv
#     load_dotenv()
#     asyncio.run(download_all_bundles_flow())
