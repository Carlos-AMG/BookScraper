from app.annas_api import Annas_API, Book_Query_Parameters, Book_DTO
import asyncio
import json
import os 
import logging
import re
import aiohttp
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("downloader")

def normalize(s: str) -> str:
    return re.sub(r'[^a-z0-9]+', ' ', s.lower()).strip()

def title_matches(api_title: str, user_title: str) -> bool:
    api_n = normalize(api_title)
    user_n = normalize(user_title)

    if user_n in api_n:
        return True

    pattern = r'\b' + r'\s+'.join(re.escape(w) for w in user_n.split()) + r'\b'
    return re.search(pattern, api_n) is not None

def author_matches(api_author: str, user_author: str) -> bool:
    api_tokens = set(normalize(api_author).split())
    user_tokens = set(normalize(user_author).split())

    # Match if ANY user author token appears in API author tokens
    return len(api_tokens & user_tokens) > 0

def find_matching_book(api_books: list[Book_DTO], user_title: str, user_author: str) -> Optional[Book_DTO]:
    for api_book in api_books:
        if title_matches(api_book.title, user_title) and author_matches(api_book.author, user_author):
            return api_book
    return None

async def download_(
    session: aiohttp.ClientSession,
    download_links: list[str],
    save_path: Path
) -> bool:
    for i, link in enumerate(download_links):
        logger.info(f"Trying link {i + 1}/{len(download_links)}: {link[:80]}...")
        try:
            async with session.get(link, timeout=aiohttp.ClientTimeout(total=300)) as res:
                res.raise_for_status()
                content = await res.read()

                save_path.write_bytes(content)
                logger.info(f"Downloaded successfully to {save_path}")
                return True
        except Exception as e:
            logger.warning(f"Link {i + 1} failed: {e}")
            continue
    return False

async def download_book(
    annas_api: Annas_API,
    session: aiohttp.ClientSession,
    book_info: dict,
    output_dir: Path,
    semaphore: asyncio.Semaphore
) -> bool:
    title = book_info["title"]
    author = book_info.get("author", "")

    async with semaphore: 
        logger.info(f"Searching for: '{title}' by {author}")
        query = Book_Query_Parameters(
            q=title,
            author=author,
            ext=book_info.get("format", "pdf").lower()
        )

        try:
            api_books = await annas_api.search_book(query)
        except Exception as e:
            logger.error(f"Search failed for '{title}': {e}")
            return False

        if not api_books:
            logger.warning(f"No results found for '{title}'")
            return False
        
        # Find matching book 
        matched_book = find_matching_book(api_books, title, author)
        if not matched_book:
            logger.warning(f"  No matching book found for '{title}' by {author}")
            logger.debug(f"  API returned: {[b.title for b in api_books[:5]]}")
            return False
        
        logger.info(f"Found match: '{matched_book.title}' (md5: {matched_book.md5})")

        # Get download links
        try:
            download_links = await annas_api.download_book(matched_book.md5)
        except Exception as e:
            logger.error(f"Failed to get download links: {e}")
            return False
        
        if not download_links:
            logger.warning(f"No download links available for '{title}'")
            return False
        
        logger.info(f"Found {len(download_links)} download links")

        # Build save path
        safe_title = normalize(matched_book.title).replace(" ", "_")[:50]
        save_path = output_dir / f"{safe_title}.{matched_book.format.lower()}"

        if save_path.exists():
            logger.info(f"Already exists: {save_path}")
            return True
        return await download_(session, download_links, save_path)

async def process_bundle(
    annas_api: Annas_API,
    session: aiohttp.ClientSession,
    bundle: dict,
    base_dir: Path,
    semaphore: asyncio.Semaphore
) -> dict:
    metadata = bundle["bundle_metadata"]
    title = metadata.get("title", "Unknown")
    publisher = metadata.get("publisher", "Unknown")

    # Create bundle directory
    # Create bundle directory
    dir_name = f"{normalize(title).replace(' ', '_')}_{publisher}"
    bundle_dir = base_dir / dir_name
    bundle_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"\n{'='*60}")
    logger.info(f"Processing bundle: {title} ({len(bundle['books'])} books)")
    logger.info(f"Output directory: {bundle_dir}")
    logger.info(f"{'='*60}\n")

    # Download all books in the bundle concurrently
    tasks = [
        download_book(annas_api, session, book, bundle_dir, semaphore)
        for book in bundle["books"]
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Summarize results 
    successful = sum(1 for r in results if r is True)
    failed = len(results) - successful

    return {
        "bundle": title,
        "total": len(bundle["books"]),
        "successful": successful,
        "failed": failed,
    }

async def main():
    # Configuration
    load_dotenv()
    base_url = "https://annas-archive-api.p.rapidapi.com"
    bundles_file = "bundles_data.json"
    output_dir = Path("./BUNDLES")
    API_KEY = os.getenv("API_KEY")
    max_concurrent_downloads = 1  # Be nice to the API

    with open(bundles_file, "r", encoding="utf-8") as f:
        bundle_data = json.load(f)

    semaphore = asyncio.Semaphore(max_concurrent_downloads)
    annas_api = Annas_API(base_url, API_KEY)

    async with aiohttp.ClientSession() as session:
        await annas_api.ensure_api_session()

        try:
            # Process all bundles
            results = []
            for bundle in bundle_data:
                result = await process_bundle(
                    annas_api, session, bundle, output_dir, semaphore
                )
                results.append(result)

            logger.info(f"\n{'='*60}")
            logger.info("DOWNLOAD SUMMARY")
            logger.info(f"{'='*60}")
            for r in results:
                logger.info(f"  {r['bundle']}: {r['successful']}/{r['total']} successful")
            
            total_success = sum(r['successful'] for r in results)
            total_books = sum(r['total'] for r in results)
            logger.info(f"\n  TOTAL: {total_success}/{total_books} books downloaded")   

        finally:
            await annas_api.disconnect_session()

if __name__ == "__main__":
    asyncio.run(main())