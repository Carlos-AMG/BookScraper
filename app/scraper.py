"""Concurrent scraper for Humble Bundle with file locking."""
import asyncio
import json
from pathlib import Path
from typing import List, Optional
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from .models.humble_models import Bundle
from .pages.humble_pom import BooksListingPage, BundleDetailPage


class ConcurrentHumbleBundleScraper:
    """Concurrent scraper with file locking for safe JSON writes."""

    def __init__(self, headless: bool = True, max_concurrent: int = 5):
        self.headless = headless
        self.max_concurrent = max_concurrent
        self.browser: Optional[Browser] = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.file_lock = asyncio.Lock()
        self.output_file = Path("bundles_data.json")

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def start(self) -> None:
        """Start the browser."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)

    async def close(self) -> None:
        """Close the browser."""
        if self.browser:
            await self.browser.close()
        if hasattr(self, 'playwright'):
            await self.playwright.stop()

    async def get_bundle_urls(self) -> List[str]:
        """Get all bundle URLs from the books listing page."""
        context = await self.browser.new_context()
        page = await context.new_page()

        try:
            print("Opening books listing page...")
            await page.goto('https://www.humblebundle.com/books', wait_until="domcontentloaded")

            # Wait for content to load
            await page.wait_for_selector('a[href*="/books/"]', timeout=15000)
            await page.wait_for_timeout(2000)  # Extra wait for dynamic content

            books_page = BooksListingPage(page)
            urls = await books_page.get_bundle_urls()

            print(f"Found {len(urls)} bundle URLs")
            return urls

        finally:
            await page.close()
            await context.close()

    async def scrape_single_bundle(self, url: str, bundle_index: int, total: int) -> Optional[Bundle]:
        """Scrape a single bundle with semaphore control."""
        async with self.semaphore:
            context = await self.browser.new_context()
            page = await context.new_page()

            try:
                print(f"[{bundle_index}/{total}] Scraping: {url}")

                # Navigate and wait for content
                await page.goto(url, wait_until="domcontentloaded")
                await page.wait_for_selector('h2', timeout=15000)

                # Wait for carousel to render (important for getting all books)
                await page.wait_for_timeout(3000)

                # Scrape the bundle
                bundle_page = BundleDetailPage(page)
                bundle = await bundle_page.scrape_bundle(url)

                print(f"[{bundle_index}/{total}] ✓ {bundle.title} - {bundle.total_books} books")

                # Save to file with lock
                await self.save_bundle_to_file(bundle)

                return bundle

            except Exception as e:
                print(f"[{bundle_index}/{total}] ✗ Error: {e}")
                return None

            finally:
                await page.close()
                await context.close()

    # This can be better with aiofiles
    async def save_bundle_to_file(self, bundle: Bundle) -> None:
        """Save bundle to JSON file with file locking."""
        async with self.file_lock:
            # Read existing data
            if self.output_file.exists():
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    try:
                        data = json.load(f)
                        if not isinstance(data, list):
                            data = []
                    except json.JSONDecodeError:
                        data = []
            else:
                data = []

            # Add new bundle
            data.append(bundle.to_dict())

            # Write back to file
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

    async def scrape_all_bundles_concurrently(self) -> List[Bundle]:
        """Scrape all bundles concurrently."""
        # Clear output file at start
        if self.output_file.exists():
            self.output_file.unlink()

        # Get all bundle URLs
        urls = await self.get_bundle_urls()

        print(f"\n{'='*80}")
        print(f"Starting concurrent scraping of {len(urls)} bundles")
        print(f"Max concurrent connections: {self.max_concurrent}")
        print(f"{'='*80}\n")

        # Create tasks for all bundles
        tasks = [
            self.scrape_single_bundle(url, i + 1, len(urls))
            for i, url in enumerate(urls)
        ]

        # Run all tasks concurrently
        bundles = await asyncio.gather(*tasks)

        # Filter out None results (failed scrapes)
        successful_bundles = [b for b in bundles if b is not None]

        print(f"\n{'='*80}")
        print(f"Completed: {len(successful_bundles)}/{len(urls)} bundles scraped successfully")
        print(f"Data saved to: {self.output_file}")
        print(f"{'='*80}\n")

        return successful_bundles

    async def scrape_specific_bundles(self, urls: List[str]) -> List[Bundle]:
        """Scrape specific bundle URLs concurrently."""
        # Clear output file at start
        if self.output_file.exists():
            self.output_file.unlink()

        print(f"\n{'='*80}")
        print(f"Starting concurrent scraping of {len(urls)} specific bundles")
        print(f"Max concurrent connections: {self.max_concurrent}")
        print(f"{'='*80}\n")

        # Create tasks for specified bundles
        tasks = [
            self.scrape_single_bundle(url, i + 1, len(urls))
            for i, url in enumerate(urls)
        ]

        # Run all tasks concurrently
        bundles = await asyncio.gather(*tasks)

        # Filter out None results
        successful_bundles = [b for b in bundles if b is not None]

        print(f"\n{'='*80}")
        print(f"Completed: {len(successful_bundles)}/{len(urls)} bundles scraped successfully")
        print(f"Data saved to: {self.output_file}")
        print(f"{'='*80}\n")

        return successful_bundles
