"""Concurrent scraping script for Humble Bundle."""
import asyncio
import json
from pathlib import Path
from app.scraper import ConcurrentHumbleBundleScraper


async def scrape_all_bundles():
    """Scrape all bundles from the books page concurrently."""
    async with ConcurrentHumbleBundleScraper(headless=True, max_concurrent=5) as scraper:
        bundles = await scraper.scrape_all_bundles_concurrently()

        # Print summary
        print("\nSummary:")
        print("=" * 80)
        for bundle in bundles:
            print(f"- {bundle.title}: {bundle.total_books} books")


async def scrape_specific_bundles():
    """Scrape specific bundles concurrently."""
    # Define the bundles you want to scrape
    bundle_urls = [
        # "https://www.humblebundle.com/books/software-architecture-pearson-books",
        # "https://www.humblebundle.com/books/hacking-no-starch-books?"
        # "https://www.humblebundle.com/books/coding-challenges-and-interview-prep-mammoth-books"
        # "https://www.humblebundle.com/books/networking-and-security-cert-prep-pearson-it-certification-exam-cram-books-encore"
        "https://www.humblebundle.com/books/javascript-and-typescript-mastery-packt-books"
        # Add more URLs as needed
    ]

    async with ConcurrentHumbleBundleScraper(headless=True, max_concurrent=3) as scraper:
        bundles = await scraper.scrape_specific_bundles(bundle_urls)

        # Print summary
        print("\nSummary:")
        print("=" * 80)
        for bundle in bundles:
            print(f"- {bundle.title}: {bundle.total_books} books")
            print(f"  Books:")
            for i, book in enumerate(bundle.books[:5], 1):  # Show first 5 books
                print(f"    {i}. {book.title} - {book.author}")
            if bundle.total_books > 5:
                print(f"    ... and {bundle.total_books - 5} more books")


async def main():
    """Main entry point."""
    print("Humble Bundle Concurrent Scraper")
    print("=" * 80)
    print("1. Scrape all bundles (concurrent)")
    print("2. Scrape specific bundles (concurrent)")
    print("=" * 80)

    choice = input("Enter choice (1 or 2): ").strip()

    if choice == '1':
        await scrape_all_bundles()
    elif choice == '2':
        await scrape_specific_bundles()
    else:
        print("Invalid choice!")


if __name__ == "__main__":
    # For non-interactive use:
    # asyncio.run(scrape_all_bundles())
    # asyncio.run(scrape_specific_bundles())

    asyncio.run(main())
