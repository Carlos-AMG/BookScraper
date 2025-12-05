"""Page Object Model classes for Humble Bundle pages."""
from abc import ABC, abstractmethod
from playwright.async_api import Page
from typing import List
from app.models.humble_models import Book, Bundle


class BasePage(ABC):
    """Base page object."""

    def __init__(self, page: Page):
        self.page = page

    @abstractmethod
    async def is_loaded(self) -> bool:
        """Check if page is fully loaded."""
        pass


class BooksListingPage(BasePage):
    """Page object for the books listing page (https://www.humblebundle.com/books)."""

    async def is_loaded(self) -> bool:
        """Check if books listing page is loaded."""
        return await self.page.locator('h1, h2').first.count() > 0

    # First 3 links aren't actually the books, maybe pass an offset and start from link 4
    async def get_bundle_urls(self) -> List[str]:
        """Extract all bundle URLs from the books listing page."""
        await self.page.wait_for_selector('a[href*="/books/"]', timeout=10000)

        # Get all book bundle links
        links = await self.page.locator('a[href*="/books/"]').all()

        bundle_urls = []
        # for link in links:
        for link in links[2:]:
            href = await link.get_attribute('href')
            if href and '/books/' in href and href not in bundle_urls:
                # Make absolute URL if needed
                if href.startswith('/'):
                    href = f'https://www.humblebundle.com{href}'
                bundle_urls.append(href)

        return bundle_urls


class BundleDetailPage(BasePage):
    """Page object for a specific bundle detail page."""

    SKIP_HEADINGS = [
        'Bundle Filters', 'Pay What You Want', 'Bundles You May',
        'Discover how', 'Value', 'US$', 'Charity', 'Bundle Details',
        'Leaderboard', 'About', 'Trending', 'Book Bundle'
    ]

    async def is_loaded(self) -> bool:
        """Check if bundle detail page is loaded."""
        return await self.page.locator('h1').count() > 0

    async def get_bundle_title(self) -> str:
        """Extract the bundle title."""
        title_elem = self.page.locator('h1').first
        if await title_elem.count() > 0:
            title = await title_elem.text_content()
            return title.strip() if title else "Unknown Bundle"
        return "Unknown Bundle"

    async def get_bundle_description(self) -> str:
        """Extract the bundle description."""
        try:
            desc_paragraph = self.page.locator('h2:has-text("Discover")').locator('xpath=following-sibling::p[1]')
            if await desc_paragraph.count() > 0:
                desc = await desc_paragraph.first.text_content()
                return desc.strip() if desc else ""
        except:
            pass
        return ""

    async def extract_books(self) -> List[Book]:
        """Extract all books from the bundle using JavaScript."""
        await self.page.wait_for_selector('h2', timeout=15000)
        await self.page.wait_for_timeout(3000)  # Wait for dynamic content

        books_data = await self.page.evaluate(f"""
            () => {{
                const books = [];
                const skipWords = {self.SKIP_HEADINGS};

                // Find all h2 elements that are book titles
                const headings = Array.from(document.querySelectorAll('h2'));

                headings.forEach(h2 => {{
                    const title = h2.textContent.trim();

                    // Skip non-book headings
                    if (skipWords.some(word => title.includes(word)) || !title) {{
                        return;
                    }}

                    // Find the parent container (usually 3 levels up)
                    let container = h2.parentElement;
                    for (let i = 0; i < 3 && container; i++) {{
                        container = container.parentElement;
                    }}

                    if (!container) return;

                    const book = {{
                        title: title,
                        author: '',
                        publisher: '',
                        description: '',
                        format: 'PDF',
                        edition: ''
                    }};

                    // Extract author
                    const authorDivs = Array.from(container.querySelectorAll('div'));
                    for (const div of authorDivs) {{
                        const text = div.textContent.trim();
                        if (text.startsWith('Author:') && text.length < 100) {{
                            book.author = text.replace('Author:', '').trim();
                            break;
                        }}
                    }}

                    // Extract publisher
                    const publisherDivs = Array.from(container.querySelectorAll('div'));
                    for (const div of publisherDivs) {{
                        const text = div.textContent.trim();
                        if (text.startsWith('Publisher:') && text.length < 50) {{
                            const link = div.querySelector('a');
                            book.publisher = link ? link.textContent.trim() : text.replace('Publisher:', '').trim();
                            break;
                        }}
                    }}

                    // Extract description from paragraphs
                    const paragraphs = Array.from(container.querySelectorAll('p'));
                    const descriptions = [];
                    paragraphs.forEach(p => {{
                        const text = p.textContent.trim();
                        if (text.length > 50 &&
                            !text.includes('Pay at least') &&
                            !text.includes('Publisher:') &&
                            !text.includes('Author:')) {{
                            descriptions.push(text);
                        }}
                    }});
                    if (descriptions.length > 0) {{
                        book.description = descriptions.join(' ');
                    }}

                    // Extract edition from title if present
                    if (title.includes('/e')) {{
                        const parts = title.split(',');
                        if (parts.length > 1) {{
                            book.edition = parts[parts.length - 1].trim();
                        }}
                    }}

                    books.push(book);
                }});

                return books;
            }}
        """)

        return [Book(**book_data) for book_data in books_data]

    async def scrape_bundle(self, url: str) -> Bundle:
        """Scrape a complete bundle from the given URL."""
        # Navigate to the bundle page
        await self.page.goto(url, wait_until="domcontentloaded")
        await self.is_loaded()

        # Extract bundle metadata
        title = await self.get_bundle_title()
        description = await self.get_bundle_description()

        # Determine publisher from URL or description
        publisher = ""
        if 'pearson' in url.lower() or 'pearson' in description.lower():
            publisher = "Pearson"
        elif 'oreilly' in url.lower() or "o'reilly" in description.lower():
            publisher = "O'Reilly"
        elif 'manning' in url.lower() or 'manning' in description.lower():
            publisher = "Manning"
        elif 'packt' in url.lower() or 'packt' in description.lower():
            publisher = "Packt"

        # Create bundle object
        bundle = Bundle(
            title=title,
            url=url,
            description=description,
            publisher=publisher
        )

        # Extract books
        books = await self.extract_books()
        for book in books:
            bundle.add_book(book)

        return bundle
