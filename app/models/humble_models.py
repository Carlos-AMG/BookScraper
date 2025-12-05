"""Data models for Humble Bundle scraping."""
from dataclasses import dataclass, field
from typing import List


@dataclass
class Book:
    """Represents a book in a Humble Bundle."""
    title: str
    author: str = ""
    publisher: str = ""
    description: str = ""
    format: str = "PDF"
    edition: str = ""

    def to_dict(self) -> dict:
        """Convert book to dictionary."""
        return {
            'title': self.title,
            'author': self.author,
            'publisher': self.publisher,
            'description': self.description,
            'format': self.format,
            'edition': self.edition
        }


@dataclass
class Bundle:
    """Represents a Humble Bundle."""
    title: str
    url: str
    description: str = ""
    publisher: str = ""
    books: List[Book] = field(default_factory=list)

    @property
    def total_books(self) -> int:
        """Get total number of books in bundle."""
        return len(self.books)

    def to_dict(self) -> dict:
        """Convert bundle to dictionary."""
        return {
            'bundle_metadata': {
                'title': self.title,
                'description': self.description,
                'publisher': self.publisher,
                'total_books': self.total_books,
                'url': self.url
            },
            'books': [book.to_dict() for book in self.books]
        }

    def add_book(self, book: Book) -> None:
        """Add a book to the bundle."""
        self.books.append(book)
