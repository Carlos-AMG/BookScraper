from app.annas_api import Annas_API, Book_DTO, Book_Query_Parameters
import asyncio
from dotenv import load_dotenv
from pathlib import Path
import os
import re
from typing import Optional


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




async def main():
    load_dotenv()
    base_url = "https://annas-archive-api.p.rapidapi.com"
    bundles_file = "bundles_data.json"
    output_dir = Path("./BUNDLES")
    API_KEY = os.getenv("API_KEY")
    annas_api = Annas_API(base_url, API_KEY)

    book_data = {
        "title": "Financial Data Engineering",
        "author": "",
        "publisher": "O'Reilly",
        "description": "Today, investment in financial technology and digital transformation is reshaping the financial landscape and generating many opportunities. Too often, however, engineers and professionals in financial institutions lack a practical and comprehensive understanding of the concepts, problems, techniques, and technologies necessary to build a modern, reliable, and scalable financial data infrastructure. This is where financial data engineering is needed. A data engineer developing a data infrastructure for a financial product possesses not only technical data engineering skills but also a solid understanding of financial domain-specific challenges, methodologies, data ecosystems, providers, formats, technological constraints, identifiers, entities, standards, regulatory requirements, and governance. This book offers a comprehensive, practical, domain-driven approach to financial data engineering, featuring real-world use cases, industry practices, and hands-on projects. Tamer Khraisha, PhD, is a senior data engineer and scientific author with more than a decade of experience in the financial sector.",
        "format": "PDF",
        "edition": ""
      }

    title = book_data["title"]
    author = book_data["author"]

    query = Book_Query_Parameters(
        q=title,
        author=author,
        ext=book_data.get("format", "pdf").lower()
    )


    books = await annas_api.search_book(query)
    print(await annas_api.download_book(books[0].md5))

    await annas_api.disconnect_session()



if __name__ == "__main__":
    asyncio.run(main())

