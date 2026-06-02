from app.annas_api import Annas_API, Book_DTO, Book_Query_Parameters
import asyncio
from dotenv import load_dotenv
from pathlib import Path
import os
import re
from typing import Optional
import json

import asyncio
from functools import wraps

def async_retry(max_attempts=3, delay=1, exceptions=(Exception,)):
    """
    Retry decorator for async functions.
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            attempts = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    attempts += 1
                    if attempts >= max_attempts:
                        raise
                    await asyncio.sleep(delay)
        return wrapper
    return decorator


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

    with open("bundles_data.json", "r") as f:
        bundles = json.load(f)

    for bundle in bundles:
        for book in bundle["books"]:
            title = book["title"]
            author = book["author"]

            query = Book_Query_Parameters(
                q=title,
                # author=author,
                # ext=book.get("format", "pdf").lower()
                ext="pdf",
                # sort="mostRelevant",
                # source="libgenLi, libgenRs"
            )
            print(f"SEARCHING BOOK -> {query.q}")
            retry_ = async_retry()
            # books = await retry_(annas_api.search_book(query))
            books = await retry_(annas_api.search_book)(query)

            print(f"BOOKs FOUND FOR -> {query.q}")
            await asyncio.sleep(1)


    # books = await annas_api.search_book(query)
    # print(await annas_api.download_book(books[0].md5))

    await annas_api.disconnect_session()



if __name__ == "__main__":
    asyncio.run(main())

