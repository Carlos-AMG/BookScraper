import aiohttp
import os
from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from enum import Enum
import logging


class AnnasAPIError(Exception):
    """Base exception for Anna's Archive API errors."""
    pass


class AnnasAPIHTTPError(AnnasAPIError):
    """Raised when API returns an HTTP error status."""
    def __init__(self, status: int, message: str, url: str = ""):
        self.status = status
        self.url = url
        super().__init__(f"HTTP {status}: {message} (URL: {url})")

class Book_DTO(BaseModel):
    title: str
    author: str
    md5: str
    imgUrl: str
    size: str
    genre: str
    format: str 
    year: Optional[str] 
    sources: List[str]
    imgFallbackColor: str

class Sort_Enum(Enum):
    newest = "newest"
    largest = "largest"
    oldest = "oldest"
    smallest = "smallest"
    most_relevant = "mostRelevant"

class Source_Enum(Enum):
    libgen_li = "libgenLi"
    libgen_rs = "libgenRs"
    z_library = "zLibrary"
    internet_archive = "internetArchive"
    uploads = "uploads"
    nexus_stc = "nexusStc"
    duxiu = "duxiu"
    z_library_chinese = "zLibraryChinese"
    magz_db = "magzDb"
    sci_hub = "sciHub"

class Book_Query_Parameters(BaseModel):
    q: str
    author: Optional[str] = None
    cat: Optional[str] = Field(default=None, description="Book category")
    page: Optional[int] = None
    ext: Optional[str] = Field(default=None, description="Book Extension")
    sort: Optional[Sort_Enum] = None
    lang: Optional[str] = None
    source: Optional[Source_Enum] = None


class Annas_API:
    def __init__(self, url: str, api_key: str, logger: Optional[logging.Logger] = None):
        self._api_session: Optional[aiohttp.ClientSession] = None
        self._base_url = url
        self._api_key = api_key
        self._is_api_connected = False
        # Default to module logger if none provided; can be replaced with Prefect logger
        self._logger = logger or logging.getLogger(__name__)

    def set_logger(self, logger: logging.Logger) -> "Annas_API":
        """Set a custom logger (e.g., Prefect flow/task logger)."""
        self._logger = logger
        return self

    async def ensure_api_session(self):
        if self._is_api_connected:
            return
        self._logger.debug("Creating new API session")
        self._api_session = aiohttp.ClientSession(headers={"X-RapidAPI-Key": self._api_key})
        self._is_api_connected = True
        self._logger.info("API session established")

    async def disconnect_session(self):
        if self._is_api_connected:
            self._logger.debug("Closing API session")
            await self._api_session.close()
            self._is_api_connected = False
            self._api_session = None
            self._logger.info("API session closed")

    async def search_book(self, parameters: Book_Query_Parameters) -> List[Book_DTO]:
        await self.ensure_api_session()
        url = self._base_url + "/search"
        params = parameters.model_dump(exclude_none=True)
        self._logger.info(f"Searching books with query: {parameters.q}")
        self._logger.debug(f"Search parameters: {params}")

        res = await self._api_session.get(url, params=params)

        if res.status >= 400:
            error_text = await res.text()
            self._logger.error(f"Search request failed - Status: {res.status}, URL: {res.url}, Response: {error_text[:200]}")
            raise AnnasAPIHTTPError(res.status, f"Search failed: {error_text[:200]}", str(res.url))

        self._logger.debug(f"Search request successful, status: {res.status}")
        data = await res.json()
        self._logger.info(f"Found {len(data.get('books', []))} books")
        return [Book_DTO(**book_data) for book_data in data.get("books")]

    async def search_journal(self, book_title: str):
        raise NotImplementedError

    async def download_book(self, book_id: str) -> list[str]:
        """Get download links for a book by MD5 hash."""
        await self.ensure_api_session()
        url = self._base_url + "/download"
        self._logger.info(f"Fetching download links for book: {book_id}")

        res = await self._api_session.get(url, params={"md5": book_id})

        if res.status >= 400:
            error_text = await res.text()
            self._logger.error(f"Failed to get download links for {book_id} - Status: {res.status}, Response: {error_text[:200]}")
            raise AnnasAPIHTTPError(res.status, f"Download links failed: {error_text[:200]}", str(res.url))

        self._logger.debug(f"Download links retrieved successfully for: {book_id}")
        return await res.json()

    async def fast_download(self, book_id: str) -> str:
        """Get subscriber fast download link."""
        await self.ensure_api_session()
        url = self._base_url + "/download/subscriber"
        self._logger.info(f"Fetching fast download link for book: {book_id}")

        res = await self._api_session.get(url, params={"md5": book_id})

        if res.status >= 400:
            error_text = await res.text()
            self._logger.error(f"Fast download failed for {book_id} - Status: {res.status}, Response: {error_text[:200]}")
            raise AnnasAPIHTTPError(res.status, f"Fast download failed: {error_text[:200]}", str(res.url))

        self._logger.debug(f"Fast download link retrieved for: {book_id}")
        return await res.json()

    async def member_download(self, book_id: str, member_key: str) -> str:
        """Get member download link."""
        await self.ensure_api_session()
        url = self._base_url + "/download/member"
        self._logger.info(f"Fetching member download link for book: {book_id}")

        res = await self._api_session.get(url, params={"md5": book_id, "mk": member_key})

        if res.status >= 400:
            error_text = await res.text()
            self._logger.error(f"Member download failed for {book_id} - Status: {res.status}, Response: {error_text[:200]}")
            raise AnnasAPIHTTPError(res.status, f"Member download failed: {error_text[:200]}", str(res.url))

        self._logger.debug(f"Member download link retrieved for: {book_id}")
        return await res.json()