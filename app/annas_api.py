import aiohttp
import os
from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from enum import Enum

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
    source: Optional[Sort_Enum] = None


class Annas_API:
    def __init__(self, url: str, api_key: str):
        self._api_session: aiohttp.ClientSession
        self._base_url = url
        self._api_key = api_key
        self._is_api_connected = False

    async def ensure_api_session(self):
        if self._is_api_connected:
            return
        self._api_session = aiohttp.ClientSession(headers={"X-RapidAPI-Key": self._api_key})
        self._is_api_connected = True

    async def disconnect_session(self):
        if self._is_api_connected:
            await self._api_session.close()
            self._is_api_connected = False
            self._api_session = None

    async def search_book(self, parameters: Book_Query_Parameters,) -> List[Book_DTO]:
        await self.ensure_api_session()
        url = self._base_url + "/search"
        res = await self._api_session.get(url, params=parameters.model_dump(exclude_none=True))
        res.raise_for_status()
        data = await res.json()
        
        return [Book_DTO(**book_data) for book_data in data.get("books")]

    async def search_journal(self, book_title: str):
        raise NotImplementedError

    # Improve this for concurrency (maybe make it return raw data and then we can decide how to parse, for now I believe returning download link is fine)
    async def download_book(self, book_id: str) -> list[str]:
        await self.ensure_api_session()
        url = self._base_url + "/download"
        res = await self._api_session.get(url, params={"md5": book_id})
        res.raise_for_status()
        return await res.json()

    async def fast_download(self, book_id: str) -> str:
        await self.ensure_api_session()
        url = self._base_url + "/download/subscriber"
        res = await self._api_session.get(url, params={"md5": book_id})
        res.raise_for_status()
        return await res.json()
    
    async def member_download(self, book_id: str, member_key: str) -> str:
        await self.ensure_api_session()
        url = self._base_url + "/download/member"
        res = await self._api_session.get(url, params={"md5": book_id, "mk": member_key})
        res.raise_for_status()
        return await res.json()