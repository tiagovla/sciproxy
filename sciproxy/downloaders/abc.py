from abc import ABC, abstractmethod
from typing import Optional
import aiohttp


class DownloaderUnavailable(Exception):
    """Raised when a source cannot be reached or is temporarily failing."""


class Downloader(ABC):
    @abstractmethod
    async def fetch_pdf(
        self, doi: str, session: aiohttp.ClientSession
    ) -> Optional[aiohttp.ClientResponse]:
        """Return a live 200 PDF response, or None. The caller releases the response."""
        pass
