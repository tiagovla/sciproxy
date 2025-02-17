from abc import ABC, abstractmethod
from typing import Optional
import aiohttp


class Downloader(ABC):
    @abstractmethod
    async def fetch_pdf(
        self, doi: str, session: aiohttp.ClientSession
    ) -> Optional[bytes]:
        """Fetch the PDF for the given DOI using the provided session."""
        pass
