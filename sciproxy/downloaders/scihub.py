import asyncio
import re
from sciproxy.client import Optional
from sciproxy.downloaders.abc import Downloader
import aiohttp
import logging

logger = logging.getLogger(__name__)

class SciHubDownloader(Downloader):
    async def fetch_pdf(
        self, doi: str, session: aiohttp.ClientSession
    ) -> Optional[aiohttp.ClientResponse]:
        """Fetch the PDF for the given DOI from Sci-Hub."""
        logger.info(f"Fetching PDF for DOI {doi} from Sci-Hub")
        return await self.get_pdf_from_sci_hub(doi, session)

    async def get_pdf_from_sci_hub(
        self, doi: str, session: aiohttp.ClientSession
    ) -> Optional[aiohttp.ClientResponse]:
        """Get the PDF from Sci-Hub using the DOI."""
        url = f"https://sci-hub.se/{doi}"
        async with session.get(url) as response:
            text = await response.text()
            match = re.search(r"location\.href='(/[^']*)'", text)
            if match:
                pdf_url = self._construct_pdf_url(match.group(1))
                logger.info(f"PDF URL obtained from Sci-Hub: {pdf_url}")
                return await self._retry_fetch_pdf(pdf_url, session)
            logger.warning(f"No PDF found for DOI {doi} on Sci-Hub")
            return None

    def _construct_pdf_url(self, url: str) -> str:
        """Construct the full PDF URL from the matched URL."""
        if url.startswith("//"):
            return "https:" + url
        return "https://sci-hub.se" + url

    async def _retry_fetch_pdf(
        self, pdf_url: str, session: aiohttp.ClientSession, retry_limit: int = 4
    ) -> Optional[aiohttp.ClientResponse]:
        """Retry fetching the PDF if a ConnectionRefusedError occurs with increasing delay."""
        for attempt in range(retry_limit):
            try:
                pdf_response = await session.get(pdf_url)
                if pdf_response.status == 200:
                    return pdf_response
                logger.warning(f"Failed to fetch PDF from Sci-Hub: {pdf_url}")
                return None
            except aiohttp.ClientConnectionError:
                logger.warning(f"Connection refused on attempt {attempt + 1} to fetch PDF from Sci-Hub: {pdf_url}")
                await asyncio.sleep(3 ** attempt)  # Exponential backoff using attempt index
        logger.warning(f"Failed to fetch PDF from Sci-Hub after {retry_limit} attempts: {pdf_url}")
        return None
