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
                url = match.group(1)
                if url.startswith("//"):
                    pdf_url = "https:" + url
                else:
                    pdf_url = "https://sci-hub.se" + url
                logger.info(f"PDF URL obtained from Sci-Hub: {pdf_url}")

                pdf_response = await session.get(pdf_url)
                if pdf_response.status == 200:
                    return pdf_response
                else:
                    logger.warning(f"Failed to fetch PDF from Sci-Hub: {pdf_url}")
                    return None
            logger.warning(f"No PDF found for DOI {doi} on Sci-Hub")
            return None
