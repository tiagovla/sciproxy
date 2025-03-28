from sciproxy.downloaders.abc import Downloader
import sciproxy.utils
import aiohttp
import logging
from typing import Optional

logger = logging.getLogger(__name__)

IEEE_HOSTNAME = "ieeexplore.ieee.org"


class IEEEDownloader(Downloader):
    def __init__(self, hostname: str, proxy_url: Optional[str] = None):
        """
        Initialize the IEEE Downloader.

        :param hostname: The hostname for IEEE (e.g., "ieeexplore.ieee.org").
        :param proxy_url: Optional proxy URL for requests.
        """
        self.hostname = hostname
        self.proxy_url = proxy_url

    def _get_pdf_url(self, doc_id: str) -> str:
        """
        Construct the PDF URL for a given document ID.

        :param doc_id: The document ID.
        :return: The constructed PDF URL.
        """
        return f"https://{self.hostname}/stampPDF/getPDF.jsp?tp=&arnumber={doc_id}"

    async def fetch_pdf(
        self, doi: str, session: aiohttp.ClientSession
    ) -> Optional[aiohttp.ClientResponse]:
        """
        Fetch the PDF for the given DOI from IEEE.

        :param doi: The DOI of the document.
        :param session: An active aiohttp.ClientSession.
        :return: The aiohttp.ClientResponse if successful, None otherwise.
        """
        logger.info(f"Fetching PDF for DOI {doi} from IEEE")

        try:
            redirect_url = await sciproxy.utils.get_redirect_url(doi, session)
        except Exception as e:
            logger.error(f"Failed to get redirect URL for DOI {doi}: {e}")
            return None

        if IEEE_HOSTNAME not in redirect_url:
            logger.debug(
                f"Redirect URL does not contain '{IEEE_HOSTNAME}': {redirect_url}"
            )
            return None

        try:
            doc_id = redirect_url.split("/")[
                redirect_url.split("/").index("document") + 1
            ]
        except (ValueError, IndexError) as e:
            logger.error(
                f"Failed to extract document ID from redirect URL: {redirect_url}, Error: {e}"
            )
            return None

        logger.info(f"Redirect URL obtained: {redirect_url}, Document ID: {doc_id}")
        return await self.fetch_pdf_doc_id(doc_id, session)

    async def fetch_pdf_doc_id(
        self, doc_id: str, session: aiohttp.ClientSession
    ) -> Optional[aiohttp.ClientResponse]:
        """
        Fetch the PDF from IEEE using the document ID.

        :param doc_id: The document ID.
        :param session: An active aiohttp.ClientSession.
        :return: The aiohttp.ClientResponse if successful, None otherwise.
        """
        url = self._get_pdf_url(doc_id)
        logger.info(f"Fetching PDF from URL: {url}")
        logger.info(f"Proxy URL: {self.proxy_url}")

        try:
            async with session.get(url, proxy=self.proxy_url) as response:
                if response.status != 200:
                    logger.warning(
                        f"Failed to fetch PDF from IEEE ({self.hostname}): {url}, Status: {response.status}"
                    )
                    return None
                return response
        except aiohttp.ClientError as e:
            logger.error(f"HTTP request failed for URL {url}: {e}")
            return None
