import asyncio
import logging
import urllib.parse
from typing import Optional

import aiohttp
from sciproxy.downloaders.abc import Downloader
import sciproxy.utils


logger = logging.getLogger(__name__)

DEFAULT_IEEE_HOSTNAME = "ieeexplore.ieee.org"
DEFAULT_TIMEOUT_SECONDS: int = 60


class IEEEDownloader(Downloader):
    """
    Downloader implementation for fetching PDF documents from IEEE Xplore.

    Resolve DOIs to IEEE document URLs, extract the document ID,
    and attempt to download the PDF using the standard IEEE mechanism.
    """

    def __init__(
        self,
        hostname: str = DEFAULT_IEEE_HOSTNAME,
        proxy_url: Optional[str] = None,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ):
        """
        Initialize the IEEE Downloader.

        Args:
            hostname: The base hostname for the IEEE Xplore instance.
                      Defaults to 'ieeexplore.ieee.org'.
            proxy_url: Optional proxy URL (e.g., 'http://user:pass@host:port')
                       to use for the final PDF download request.
            timeout_seconds: The total timeout in seconds for the PDF download request.
                             Defaults to 60 seconds.
        """
        if not hostname:
            raise ValueError("IEEE hostname cannot be empty.")
        if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be a positive integer.")

        self.hostname = hostname.lower()
        self.proxy_url = proxy_url
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)

        logger.info(
            f"IEEEDownloader initialized for hostname '{self.hostname}' "
            f"with proxy {'enabled' if proxy_url else 'disabled'} "
            f"and total timeout {timeout_seconds}s."
        )

    def _get_pdf_url(self, doc_id: str) -> str:
        """Construct the direct PDF download URL for a given IEEE document ID."""
        return f"https://{self.hostname}/stampPDF/getPDF.jsp?tp=&arnumber={doc_id}"

    async def _extract_doc_id_from_url(self, url_str: str) -> Optional[str]:
        """
        Safely extract the IEEE document ID from a document URL.
        """
        try:
            parsed_url = urllib.parse.urlparse(url_str)
            if parsed_url.netloc.lower() != DEFAULT_IEEE_HOSTNAME:
                return None
            path_segments = [seg for seg in parsed_url.path.split("/") if seg]
            try:
                doc_index = path_segments.index("document")
                if doc_index + 1 < len(path_segments):
                    potential_doc_id = path_segments[doc_index + 1]
                    if potential_doc_id.isdigit():
                        return potential_doc_id
            except (ValueError, IndexError):
                pass
        except Exception as e:
            logger.error(
                f"Error parsing URL or extracting doc ID from '{url_str}': {e}",
                exc_info=True,
            )
        logger.debug(f"Could not extract IEEE doc ID from URL: {url_str}")
        return None

    async def fetch_pdf(
        self, doi: str, session: aiohttp.ClientSession
    ) -> Optional[aiohttp.ClientResponse]:
        """
        Fetch the PDF for a given DOI by resolving it, extracting the doc ID,
        and then downloading.
        """
        logger.info(f"Attempting fetch for DOI {doi} via IEEE ({self.hostname})")
        redirect_url_str: Optional[str] = None
        try:
            redirect_url_str = await sciproxy.utils.get_redirect_url(doi, session)
            if not redirect_url_str:
                logger.warning(f"Failed to resolve DOI {doi} to a redirect URL.")
                return None
            logger.debug(f"DOI {doi} resolved to redirect URL: {redirect_url_str}")
        except Exception as e:
            logger.error(f"Error resolving DOI {doi}: {e}", exc_info=True)
            return None

        doc_id = await self._extract_doc_id_from_url(redirect_url_str)
        if not doc_id:
            logger.warning(
                f"Could not extract valid IEEE doc ID for DOI {doi} from URL: {redirect_url_str}"
            )
            return None

        return await self.fetch_pdf_doc_id(doc_id, session)

    async def fetch_pdf_doc_id(
        self, doc_id: str, session: aiohttp.ClientSession
    ) -> Optional[aiohttp.ClientResponse]:
        """
        Fetch the PDF directly using the IEEE document ID.

        Args:
            doc_id: The IEEE document ID.
            session: An active aiohttp.ClientSession.

        Returns:
            An aiohttp.ClientResponse containing the PDF stream if successful,
            None otherwise. The caller is responsible for releasing this response.
        """
        pdf_url = self._get_pdf_url(doc_id)
        logger.info(f"Fetching PDF using document ID {doc_id} from URL: {pdf_url}")
        if self.proxy_url:
            logger.debug(f"Using proxy: {self.proxy_url}")

        response: Optional[aiohttp.ClientResponse] = None

        if "capes" in self.hostname:
            warm_urls = [
                "https://ez27.periodicos.capes.gov.br/login?url=" + pdf_url,
                "https://www.periodicos.capes.gov.br/?option=com_pezproxy&controller=auth&view=pezproxyauth&url="
                + pdf_url,
                "https://ez27.periodicos.capes.gov.br/connect?qurl=" + pdf_url,
            ]
            try:
                for warm_url in warm_urls:
                    warm_response = await session.get(
                        url=warm_url,
                        proxy=self.proxy_url,
                        timeout=self.timeout,
                        allow_redirects=True,
                    )
                    await warm_response.release()
            except Exception as e:
                logger.warning(f"Error warming CAPES credentials: {e}", exc_info=True)
        try:
            response = await session.get(
                url=pdf_url,
                proxy=self.proxy_url,
                timeout=self.timeout,
                allow_redirects=True,
            )
            if response.status == 200:
                content_type = response.headers.get("Content-Type", "").lower()
                if "application/pdf" in content_type:
                    logger.info(
                        f"Successfully initiated PDF download for doc ID {doc_id}"
                    )
                    return response  # Caller must release
                else:
                    logger.warning(
                        f"Expected PDF, received Content-Type '{content_type}' for doc ID {doc_id}"
                    )
                    await response.release()
                    return None
            else:
                logger.warning(
                    f"Failed fetch for doc ID {doc_id}. Status: {response.status}"
                )
                await response.release()
                return None

        except asyncio.TimeoutError:

            logger.error(
                f"Timeout ({self.timeout.total}s) fetching PDF for doc ID {doc_id}"
            )
            if response:
                await response.release()
            return None
        except aiohttp.ClientError as e:
            logger.error(
                f"HTTP client error fetching PDF for doc ID {doc_id}: {e}",
                exc_info=True,
            )
            if response:
                await response.release()
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error fetching PDF for doc ID {doc_id}: {e}", exc_info=True
            )
            if response:
                await response.release()
            return None
