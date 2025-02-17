from sciproxy.downloaders.abc import Downloader
import aiohttp
import logging

from typing import Optional, Dict

logger = logging.getLogger(__name__)


class CAPESDownloader(Downloader):
    def __init__(
        self,
        proxy_url: Optional[str] = None,
        proxy_auth: Optional[Dict[str, str]] = None,
    ):
        self.proxy_url = proxy_url
        self.proxy_auth = proxy_auth

    async def fetch_pdf(
        self, doi: str, session: aiohttp.ClientSession) -> Optional[aiohttp.ClientResponse]:
        """Fetch the PDF for the given DOI from IEEE."""
        logger.info(f"Fetching PDF for DOI {doi} from IEEE (CAPES)")
        redirect_url = await self.get_redirect_url(doi, session)
        if "ieeexplore.ieee.org" in redirect_url:
            doc_id = redirect_url.split("/")[
                redirect_url.split("/").index("document") + 1
            ]
            logger.info(f"Redirect URL obtained: {redirect_url}, Document ID: {doc_id}")
            return await self.fetch_pdf_from_ieee(doc_id, session)
        logger.warning(
            f"Redirect URL does not contain 'ieeexplore.ieee.org': {redirect_url}"
        )
        return None

    async def get_redirect_url(self, doi: str, session: aiohttp.ClientSession) -> str:
        """Get the redirect URL for the given DOI."""
        async with session.head(
            f"https://doi.org/{doi}", allow_redirects=True
        ) as response:
            return str(response.url)

    async def fetch_pdf_from_ieee(
        self, doc_id: str, session: aiohttp.ClientSession
    ) -> Optional[aiohttp.ClientResponse]:
        """Fetch the PDF from IEEE (CAPES) using the document ID."""
        url = f"https://ieeexplore-ieee-org.ez27.periodicos.capes.gov.br/stampPDF/getPDF.jsp?tp=&arnumber={doc_id}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"
        }
        proxy_auth = (
            aiohttp.BasicAuth(self.proxy_auth["username"], self.proxy_auth["password"])
            if self.proxy_auth
            else None
        )

        pdf_response = await session.get( url, proxy=self.proxy_url, proxy_auth=proxy_auth, headers=headers) 
        if pdf_response.status == 200:
            return pdf_response
        else:
            logger.warning(f"Failed to fetch PDF from IEEE (CAPES): {pdf_url}")
            return None
