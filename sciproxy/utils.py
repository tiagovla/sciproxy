import aiohttp
from urllib.parse import quote

from sciproxy.downloaders.abc import DownloaderUnavailable


async def get_redirect_url(doi: str, session: aiohttp.ClientSession) -> str:
    """Get the redirect URL for the given DOI."""
    url = f"https://doi.org/{quote(doi, safe='/')}"
    async with session.head(url, allow_redirects=True) as response:
        if response.status in (401, 403, 429) or response.status >= 500:
            raise DownloaderUnavailable(
                f"DOI resolver returned HTTP {response.status}"
            )
        if response.status >= 400:
            return ""
        return str(response.url)
