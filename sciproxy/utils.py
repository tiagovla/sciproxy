import aiohttp


async def get_redirect_url(doi: str, session: aiohttp.ClientSession) -> str:
    """Get the redirect URL for the given DOI."""
    url = f"https://doi.org/{doi}"
    async with session.head(url, allow_redirects=True) as response:
        return str(response.url)
