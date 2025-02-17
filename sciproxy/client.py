import aiohttp
from sciproxy.downloaders.abc import Downloader
from aiohttp import web
from typing import Optional, List
import os
import logging


logger = logging.getLogger(__name__)


class SciProxy:
    def __init__(self, downloaders: List[Downloader], cache_dir: Optional[str] = None):
        self.strategies = downloaders
        self.cache_dir = cache_dir
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)

    async def handle_request(self, request: web.Request) -> web.Response:
        """Handle the incoming request to fetch a PDF by DOI."""
        try:
            doi = request.match_info.get("doi", "")
            sanitized_doi = doi.replace("/", "_")
            cache_path = (
                os.path.join(self.cache_dir, f"{sanitized_doi}.pdf")
                if self.cache_dir
                else None
            )

            if cache_path and os.path.exists(cache_path):
                with open(cache_path, "rb") as f:
                    pdf = f.read()
                return web.Response(body=pdf, content_type="application/pdf")

            async with aiohttp.ClientSession() as session:
                for strategy in self.strategies:
                    pdf = await strategy.fetch_pdf(doi, session)
                    if pdf:
                        if cache_path:
                            with open(cache_path, "wb") as f:
                                f.write(pdf)
                        return web.Response(body=pdf, content_type="application/pdf")
                raise Exception("Invalid DOI URL")
        except Exception as e:
            logger.error(f"Error fetching PDF: {e}")
            return web.Response(status=500, text=f"Error fetching PDF: {e}")

    def run(self, host: str, port: int):
        logger.info("Starting SciProxy server on {host}:{port}")
        app = web.Application()
        app.router.add_get("/favicon.ico", lambda _: web.Response(status=404))
        app.router.add_get("/{doi:.*}", self.handle_request)
        web.run_app(app, host=host, port=port)
