import aiohttp
import aiofiles
from sciproxy.downloaders.abc import Downloader
from aiohttp import web
from typing import Optional, List
import os
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
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
            api = request.query.get("api", None) is not None
            nocache = request.query.get("nocache", None) is not None

            if api:
                pdf_url = f"{request.url.scheme}://{request.url.host}/{doi}"
                logger.info(f"API request for pdf_url: {pdf_url}")
                return web.json_response({"url_for_pdf": pdf_url})

            if not doi.startswith("10."):
                return await self.handle_not_found()
            sanitized_doi = doi.replace("/", "_")
            cache_path = (
                os.path.join(self.cache_dir, f"{sanitized_doi}.pdf")
                if self.cache_dir
                else None
            )

            if nocache:
                logger.info("Skipping cache")
            elif cache_path and await self._file_exists(cache_path):
                pdf = await self._read_file(cache_path)
                return web.Response(body=pdf, content_type="application/pdf")

            async with aiohttp.ClientSession() as session:
                for strategy in self.strategies:
                    pdf_response = await strategy.fetch_pdf(doi, session)
                    if pdf_response:

                        stream_response = web.StreamResponse(
                            status=200,
                            headers={"Content-Type": "application/pdf"},
                        )

                        pdf_data = bytearray()
                        await stream_response.prepare(request)

                        async for chunk in pdf_response.content.iter_chunked(8 * 1024):
                            await stream_response.write(chunk)
                            pdf_data.extend(chunk)

                        await stream_response.write_eof()

                        if cache_path:
                            await self._write_file(cache_path, pdf_data)
                        return stream_response
                return await self.handle_not_found()
        except Exception as e:
            logger.error(f"Error fetching PDF: {e}")
            return web.Response(status=500, text=f"Error fetching PDF: {e}")

    async def handle_index(self, request: web.Request) -> web.Response:
        logger.info("Index page requested")
        index_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
        return web.FileResponse(index_path)

    async def handle_api(self, request: web.Request) -> web.Response:
        doi = request.match_info.get("doi", "")
        url = f"https://api.unpaywall.org/v2/{doi}?email=test@mail.com"
        logger.info(
            f"API request for pdf_url: {f"{request.url.scheme}://{request.url.host}/{doi}"}"
        )
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    resp_json = await response.json()
                    resp_json = resp_json | {
                        "oa_locations": [
                            {
                                "url_for_pdf": f"{request.url.scheme}://{request.url.host}/{doi}"
                            }
                        ]
                    }
                    return web.json_response(resp_json)
        except Exception as e:
            return web.json_response({"error": str(e)})

    async def handle_not_found(self) -> web.Response:
        logger.info("Not found page request.")
        index_path = os.path.join(os.path.dirname(__file__), "static", "not_found.html")
        return web.FileResponse(index_path)

    async def _file_exists(self, path: str) -> bool:
        return os.path.exists(path)

    async def _read_file(self, path: str) -> bytes:
        async with aiofiles.open(path, "rb") as f:
            return await f.read()

    async def _write_file(self, path: str, data: bytes):
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)

    def run(self, host: str, port: int):
        logger.info(f"Starting SciProxy server on {host}:{port}")
        app = web.Application()
        app.router.add_static(
            "/static", os.path.join(os.path.dirname(__file__), "static")
        )
        app.router.add_get("/", self.handle_index)
        app.router.add_get("/favicon.ico", lambda _: web.Response(status=404))
        app.router.add_get("/{doi:.*}", self.handle_request)
        app.router.add_get("/api/{doi:.*}", self.handle_api)
        web.run_app(app, host=host, port=port)
