import asyncio
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Optional
from urllib.parse import quote

import aiofiles
import aiohttp
import pikepdf
from aiohttp import web

from sciproxy.downloaders.abc import Downloader, DownloaderUnavailable

logger = logging.getLogger(__name__)
DOI_PATTERN = re.compile(r"^10\.\d{4,9}/\S+$")
MAX_DOI_LENGTH = 2048
DEFAULT_MAX_PDF_BYTES = 100 * 1024 * 1024
DEFAULT_MAX_CACHE_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_MAX_CONCURRENT_DOWNLOADS = 4
DEFAULT_MAX_QUEUED_DOWNLOADS = 16
CHUNK_SIZE = 64 * 1024
USER_AGENT = "SciProxy/0.1"


def valid_doi(doi: str) -> bool:
    return (
        len(doi) <= MAX_DOI_LENGTH
        and DOI_PATTERN.fullmatch(doi) is not None
        and not any(ord(char) < 32 or ord(char) == 127 for char in doi)
    )


def _validate_and_save_pdf(source: str, destination: str) -> None:
    """Validate the complete download and save a linearized PDF."""
    with pikepdf.open(source) as pdf:
        pdf.save(destination, linearize=True)


class DownloadProgress:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.condition = asyncio.Condition()
        self.finished = False
        self.cancelled = False
        self.task: Optional[asyncio.Task] = None
        self.updated_at = time.monotonic()

    async def publish(self, event: dict[str, object]) -> None:
        async with self.condition:
            if self.finished:
                return
            self.events.append(event)
            self.updated_at = time.monotonic()
            self.condition.notify_all()

    async def finish(self, event: dict[str, object]) -> None:
        async with self.condition:
            if self.finished:
                return
            self.events.append(event)
            self.finished = True
            self.updated_at = time.monotonic()
            self.condition.notify_all()


class SciProxy:
    def __init__(
        self,
        downloaders: list[Downloader],
        cache_dir: Optional[str] = None,
        max_pdf_bytes: int = DEFAULT_MAX_PDF_BYTES,
        unpaywall_email: Optional[str] = None,
        max_cache_bytes: int = DEFAULT_MAX_CACHE_BYTES,
        max_concurrent_downloads: int = DEFAULT_MAX_CONCURRENT_DOWNLOADS,
        max_queued_downloads: int = DEFAULT_MAX_QUEUED_DOWNLOADS,
    ):
        if max_pdf_bytes <= 0:
            raise ValueError("max_pdf_bytes must be positive")
        if max_cache_bytes < 0:
            raise ValueError("max_cache_bytes cannot be negative")
        if max_concurrent_downloads <= 0:
            raise ValueError("max_concurrent_downloads must be positive")
        if max_queued_downloads < 0:
            raise ValueError("max_queued_downloads cannot be negative")
        self.strategies = downloaders
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.max_pdf_bytes = max_pdf_bytes
        self.max_cache_bytes = max_cache_bytes
        self.max_concurrent_downloads = max_concurrent_downloads
        self.max_queued_downloads = max_queued_downloads
        self.unpaywall_email = unpaywall_email
        self._download_progress: dict[str, DownloadProgress] = {}
        self._cache_lock = asyncio.Lock()
        self._download_slots = asyncio.Semaphore(max_concurrent_downloads)
        self._queue_lock = asyncio.Lock()
        self._admitted_downloads = 0
        self._target_locks: dict[str, tuple[asyncio.Lock, int]] = {}
        self._target_locks_guard = asyncio.Lock()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    async def _on_startup(self, app: web.Application) -> None:
        if self.cache_dir is None:
            return
        async with self._cache_lock:
            self._trim_cache_sync()

    def _trim_cache_sync(self) -> None:
        if self.cache_dir is None:
            return
        files: list[tuple[int, Path, int]] = []
        total_size = 0
        for path in self.cache_dir.glob("*.pdf"):
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue
            files.append((stat.st_mtime_ns, path, stat.st_size))
            total_size += stat.st_size

        for _, path, size in sorted(files, key=lambda item: item[0]):
            if total_size <= self.max_cache_bytes:
                break
            try:
                path.unlink()
                total_size -= size
                logger.info("Evicted cached PDF %s to enforce cache size limit", path)
            except FileNotFoundError:
                total_size -= size

    def _store_cached_pdf_sync(self, source: str, destination: Path) -> bool:
        if self.cache_dir is None:
            return False
        incoming_size = os.path.getsize(source)
        if incoming_size > self.max_cache_bytes:
            return False

        files: list[tuple[int, Path, int]] = []
        total_size = 0
        for path in self.cache_dir.glob("*.pdf"):
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue
            if path == destination:
                continue
            files.append((stat.st_mtime_ns, path, stat.st_size))
            total_size += stat.st_size

        for _, path, size in sorted(files, key=lambda item: item[0]):
            if total_size + incoming_size <= self.max_cache_bytes:
                break
            try:
                path.unlink()
                total_size -= size
                logger.info("Evicted cached PDF %s to make room", path)
            except FileNotFoundError:
                total_size -= size

        if total_size + incoming_size > self.max_cache_bytes:
            return False
        os.replace(source, destination)
        return True

    async def _store_cached_pdf(
        self, source: str, destination: Path
    ) -> Optional[tuple[object, int]]:
        if self.cache_dir is None or self.max_cache_bytes == 0:
            return None
        async with self._cache_lock:
            content_length = os.path.getsize(source)
            source_file = await aiofiles.open(source, "rb")
            try:
                stored = self._store_cached_pdf_sync(source, destination)
            except Exception:
                await source_file.close()
                raise
            if not stored:
                await source_file.close()
                return None
            return source_file, content_length

    async def _admit_download(self) -> bool:
        async with self._queue_lock:
            capacity = self.max_concurrent_downloads + self.max_queued_downloads
            if self._admitted_downloads >= capacity:
                return False
            self._admitted_downloads += 1
            return True

    async def _release_download_admission(self) -> None:
        async with self._queue_lock:
            self._admitted_downloads -= 1

    @asynccontextmanager
    async def _target_download_lock(self, key: str) -> AsyncIterator[bool]:
        async with self._target_locks_guard:
            entry = self._target_locks.get(key)
            lock = entry[0] if entry else asyncio.Lock()
            users = entry[1] if entry else 0
            self._target_locks[key] = (lock, users + 1)

        waited = lock.locked()
        acquired = False
        try:
            await lock.acquire()
            acquired = True
            yield waited
        finally:
            if acquired:
                lock.release()
            async with self._target_locks_guard:
                current = self._target_locks.get(key)
                if current and current[0] is lock:
                    users = current[1] - 1
                    if users == 0:
                        self._target_locks.pop(key, None)
                    else:
                        self._target_locks[key] = (lock, users)

    def _get_download_progress(self, job_id: str) -> Optional[DownloadProgress]:
        if not re.fullmatch(r"[a-f0-9]{32}", job_id):
            return None
        now = time.monotonic()
        for expired_id, progress in list(self._download_progress.items()):
            if now - progress.updated_at > 300:
                self._download_progress.pop(expired_id, None)
        progress = self._download_progress.get(job_id)
        if progress is None:
            progress = DownloadProgress()
            self._download_progress[job_id] = progress
        return progress

    def _cache_path(self, doi: str) -> Optional[Path]:
        if self.cache_dir is None:
            return None
        digest = hashlib.sha256(doi.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.pdf"

    @staticmethod
    def _pdf_url(request: web.Request, doi: str) -> str:
        return str(request.url.origin().with_path(f"/{doi}"))

    async def _download_pdf(
        self,
        response: aiohttp.ClientResponse,
        path: str,
        progress: Optional[DownloadProgress] = None,
        source: str = "source",
    ) -> None:
        size = 0
        total = response.content_length
        last_reported = 0
        async with aiofiles.open(path, "wb") as output:
            async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                size += len(chunk)
                if size > self.max_pdf_bytes:
                    raise ValueError("PDF exceeds configured size limit")
                await output.write(chunk)
                report_interval = (
                    CHUNK_SIZE * 4
                    if total is None
                    else max(CHUNK_SIZE * 4, total // 100)
                )
                if progress and (
                    size - last_reported >= report_interval or size == total
                ):
                    event: dict[str, object] = {
                        "phase": "downloading",
                        "message": f"Downloading PDF from {source}…",
                        "downloaded": size,
                    }
                    if total is not None:
                        event["total"] = total
                    await progress.publish(event)
                    last_reported = size
        if size == 0:
            raise ValueError("Empty PDF response")

    @staticmethod
    async def _validate_pdf(source: str, destination: str) -> None:
        task = asyncio.create_task(
            asyncio.to_thread(_validate_and_save_pdf, source, destination)
        )
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await asyncio.shield(task)
            except Exception:
                pass
            raise

    async def _stream_open_file(
        self,
        request: web.Request,
        source,
        content_length: Optional[int] = None,
        remove_path: Optional[str] = None,
    ) -> web.StreamResponse:
        headers = {"Content-Type": "application/pdf"}
        if content_length is not None:
            headers["Content-Length"] = str(content_length)
        response = web.StreamResponse(headers=headers)
        try:
            await response.prepare(request)
            while chunk := await source.read(CHUNK_SIZE):
                await response.write(chunk)
            await response.write_eof()
            return response
        except ConnectionResetError:
            return response
        finally:
            await source.close()
            if remove_path:
                Path(remove_path).unlink(missing_ok=True)

    async def _stream_temporary(
        self, request: web.Request, path: str
    ) -> web.StreamResponse:
        source = await aiofiles.open(path, "rb")
        return await self._stream_open_file(request, source, remove_path=path)

    async def _stream_cached_pdf(
        self, request: web.Request, path: Optional[Path]
    ) -> Optional[web.StreamResponse]:
        if path is None or self.max_cache_bytes == 0:
            return None
        async with self._cache_lock:
            if not path.is_file():
                return None
            try:
                stat = path.stat()
                try:
                    os.utime(path, None)
                except OSError:
                    logger.debug("Could not update cache recency for %s", path)
                source = await aiofiles.open(path, "rb")
            except FileNotFoundError:
                return None
            except OSError:
                logger.exception("Could not open cached PDF %s", path)
                return None
        return await self._stream_open_file(
            request, source, content_length=stat.st_size
        )

    async def handle_request(self, request: web.Request) -> web.StreamResponse:
        doi = request.match_info.get("doi", "")
        document_id = request.match_info.get("document_id")
        target_label = (
            f"IEEE document {document_id}" if document_id is not None else doi
        )
        progress_id = request.query.get("progress")
        progress = self._get_download_progress(progress_id) if progress_id else None
        if progress_id and progress is None:
            return web.Response(status=400, text="Invalid progress ID")
        if progress and progress.cancelled:
            await progress.finish(
                {"phase": "cancelled", "message": "Download cancelled."}
            )
            return web.json_response(
                {"error": "cancelled", "message": "Download cancelled."},
                status=499,
            )
        if progress:
            progress.task = asyncio.current_task()
        try:
            return await self._handle_download_request(
                request, doi, document_id, progress
            )
        except asyncio.CancelledError:
            if not progress or not progress.cancelled:
                raise
            await progress.finish(
                {"phase": "cancelled", "message": "Download cancelled."}
            )
            return web.json_response(
                {"error": "cancelled", "message": "Download cancelled."},
                status=499,
            )
        except Exception:
            logger.exception("Download request failed for %s", target_label)
            if progress:
                await progress.finish(
                    {
                        "phase": "failed",
                        "error": "server_error",
                        "message": "The server could not prepare the PDF.",
                    }
                )
                return web.json_response(
                    {
                        "error": "server_error",
                        "message": "The server could not prepare the PDF.",
                    },
                    status=500,
                )
            return web.Response(status=500, text="Could not prepare PDF")
        finally:
            if progress and progress.task is asyncio.current_task():
                progress.task = None

    async def _handle_download_request(
        self,
        request: web.Request,
        doi: str,
        document_id: Optional[str],
        progress: Optional[DownloadProgress],
    ) -> web.StreamResponse:
        is_ieee_request = document_id is not None
        if is_ieee_request:
            valid_target = re.fullmatch(r"\d+", document_id) is not None
            cache_key = f"ieee:{document_id}"
            target_label = f"IEEE document {document_id}"
            pdf_path = f"/ieee/document/{document_id}"
        else:
            valid_target = valid_doi(doi)
            cache_key = doi
            target_label = doi
            pdf_path = f"/{doi}"

        if not valid_target:
            if progress:
                await progress.finish(
                    {
                        "phase": "failed",
                        "error": "invalid_input",
                        "message": "Enter a valid DOI or IEEE document ID.",
                    }
                )
                return web.json_response(
                    {
                        "error": "invalid_input",
                        "message": "Enter a valid DOI or IEEE document ID.",
                    },
                    status=400,
                )
            return await self.handle_not_found()
        if "api" in request.query:
            return web.json_response(
                {"url_for_pdf": str(request.url.origin().with_path(pdf_path))}
            )

        cache_path = self._cache_path(cache_key)
        use_cache = "nocache" not in request.query
        if not await self._admit_download():
            message = "The download queue is full. Please try again shortly."
            error = {"error": "server_busy", "message": message}
            if progress:
                await progress.finish(
                    {"phase": "failed", "error": "server_busy", "message": message}
                )
            return web.json_response(
                error, status=503, headers={"Retry-After": "5"}
            )

        try:
            if use_cache:
                if self._download_slots.locked() and progress:
                    await progress.publish(
                        {
                            "phase": "queued",
                            "message": "Waiting for an available download slot…",
                        }
                    )
                async with self._download_slots:
                    cached_response = await self._stream_cached_pdf(
                        request, cache_path
                    )
                    if cached_response is not None:
                        if progress:
                            await progress.finish(
                                {
                                    "phase": "ready",
                                    "message": "Cached PDF ready. Sending it to your browser…",
                                }
                            )
                        return cached_response

            async with self._target_download_lock(cache_key) as waited_for_target:
                if waited_for_target and progress:
                    await progress.publish(
                        {
                            "phase": "queued",
                            "message": "This PDF is already being prepared. Waiting for it…",
                        }
                    )
                if self._download_slots.locked() and progress:
                    await progress.publish(
                        {
                            "phase": "queued",
                            "message": "Waiting for an available download slot…",
                        }
                    )
                async with self._download_slots:
                    if use_cache:
                        cached_response = await self._stream_cached_pdf(
                            request, cache_path
                        )
                        if cached_response is not None:
                            if progress:
                                await progress.finish(
                                    {
                                        "phase": "ready",
                                        "message": "Cached PDF ready. Sending it to your browser…",
                                    }
                                )
                            return cached_response
                    return await self._fetch_uncached_pdf(
                        request,
                        doi,
                        document_id,
                        progress,
                        cache_path,
                        target_label,
                        is_ieee_request,
                    )
        finally:
            await self._release_download_admission()

    async def _fetch_uncached_pdf(
        self,
        request: web.Request,
        doi: str,
        document_id: Optional[str],
        progress: Optional[DownloadProgress],
        cache_path: Optional[Path],
        target_label: str,
        is_ieee_request: bool,
    ) -> web.StreamResponse:
        timeout = aiohttp.ClientTimeout(total=120)
        source_timed_out = False
        source_unavailable = False
        async with aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT}, timeout=timeout
        ) as session:
            for strategy in self.strategies:
                pdf_response = None
                raw_path = None
                ready_path = None
                source_name = type(strategy).__name__.replace("Downloader", "")
                fetch_by_document_id = None
                if is_ieee_request:
                    fetch_by_document_id = getattr(
                        strategy, "fetch_pdf_doc_id", None
                    )
                    if not callable(fetch_by_document_id):
                        continue
                try:
                    if progress:
                        search_message = (
                            f"Searching {source_name} for the IEEE document…"
                            if is_ieee_request
                            else f"Resolving the DOI and searching {source_name}…"
                        )
                        await progress.publish(
                            {
                                "phase": "searching",
                                "message": search_message,
                            }
                        )
                    if is_ieee_request:
                        pdf_response = await fetch_by_document_id(
                            document_id, session
                        )
                    else:
                        pdf_response = await strategy.fetch_pdf(doi, session)
                    if pdf_response is None:
                        continue
                    if (
                        pdf_response.status != 200
                        or pdf_response.content_type != "application/pdf"
                    ):
                        logger.warning(
                            "Downloader %s returned a non-PDF response",
                            type(strategy).__name__,
                        )
                        continue
                    if (
                        pdf_response.content_length is not None
                        and pdf_response.content_length > self.max_pdf_bytes
                    ):
                        logger.warning(
                            "PDF from %s exceeds size limit", type(strategy).__name__
                        )
                        continue

                    work_dir = (
                        str(self.cache_dir.parent) if self.cache_dir else None
                    )
                    raw_fd, raw_path = tempfile.mkstemp(
                        suffix=".download", dir=work_dir
                    )
                    os.close(raw_fd)
                    if progress:
                        await progress.publish(
                            {
                                "phase": "downloading",
                                "message": f"Downloading PDF from {source_name}…",
                            }
                        )
                    await self._download_pdf(
                        pdf_response,
                        raw_path,
                        progress=progress,
                        source=source_name,
                    )

                    if progress:
                        await progress.publish(
                            {
                                "phase": "preparing",
                                "message": "Validating and preparing the PDF…",
                            }
                        )
                    ready_fd, ready_path = tempfile.mkstemp(
                        suffix=".pdf", dir=work_dir
                    )
                    os.close(ready_fd)
                    await self._validate_pdf(raw_path, ready_path)
                    if os.path.getsize(ready_path) > self.max_pdf_bytes:
                        raise ValueError("Processed PDF exceeds configured size limit")
                    cached_file = None
                    if cache_path and self.max_cache_bytes > 0:
                        try:
                            cached_file = await self._store_cached_pdf(
                                ready_path, cache_path
                            )
                        except OSError:
                            logger.exception(
                                "Could not cache the PDF for %s", target_label
                            )
                    if cached_file is not None:
                        ready_path = None
                        if progress:
                            await progress.finish(
                                {
                                    "phase": "ready",
                                    "message": "PDF ready. Sending it to your browser…",
                                }
                            )
                        source, content_length = cached_file
                        return await self._stream_open_file(
                            request, source, content_length=content_length
                        )
                    if progress:
                        await progress.finish(
                            {
                                "phase": "ready",
                                "message": "PDF ready. Sending it to your browser…",
                            }
                        )
                    result = await self._stream_temporary(request, ready_path)
                    ready_path = None
                    return result
                except asyncio.TimeoutError as error:
                    source_timed_out = True
                    logger.warning(
                        "Downloader %s timed out for %s: %s",
                        type(strategy).__name__,
                        target_label,
                        error,
                    )
                except (DownloaderUnavailable, aiohttp.ClientError) as error:
                    source_unavailable = True
                    logger.warning(
                        "Downloader %s was unavailable for %s: %s",
                        type(strategy).__name__,
                        target_label,
                        error,
                    )
                except (pikepdf.PdfError, ValueError) as error:
                    logger.warning(
                        "Downloader %s failed for %s: %s",
                        type(strategy).__name__,
                        target_label,
                        error,
                    )
                except OSError:
                    logger.exception("Could not store PDF for %s", target_label)
                    if progress:
                        await progress.finish(
                            {
                                "phase": "failed",
                                "error": "storage_error",
                                "message": "The server could not store the PDF.",
                            }
                        )
                        return web.json_response(
                            {
                                "error": "storage_error",
                                "message": "The server could not store the PDF.",
                            },
                            status=500,
                        )
                    return web.Response(status=500, text="Could not store PDF")
                finally:
                    if pdf_response is not None:
                        pdf_response.release()
                    if raw_path:
                        Path(raw_path).unlink(missing_ok=True)
                    if ready_path:
                        Path(ready_path).unlink(missing_ok=True)
        if source_timed_out:
            error = {
                "error": "source_timeout",
                "message": "A download source timed out. Please try again.",
            }
            status_code = 504
        elif source_unavailable:
            error = {
                "error": "source_unavailable",
                "message": "A download source could not be reached. Please try again.",
            }
            status_code = 502
        else:
            error = {
                "error": "not_found",
                "message": "No PDF was found at the available sources.",
            }
            status_code = 404

        if progress:
            await progress.finish(
                {
                    "phase": "failed",
                    "error": error["error"],
                    "message": error["message"],
                }
            )
            return web.json_response(error, status=status_code)
        if status_code == 404:
            return await self.handle_not_found()
        return web.json_response(error, status=status_code)

    async def handle_index(self, request: web.Request) -> web.Response:
        return web.FileResponse(Path(__file__).parent / "static" / "index.html")

    async def handle_progress(self, request: web.Request) -> web.StreamResponse:
        job_id = request.match_info.get("job_id", "")
        progress = self._get_download_progress(job_id)
        if progress is None:
            return web.Response(status=400, text="Invalid progress ID")

        response = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            }
        )
        await response.prepare(request)
        cursor = 0
        try:
            while True:
                async with progress.condition:
                    try:
                        await asyncio.wait_for(
                            progress.condition.wait_for(
                                lambda: cursor < len(progress.events)
                                or progress.finished
                            ),
                            timeout=15,
                        )
                    except asyncio.TimeoutError:
                        pass
                    events = progress.events[cursor:]
                    cursor += len(events)
                    finished = progress.finished and cursor == len(progress.events)

                for event in events:
                    payload = json.dumps(event, separators=(",", ":"))
                    await response.write(f"data: {payload}\n\n".encode("utf-8"))

                if finished:
                    break
                if not events:
                    await response.write(b": keep-alive\n\n")
            await response.write_eof()
        except ConnectionResetError:
            pass
        return response

    async def handle_cancel_progress(self, request: web.Request) -> web.Response:
        job_id = request.match_info.get("job_id", "")
        progress = self._get_download_progress(job_id)
        if progress is None:
            return web.json_response(
                {"error": "invalid_progress_id"}, status=400
            )
        if progress.finished:
            return web.json_response({"cancelled": False, "finished": True})

        progress.cancelled = True
        task = progress.task
        if task and task is not asyncio.current_task():
            task.cancel()
        else:
            await progress.finish(
                {"phase": "cancelled", "message": "Download cancelled."}
            )
        return web.json_response({"cancelled": True}, status=202)

    async def handle_api(self, request: web.Request) -> web.Response:
        doi = request.match_info.get("doi", "")
        if not valid_doi(doi):
            return web.json_response({"error": "Invalid DOI"}, status=400)
        if not self.unpaywall_email:
            return web.json_response(
                {"error": "UNPAYWALL_EMAIL is not configured"}, status=503
            )
        url = f"https://api.unpaywall.org/v2/{quote(doi, safe='/')}"
        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    url, params={"email": self.unpaywall_email}
                ) as upstream:
                    if upstream.status != 200:
                        return web.json_response(
                            {"error": "Unpaywall request failed"}, status=upstream.status
                        )
                    data = await upstream.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            logger.exception("Unpaywall request failed for %s", doi)
            return web.json_response({"error": "Unpaywall unavailable"}, status=502)

        if not isinstance(data, dict):
            return web.json_response({"error": "Invalid Unpaywall response"}, status=502)
        locations = data.get("oa_locations") or []
        if not isinstance(locations, list):
            return web.json_response({"error": "Invalid Unpaywall response"}, status=502)
        data["oa_locations"] = [*locations, {"url_for_pdf": self._pdf_url(request, doi)}]
        return web.json_response(data)

    async def handle_not_found(self) -> web.Response:
        return web.FileResponse(
            Path(__file__).parent / "static" / "not_found.html", status=404
        )

    def create_app(self) -> web.Application:
        app = web.Application()
        app.on_startup.append(self._on_startup)
        app.router.add_static("/static", Path(__file__).parent / "static")
        app.router.add_get("/", self.handle_index)
        app.router.add_get("/favicon.ico", lambda _: web.Response(status=404))
        app.router.add_get("/progress/{job_id}", self.handle_progress)
        app.router.add_post(
            "/progress/{job_id}/cancel", self.handle_cancel_progress
        )
        app.router.add_get("/api/{doi:.*}", self.handle_api)
        app.router.add_get(
            "/ieee/document/{document_id:\\d+}", self.handle_request
        )
        app.router.add_get("/{doi:.*}", self.handle_request)
        return app

    def run(self, host: str, port: int) -> None:
        logger.info("Starting SciProxy server on %s:%s", host, port)
        web.run_app(self.create_app(), host=host, port=port)
