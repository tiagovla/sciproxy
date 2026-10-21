import asyncio
import os
import time

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from sciproxy.client import SciProxy


@pytest.mark.asyncio
async def test_download_queue_limits_active_and_waiting_requests(tmp_path):
    proxy = SciProxy(
        [],
        cache_dir=str(tmp_path / "cache"),
        max_concurrent_downloads=1,
        max_queued_downloads=1,
    )
    first_download_started = asyncio.Event()
    allow_downloads_to_finish = asyncio.Event()
    active_downloads = 0
    peak_active_downloads = 0

    async def fake_fetch(
        request,
        doi,
        document_id,
        progress,
        cache_path,
        target_label,
        is_ieee_request,
    ):
        nonlocal active_downloads, peak_active_downloads
        active_downloads += 1
        peak_active_downloads = max(peak_active_downloads, active_downloads)
        first_download_started.set()
        try:
            await allow_downloads_to_finish.wait()
            return web.Response(body=b"pdf", content_type="application/pdf")
        finally:
            active_downloads -= 1

    proxy._fetch_uncached_pdf = fake_fetch

    def request_for(doi):
        return make_mocked_request("GET", f"/{doi}", match_info={"doi": doi})

    try:
        first_request = asyncio.create_task(
            proxy.handle_request(request_for("10.1000/first"))
        )
        await asyncio.wait_for(first_download_started.wait(), timeout=1)
        second_request = asyncio.create_task(
            proxy.handle_request(request_for("10.1000/second"))
        )
        for _ in range(100):
            if proxy._admitted_downloads == 2:
                break
            await asyncio.sleep(0.01)
        assert proxy._admitted_downloads == 2

        rejected = await proxy.handle_request(request_for("10.1000/third"))
        assert rejected.status == 503
        assert rejected.headers["Retry-After"] == "5"

        allow_downloads_to_finish.set()
        first_response, second_response = await asyncio.gather(
            first_request, second_request
        )
        assert first_response.status == 200
        assert second_response.status == 200
        assert peak_active_downloads == 1
    finally:
        allow_downloads_to_finish.set()


def test_cache_evicts_least_recently_used_pdf_to_stay_under_limit(tmp_path):
    cache_dir = tmp_path / "cache"
    proxy = SciProxy([], cache_dir=str(cache_dir), max_cache_bytes=6)
    old_cache_path = proxy._cache_path("10.1000/old")
    new_cache_path = proxy._cache_path("10.1000/new")
    incoming_path = tmp_path / "incoming.pdf"

    old_cache_path.write_bytes(b"old")
    new_cache_path.write_bytes(b"new")
    incoming_path.write_bytes(b"in")
    old_time = time.time() - 60
    os.utime(old_cache_path, (old_time, old_time))

    incoming_cache_path = proxy._cache_path("10.1000/incoming")
    stored = proxy._store_cached_pdf_sync(str(incoming_path), incoming_cache_path)

    assert stored is True
    assert not old_cache_path.exists()
    assert new_cache_path.exists()
    assert incoming_cache_path.exists()
    assert sum(path.stat().st_size for path in cache_dir.glob("*.pdf")) <= 6


def test_cache_does_not_store_pdf_larger_than_cache_limit(tmp_path):
    cache_dir = tmp_path / "cache"
    proxy = SciProxy([], cache_dir=str(cache_dir), max_cache_bytes=3)
    incoming_path = tmp_path / "large.pdf"
    incoming_path.write_bytes(b"large")

    stored = proxy._store_cached_pdf_sync(
        str(incoming_path), proxy._cache_path("10.1000/large")
    )

    assert stored is False
    assert incoming_path.exists()
    assert list(cache_dir.glob("*.pdf")) == []
