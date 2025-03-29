# test_pdf_cache.py

import pytest
import pytest_asyncio
import os
import io
import time
import asyncio
import logging
import aiofiles

# Assuming PdfCache is in pdf_cache.py in the same directory
from sciproxy.cache import PdfCache, logger

# Configure logging for tests (optional, helps debugging)
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger.setLevel(logging.DEBUG)  # Ensure cache logger is also debug level for tests


# --- Test Data ---
DUMMY_PDF_CONTENT_1 = b"""%PDF-1.4
1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj
2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj
3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] >> endobj
xref
0 4
0000000000 65535 f
0000000009 00000 n
0000000060 00000 n
0000000124 00000 n
trailer << /Size 4 /Root 1 0 R >>
startxref
197
%%EOF"""

# Slightly different valid PDF (e.g., different MediaBox)
DUMMY_PDF_CONTENT_2 = b"""%PDF-1.4
1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj
2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj
3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 300 300] /Resources <<>> >> endobj
xref
0 4
0000000000 65535 f
0000000009 00000 n
0000000060 00000 n
0000000124 00000 n
trailer << /Size 4 /Root 1 0 R >>
startxref
218
%%EOF"""

# --- Fixtures ---


@pytest_asyncio.fixture
async def pdf_cache(tmp_path):
    """Provides a PdfCache instance initialized in a unique temp directory."""
    cache_dir = tmp_path / "test_cache"
    cache = PdfCache(cache_dir=str(cache_dir))
    # Ensure directory exists after init (though __init__ should handle it)
    assert os.path.exists(cache_dir)
    return cache


# --- Helper Functions ---


def _create_dummy_pdf(content: bytes = DUMMY_PDF_CONTENT_1) -> io.BytesIO:
    """Creates a BytesIO buffer with dummy PDF content."""
    return io.BytesIO(content)


async def _get_atime(file_path: str) -> float:
    """Gets the access time of a file."""
    stat_res = await aiofiles.os.stat(file_path)
    return stat_res.st_atime


async def _create_file(path: str, content: bytes = b""):
    """Helper to create a file asynchronously."""
    async with aiofiles.open(path, "wb") as f:
        await f.write(content)


# --- Test Class ---


@pytest.mark.asyncio
class TestPdfCache:

    async def test_init(self, tmp_path):
        """Test cache initialization and directory creation."""
        cache_dir = tmp_path / "new_cache"
        assert not os.path.exists(cache_dir)
        cache = PdfCache(cache_dir=str(cache_dir))
        assert cache.cache_dir == str(cache_dir)
        assert os.path.isdir(cache_dir)

    async def test_key_sanitization(self, pdf_cache: PdfCache):
        """Test if keys are correctly sanitized for filenames."""
        raw_key = "10.123/test:key"
        sanitized = pdf_cache._sanitize_key(raw_key)
        # '/' replaced by '@', ':' is not replaced by default in this version
        assert sanitized == "10.123@test:key"
        # Test unsanitize reverses it
        assert pdf_cache._unsanitize_filename(sanitized) == raw_key
        #
        # Test path generation uses sanitized key
        expected_filename = f"{sanitized}{pdf_cache.CACHE_SUFFIX}"
        path = pdf_cache._get_cache_path(raw_key)
        assert path.endswith(expected_filename)
        assert os.path.basename(path) == expected_filename

    async def test_put_exists_get_path_get_data(self, pdf_cache: PdfCache):
        """Test basic put, exists, get_path, and get_data flow."""
        raw_key = "10.1000/get_test"
        pdf_data = _create_dummy_pdf()

        # Initial state
        assert not await pdf_cache.exists(raw_key)
        assert await pdf_cache.get_path(raw_key) is None
        assert await pdf_cache.get_data(raw_key) is None

        # Put data
        await pdf_cache.put(raw_key, pdf_data)
        # Allow executor task to likely complete (though await ensures it waits)
        await asyncio.sleep(0.05)

        # Check existence
        assert await pdf_cache.exists(raw_key)
        #
        # Get path (first time)
        cache_path = await pdf_cache.get_path(raw_key)
        assert cache_path is not None
        assert os.path.exists(cache_path)
        assert pdf_cache._sanitize_key(raw_key) in cache_path
        atime1 = await _get_atime(cache_path)
        #
        # Get data
        retrieved_data = await pdf_cache.get_data(raw_key)
        assert retrieved_data is not None
        pdf_data.seek(0)  # Reset original buffer
        atime2 = await _get_atime(cache_path)  # get_data also touches
        # Note: Depending on OS timer resolution, atime might not visibly change if calls are too fast.
        # A small sleep helps ensure it's observably different. Allow for minor difference or equality.
        await asyncio.sleep(0.05)
        assert atime2 >= atime1  # Access time should be updated or same

        # Test non-existent key again
        assert not await pdf_cache.exists("nonexistent/key")
        assert await pdf_cache.get_path("nonexistent/key") is None
        assert await pdf_cache.get_data("nonexistent/key") is None

    async def test_put_overwrite(self, pdf_cache: PdfCache):
        """Test that putting the same key overwrites the file."""
        raw_key = "10.2000/overwrite"
        pdf_data1 = _create_dummy_pdf(DUMMY_PDF_CONTENT_1)
        pdf_data2 = _create_dummy_pdf(DUMMY_PDF_CONTENT_2)

        # Put first version
        await pdf_cache.put(raw_key, pdf_data1)
        await asyncio.sleep(0.05)

        # Verify first version
        retrieved1 = await pdf_cache.get_data(raw_key)
        assert retrieved1 is not None
        assert retrieved1.getvalue() == DUMMY_PDF_CONTENT_1

        # Put second version
        await pdf_cache.put(raw_key, pdf_data2)
        await asyncio.sleep(0.05)

        # Verify second version
        retrieved2 = await pdf_cache.get_data(raw_key)
        assert retrieved2 is not None
        assert retrieved2.getvalue() == DUMMY_PDF_CONTENT_2  # Should now have content 2

    #
    async def test_get_path_touch_effect(self, pdf_cache: PdfCache):
        """Test explicitly that get_path updates the access time."""
        raw_key = "10.3000/touch_test"
        await pdf_cache.put(raw_key, _create_dummy_pdf())
        await asyncio.sleep(0.05)  # Allow put to finish

        cache_path = await pdf_cache.get_path(raw_key)  # Initial get/touch
        assert cache_path is not None
        atime1 = await _get_atime(cache_path)

        await asyncio.sleep(0.2)  # Wait a bit

        cache_path_2 = await pdf_cache.get_path(raw_key)  # Second get/touch
        assert cache_path_2 == cache_path
        atime2 = await _get_atime(cache_path)

        assert atime2 > atime1  # Access time must have been updated

    async def test_get_data_touch_effect(self, pdf_cache: PdfCache):
        """Test explicitly that get_data updates the access time."""
        raw_key = "10.4000/touch_data_test"
        await pdf_cache.put(raw_key, _create_dummy_pdf())
        await asyncio.sleep(0.05)

        # Use get_path first to establish a baseline touch time if needed,
        # but directly getting data should also touch.
        initial_path = await pdf_cache.get_path(raw_key)
        assert initial_path is not None
        atime1 = await _get_atime(initial_path)

        await asyncio.sleep(0.2)  # Wait a bit

        retrieved_data = await pdf_cache.get_data(raw_key)  # This call should touch
        assert retrieved_data is not None
        atime2 = await _get_atime(initial_path)

        assert atime2 > atime1  # Access time must have been updated by get_data

    async def test_list_cached_keys(self, pdf_cache: PdfCache):
        """Test listing cached keys."""
        key1 = "10.5000/list_key1"
        key2 = "10.5000/another/list_key2"  # Key with multiple slashes

        # Empty cache
        assert await pdf_cache.list_cached_keys() == []

        # Add one key
        await pdf_cache.put(key1, _create_dummy_pdf())
        await asyncio.sleep(0.05)
        keys1 = await pdf_cache.list_cached_keys()
        assert sorted(keys1) == sorted([key1])

        # Add second key
        await pdf_cache.put(key2, _create_dummy_pdf(DUMMY_PDF_CONTENT_2))
        await asyncio.sleep(0.05)
        keys2 = await pdf_cache.list_cached_keys()
        assert sorted(keys2) == sorted(
            [key1, key2]
        )  # Ensure original keys are returned

    # --- Purge Tests ---

    async def test_purge_no_limits(self, pdf_cache: PdfCache):
        """Test purge with no limits does nothing."""
        key1 = "10.6000/purge_no_limit"
        await pdf_cache.put(key1, _create_dummy_pdf())
        await asyncio.sleep(0.05)

        purged_keys = await pdf_cache.purge()  # No args
        assert purged_keys == []
        assert await pdf_cache.exists(key1)  # File should still exist

    async def test_purge_by_age_only(self, pdf_cache: PdfCache):
        """Test purging based only on age."""
        key_old = "10.7000/purge_old"
        key_new = "10.7000/purge_new"

        await pdf_cache.put(key_old, _create_dummy_pdf())
        await asyncio.sleep(0.2)  # Make key_old significantly older
        await pdf_cache.put(key_new, _create_dummy_pdf())
        await asyncio.sleep(0.05)

        # Purge files older than 0.15 seconds
        max_age_days = 0.15 / pdf_cache.SECONDS_PER_DAY
        purged_keys = await pdf_cache.purge(max_age_days=max_age_days)

        assert sorted(purged_keys) == sorted([key_old])
        assert not await pdf_cache.exists(key_old)
        assert await pdf_cache.exists(key_new)

    async def test_purge_by_age_exact_cutoff(self, pdf_cache: PdfCache):
        """Test purging where age is very close to cutoff (should not purge)."""
        key_just_added = "10.7500/purge_cutoff"

        await pdf_cache.put(key_just_added, _create_dummy_pdf())
        await asyncio.sleep(0.05)  # Wait a short time

        # Purge files older than 0.1 seconds (key_just_added is younger)
        max_age_days = 0.1 / pdf_cache.SECONDS_PER_DAY
        purged_keys = await pdf_cache.purge(max_age_days=max_age_days)

        assert purged_keys == []  # Nothing should be purged
        assert await pdf_cache.exists(key_just_added)

    async def test_purge_by_size_only(self, pdf_cache: PdfCache):
        """Test purging based only on size limit."""
        key_lru = "10.8000/purge_lru"  # Least Recently Used
        key_mru = "10.8000/purge_mru"  # Most Recently Used

        pdf_data1 = _create_dummy_pdf(DUMMY_PDF_CONTENT_1)  # Smaller
        pdf_data2 = _create_dummy_pdf(DUMMY_PDF_CONTENT_2)  # Larger
        size1 = len(pdf_data1.getvalue())
        size2 = len(pdf_data2.getvalue())
        total_size = size1 + size2

        # Put LRU first
        await pdf_cache.put(key_lru, pdf_data1)
        await asyncio.sleep(0.2)  # Make it older

        # Put MRU later
        await pdf_cache.put(key_mru, pdf_data2)
        await asyncio.sleep(0.05)

        # Target size less than total, but more than MRU size
        target_size_bytes = (
            total_size - size1 + 10
        )  # Should require removing key_lru (size1)
        target_size_mbytes = target_size_bytes / pdf_cache.BYTES_PER_MBYTE

        purged_keys = await pdf_cache.purge(max_size_mbytes=target_size_mbytes)

        assert sorted(purged_keys) == sorted(
            [key_lru]
        )  # Oldest (LRU) should be purged first
        assert not await pdf_cache.exists(key_lru)
        assert await pdf_cache.exists(key_mru)

    async def test_purge_by_size_exact_limit(self, pdf_cache: PdfCache):
        """Test purge by size when current size equals limit."""
        key1 = "10.8500/purge_size_exact"
        pdf_data1 = _create_dummy_pdf(DUMMY_PDF_CONTENT_1)
        size1 = len(pdf_data1.getvalue())

        await pdf_cache.put(key1, pdf_data1)
        await asyncio.sleep(0.05)

        # Set limit exactly equal to current size
        target_size_mbytes = size1 / pdf_cache.BYTES_PER_MBYTE
        purged_keys = await pdf_cache.purge(max_size_mbytes=target_size_mbytes)

        assert purged_keys == []  # No purge needed
        assert await pdf_cache.exists(key1)

    async def test_purge_by_size_purge_all(self, pdf_cache: PdfCache):
        """Test purge by size targeting zero bytes."""
        key1 = "10.9000/purge_size_all1"
        key2 = "10.9000/purge_size_all2"
        await pdf_cache.put(key1, _create_dummy_pdf(DUMMY_PDF_CONTENT_1))
        await asyncio.sleep(0.1)
        await pdf_cache.put(key2, _create_dummy_pdf(DUMMY_PDF_CONTENT_2))
        await asyncio.sleep(0.05)

        purged_keys = await pdf_cache.purge(max_size_mbytes=0)  # Target 0 size

        assert sorted(purged_keys) == sorted([key1, key2])  # Both should be purged
        assert not await pdf_cache.exists(key1)
        assert not await pdf_cache.exists(key2)

    async def test_purge_combined_age_then_size(self, pdf_cache: PdfCache):
        """Test combined purge: age criteria applied first, then size."""
        key_old = "10.9500/purge_old_combined"
        key_mid_lru = "10.9500/purge_mid_combined_lru"  # Middle age, but less recently used for size
        key_new_mru = "10.9500/purge_new_combined_mru"  # Newest

        pdf_data1 = _create_dummy_pdf(DUMMY_PDF_CONTENT_1)
        pdf_data2 = _create_dummy_pdf(DUMMY_PDF_CONTENT_2)
        pdf_data3 = _create_dummy_pdf(
            DUMMY_PDF_CONTENT_1
        )  # Use same content for predictable size
        size1 = len(pdf_data1.getvalue())
        size2 = len(pdf_data2.getvalue())
        size3 = len(pdf_data3.getvalue())
        # total_size = size1 + size2 + size3 # Calculated after age purge

        await pdf_cache.put(key_old, pdf_data1)
        await asyncio.sleep(0.2)
        await pdf_cache.put(key_mid_lru, pdf_data2)
        await asyncio.sleep(0.2)
        await pdf_cache.put(key_new_mru, pdf_data3)
        await asyncio.sleep(0.05)

        # Age limit should remove key_old (older than 0.3 sec)
        age_cutoff_days = 0.3 / pdf_cache.SECONDS_PER_DAY
        # Size limit should remove key_mid_lru after key_old is gone
        # Size after age purge = size2 + size3
        size_after_age_purge = size2 + size3
        target_size_bytes = (
            size_after_age_purge - size2 + 10
        )  # Requires removing size2 (key_mid_lru)
        target_size_mbytes = target_size_bytes / pdf_cache.BYTES_PER_MBYTE

        purged_keys = await pdf_cache.purge(
            max_age_days=age_cutoff_days, max_size_mbytes=target_size_mbytes
        )

        # Expect key_old (age) and key_mid_lru (size/LRU) to be purged
        assert sorted(purged_keys) == sorted([key_old, key_mid_lru])
        assert not await pdf_cache.exists(key_old)
        assert not await pdf_cache.exists(key_mid_lru)
        assert await pdf_cache.exists(key_new_mru)  # Newest should remain

    async def test_purge_ignores_temp_files(self, pdf_cache: PdfCache):
        """Test that purge logic correctly ignores .part files."""
        key1 = "10.9600/purge_ignore_temp"
        await pdf_cache.put(key1, _create_dummy_pdf())
        await asyncio.sleep(0.05)

        # Manually create a temp file
        temp_file_path = pdf_cache._get_cache_path(key1) + pdf_cache.TEMP_SUFFIX
        await _create_file(temp_file_path, b"temp data")
        assert os.path.exists(temp_file_path)

        # Purge everything by size
        purged_keys = await pdf_cache.purge(max_size_mbytes=0)

        assert sorted(purged_keys) == sorted([key1])  # Only the main file purged
        assert not await pdf_cache.exists(key1)  # Main file gone
        assert os.path.exists(temp_file_path)  # Temp file ignored and remains

        # Cleanup the manually created temp file
        os.remove(temp_file_path)

    async def test_purge_ignores_zero_byte_files(self, pdf_cache: PdfCache):
        """Test that purge logic ignores zero-byte cache files."""
        key1 = "10.9700/purge_ignore_zero"
        key_zero = "10.9700/purge_zero_byte"

        await pdf_cache.put(key1, _create_dummy_pdf())  # Valid file
        await asyncio.sleep(0.05)

        # Manually create a zero-byte cache file (simulate corruption/issue)
        zero_file_path = pdf_cache._get_cache_path(key_zero)
        await _create_file(zero_file_path, b"")  # Creates empty file
        assert os.path.exists(zero_file_path)
        assert os.path.getsize(zero_file_path) == 0

        # Purge everything by size
        purged_keys = await pdf_cache.purge(max_size_mbytes=0)

        assert sorted(purged_keys) == sorted([key1])  # Only the valid file purged
        assert not await pdf_cache.exists(key1)  # Valid file gone
        assert os.path.exists(zero_file_path)  # Zero-byte file ignored and remains

        # Cleanup the manually created zero-byte file
        os.remove(zero_file_path)

    async def test_list_cached_keys_ignores_temp_and_zero(self, pdf_cache: PdfCache):
        """Test listing ignores temporary and zero-byte files."""
        key1 = "10.9800/list_valid"
        key_zero = "10.9800/list_zero"
        key_temp_suffix = "10.9800/list_temp"  # Base name for temp file

        await pdf_cache.put(key1, _create_dummy_pdf())
        await asyncio.sleep(0.05)

        # Create zero-byte and temp files manually
        zero_file_path = pdf_cache._get_cache_path(key_zero)
        temp_file_path = (
            pdf_cache._get_cache_path(key_temp_suffix) + pdf_cache.TEMP_SUFFIX
        )
        await _create_file(zero_file_path, b"")
        await _create_file(temp_file_path, b"temp")

        assert os.path.exists(zero_file_path)
        assert os.path.exists(temp_file_path)

        # List keys
        keys = await pdf_cache.list_cached_keys()

        # Only the valid key should be listed
        assert sorted(keys) == sorted([key1])

        # Cleanup
        os.remove(zero_file_path)
        os.remove(temp_file_path)
