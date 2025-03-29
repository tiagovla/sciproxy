# -*- coding: utf-8 -*-
"""
PDF Cache Manager (`PdfCache`)

This module provides the `PdfCache` class, designed to manage a filesystem cache
for PDF files, typically identified by a unique key like a Digital Object Identifier (DOI).

Core functionalities include:
- Storing PDF data (provided as BytesIO) into the cache.
- Retrieving the filesystem path for a cached PDF.
- Retrieving the content of a cached PDF as a BytesIO object.
- Checking for the existence of a PDF in the cache.
- Automatically handling minimal sanitization of keys (DOIs) for filesystem compatibility
  (specifically, replacing '/' with '@').
- Automatically "touching" cached files upon retrieval (updating access/modification times)
  to support Least Recently Used (LRU) strategies.
- Providing methods to list cached items.
- Providing a unified method to purge the cache based on optional criteria:
    - Maximum age of files (based on last access time).
    - Maximum total size of the cache directory (purging LRU files until the limit is met).

Dependencies:
- aiofiles: For asynchronous file operations.
- pikepdf: For opening, validating, and saving PDF files (including linearization).
- asyncio: For running synchronous operations (like `os.utime`, `pikepdf.save`) in executors.
"""

import io
import aiofiles
import aiofiles.os
import os
import logging
import pikepdf
import asyncio
import time
from typing import Optional, List, Tuple, Set

logger = logging.getLogger(__name__)


class PdfCache:
    """
    Manage PDF file caching using pikepdf. Replace '/' with '@'.
    Touch files on cache hit. Provide a unified purge method based on
    optional age and/or total size limits.
    """

    # --- Constants ---
    SANITIZE_TARGET = "/"
    SANITIZE_REPLACEMENT = "@"
    CACHE_SUFFIX = ".pdf"
    TEMP_SUFFIX = ".part"
    SECONDS_PER_DAY = 24 * 60 * 60
    BYTES_PER_MBYTE = 1024 * 1024

    def __init__(self, cache_dir: str):
        """
        Initialize the PDF cache manager.

        Args:
            cache_dir: Specify the directory path for storing cached files.
                       Create the directory if it does not exist.
        """
        self.cache_dir = cache_dir
        try:
            # Ensure the cache directory exists.
            os.makedirs(self.cache_dir, exist_ok=True)
            logger.info(f"Cache directory confirmed/created at: {self.cache_dir}")
        except OSError as e:
            logger.error(
                f"CRITICAL: Failed to create cache directory {self.cache_dir}: {e}"
            )
            raise e

    # --- Key Handling ---
    def _sanitize_key(self, key: str) -> str:
        """Replace '/' with '@' in the provided key."""
        sanitized = key.replace(self.SANITIZE_TARGET, self.SANITIZE_REPLACEMENT)
        if not sanitized or sanitized == "." or sanitized == "..":
            logger.warning(
                f"Sanitized key '{sanitized}' from original '{key}' may be invalid."
            )
        return sanitized

    def _unsanitize_filename(self, filename: str) -> str:
        """Convert a sanitized filename back to the original key (Replace '@' with '/')."""
        # Expect filename without the cache suffix.
        return filename.replace(self.SANITIZE_REPLACEMENT, self.SANITIZE_TARGET)

    def _get_cache_path(self, raw_key: str) -> str:
        """Generate the full cache file path for a given raw key."""
        sanitized_key = self._sanitize_key(raw_key)
        return os.path.join(self.cache_dir, f"{sanitized_key}{self.CACHE_SUFFIX}")

    # --- Core Cache Operations ---
    async def exists(self, raw_key: str) -> bool:
        """
        Check if a cached file exists for the specified raw key.

        Args:
            raw_key: The identifier (e.g., DOI) to check.

        Returns:
            True if a cached file exists, False otherwise. Catch OS errors.
        """
        try:
            cache_path = self._get_cache_path(raw_key)
            return await aiofiles.os.path.exists(cache_path)
        except OSError as e:
            logger.error(f"OS Error checking existence for key '{raw_key}': {e}")
            return False
        except Exception as e:
            logger.error(
                f"Unexpected error checking existence for key '{raw_key}': {e}",
                exc_info=True,
            )
            return False

    async def get_path(self, raw_key: str) -> Optional[str]:
        """
        Retrieve the path to the cached file if it exists.
        Touch the file to update its access time on success (cache hit).

        Args:
            raw_key: The identifier (e.g., DOI) to retrieve.

        Returns:
            The full path to the file if found, otherwise None. Catch OS errors.
        """
        try:
            cache_path = self._get_cache_path(raw_key)
            if await aiofiles.os.path.exists(cache_path):
                logger.debug(f"Cache hit for key '{raw_key}' at {cache_path}")
                # --- Touch the file ---
                try:
                    loop = asyncio.get_running_loop()
                    # Update access and modification times to now via executor.
                    await loop.run_in_executor(None, os.utime, cache_path, None)
                    logger.debug(f"Touched cache file: {cache_path}")
                except Exception as touch_err:
                    # Log failure but proceed with returning the path.
                    logger.warning(
                        f"Failed to touch cache file {cache_path}: {touch_err}"
                    )
                # --- End Touch ---
                return cache_path
            else:
                logger.debug(f"Cache miss for key '{raw_key}'")
                return None
        except OSError as e:
            logger.error(f"OS Error during get_path for key '{raw_key}': {e}")
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error in get_path for key '{raw_key}': {e}", exc_info=True
            )
            return None

    async def get_data(self, raw_key: str) -> Optional[io.BytesIO]:
        """
        Retrieve the content of a cached PDF as an in-memory BytesIO object if it exists.
        On successful retrieval, update the file's access time.

        Args:
            raw_key: The unique identifier for the PDF (e.g., a DOI).

        Returns:
            Optional[io.BytesIO]: A BytesIO object containing the PDF data if found, otherwise None.
        """
        # Use get_path first, as it handles existence check and touching the file
        cache_path = await self.get_path(raw_key)
        if cache_path:
            try:
                # Asynchronously open and read the file content
                async with aiofiles.open(cache_path, mode="rb") as f:
                    content = await f.read()
                logger.debug(
                    f"Successfully read {len(content)} bytes for key '{raw_key}' from {cache_path}"
                )
                return io.BytesIO(content)
            except OSError as e:
                logger.error(
                    f"OS error reading cache file {cache_path} for key '{raw_key}': {e}"
                )
                return None
            except Exception as e:
                logger.error(
                    f"Unexpected error reading cache file {cache_path} for key '{raw_key}': {e}",
                    exc_info=True,
                )
                return None
        else:
            # get_path already logged the cache miss
            return None

    def _save_pdf_sync(self, pdf_data: io.BytesIO, path: str):
        """
        Save PDF data from BytesIO synchronously using pikepdf and a temp file.
        Do not call directly; use via `run_in_executor`.

        Args:
            pdf_data: BytesIO buffer containing the PDF data.
            path: The target filesystem path for the cached PDF.
        """
        pdf_data.seek(0)
        temp_path = path + self.TEMP_SUFFIX
        try:
            # Ensure parent directory exists.
            os.makedirs(os.path.dirname(path), exist_ok=True)
            # Open buffer with pikepdf and save to temp file with linearization.
            with pikepdf.open(pdf_data) as pdf:
                pdf.save(temp_path, linearize=True)
            # Rename temp file to final path atomically (on most systems).
            os.rename(temp_path, path)
            logger.info(f"Successfully cached PDF to {path}")
        except pikepdf.PdfError as e:
            logger.error(f"Pikepdf error saving {temp_path} (for {path}): {e}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError as rm_err:
                    logger.error(f"Failed remove {temp_path}: {rm_err}")
        except OSError as e:
            logger.error(f"OS error saving/renaming {temp_path} (for {path}): {e}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError as rm_err:
                    logger.error(f"Failed remove {temp_path}: {rm_err}")
        except Exception as e:
            logger.error(
                f"Unexpected error saving {temp_path} (for {path}): {e}", exc_info=True
            )
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError as rm_err:
                    logger.error(f"Failed remove {temp_path}: {rm_err}")

    async def put(self, raw_key: str, pdf_data: io.BytesIO):
        """
        Save the provided PDF data (BytesIO buffer) to the cache for the raw key.
        Run the synchronous save operation in an executor thread.

        Args:
            raw_key: The identifier (e.g., DOI) for the PDF.
            pdf_data: BytesIO object containing the full PDF content.
        """
        try:
            cache_path = self._get_cache_path(raw_key)
            loop = asyncio.get_running_loop()
            logger.debug(f"Scheduling cache save for key '{raw_key}' to {cache_path}")
            # Schedule the sync save function in the executor.
            await loop.run_in_executor(None, self._save_pdf_sync, pdf_data, cache_path)
        except OSError as e:
            logger.error(f"OS Error preventing cache put for key '{raw_key}': {e}")
        except Exception as e:
            logger.error(
                f"Error during executor task for caching key '{raw_key}': {e}",
                exc_info=True,
            )

    # --- Cache Management ---

    async def _get_cache_files_info(
        self,
    ) -> Tuple[List[Tuple[float, int, str, str]], int]:
        """
        Internal helper: Scan cache dir, return list of file info and total size.

        Returns:
            A tuple containing:
            - List of tuples: (access_time, size, full_path, original_key) for valid cache files.
            - Integer: The current total size of all valid cache files in bytes.
        """
        cached_files_info: List[Tuple[float, int, str, str]] = []
        current_total_size: int = 0
        try:
            for filename in await aiofiles.os.listdir(self.cache_dir):
                if filename.endswith(self.CACHE_SUFFIX) and not filename.endswith(
                    self.TEMP_SUFFIX
                ):
                    full_path = os.path.join(self.cache_dir, filename)
                    try:
                        stat_result = await aiofiles.os.stat(full_path)
                        file_size = stat_result.st_size
                        if file_size > 0:  # Process only non-empty files
                            access_time = stat_result.st_atime
                            sanitized_key_part = filename[: -len(self.CACHE_SUFFIX)]
                            original_key = self._unsanitize_filename(sanitized_key_part)
                            cached_files_info.append(
                                (access_time, file_size, full_path, original_key)
                            )
                            current_total_size += file_size
                    except FileNotFoundError:
                        continue  # Race condition
                    except OSError as stat_err:
                        logger.error(
                            f"OS Error stating {filename} during info scan: {stat_err}"
                        )
        except FileNotFoundError:
            logger.warning(
                f"Cache directory {self.cache_dir} not found during info scan."
            )
        except OSError as e:
            logger.error(
                f"OS Error listing cache dir {self.cache_dir} during info scan: {e}"
            )
        except Exception as e:
            logger.error(f"Unexpected error during cache info scan: {e}", exc_info=True)
        return cached_files_info, current_total_size

    async def list_cached_keys(self) -> List[str]:
        """
        Scan the cache directory and return a list of original keys (DOIs)
        for successfully cached PDF files.
        """
        # Uses the helper function but only returns the keys
        cached_files_info, _ = await self._get_cache_files_info()
        original_keys = [info[3] for info in cached_files_info]
        logger.info(
            f"Found {len(original_keys)} valid items in cache via list_cached_keys."
        )
        return original_keys

    async def purge(
        self,
        max_size_mbytes: Optional[float] = None,
        max_age_days: Optional[int] = None,
    ) -> List[str]:
        """
        Purge cache based on optional maximum size and/or maximum age criteria.
        Age purge runs first, then size purge if needed.

        Args:
            max_size_mbytes: Optional target maximum total size of the cache in Megabytes.
            max_age_days: Optional maximum age in days. Files accessed longer ago
                          than this will be removed.

        Returns:
             A list of the unique original keys (DOIs) for files successfully purged.
        """
        purged_keys_set: Set[str] = set()

        # --- Step 1: Purge by Age (if specified) ---
        if max_age_days is not None and max_age_days > 0:
            cutoff_timestamp = time.time() - (max_age_days * self.SECONDS_PER_DAY)
            logger.info(
                f"Initiating purge for files older than {max_age_days} days (accessed before {cutoff_timestamp})..."
            )
            try:
                # We need file info for age check
                cached_files_info_for_age, _ = await self._get_cache_files_info()
                files_to_remove_by_age: List[Tuple[str, str]] = (
                    []
                )  # (full_path, original_key)

                for (
                    access_time,
                    _,
                    full_path,
                    original_key,
                ) in cached_files_info_for_age:
                    if access_time < cutoff_timestamp:
                        files_to_remove_by_age.append((full_path, original_key))

                logger.info(
                    f"[Age Purge] Identified {len(files_to_remove_by_age)} files older than {max_age_days} days."
                )

                for full_path, original_key in files_to_remove_by_age:
                    try:
                        await aiofiles.os.remove(full_path)
                        purged_keys_set.add(
                            original_key
                        )  # Add to set automatically handles duplicates
                        logger.info(
                            f"[Age Purge] Removed: {full_path} (key: {original_key})"
                        )
                    except FileNotFoundError:
                        logger.warning(
                            f"[Age Purge] File not found (already removed?): {full_path}"
                        )
                    except OSError as remove_err:
                        logger.error(
                            f"[Age Purge] Failed remove {full_path}: {remove_err}"
                        )
                    except Exception as e:
                        logger.error(
                            f"[Age Purge] Unexpected error removing {full_path}: {e}",
                            exc_info=True,
                        )

            except Exception as e:  # Catch errors during the age purge process
                logger.error(f"Error during age purge phase: {e}", exc_info=True)

        # --- Step 2: Purge by Size (if specified) ---
        if max_size_mbytes is not None and max_size_mbytes >= 0:
            max_size_bytes = int(
                max_size_mbytes * self.BYTES_PER_MBYTE
            )  # Convert MB to bytes
            logger.info(
                f"Initiating purge to bring cache size below {max_size_bytes} bytes ({max_size_mbytes} MB)..."
            )
            try:
                # Get UPDATED file info and size AFTER potential age purge
                cached_files_info_for_size, current_total_size = (
                    await self._get_cache_files_info()
                )

                logger.info(
                    f"[Size Purge] Current cache size: {current_total_size} bytes. Target size: {max_size_bytes} bytes."
                )

                if current_total_size <= max_size_bytes:
                    logger.info(
                        "[Size Purge] Cache size is within the limit. No size purge needed."
                    )
                else:
                    # Sort files by access time (oldest first - LRU) to remove oldest first
                    cached_files_info_for_size.sort(
                        key=lambda x: x[0]
                    )  # Sort by access_time[0]

                    # Iteratively remove LRU files until size limit is met
                    for (
                        access_time,
                        file_size,
                        full_path,
                        original_key,
                    ) in cached_files_info_for_size:
                        if current_total_size <= max_size_bytes:
                            logger.info(
                                f"[Size Purge] Cache size ({current_total_size}) reached target ({max_size_bytes}). Stop."
                            )
                            break  # Stop removing files

                        logger.debug(
                            f"[Size Purge] Attempting remove LRU: {full_path} (size: {file_size})"
                        )
                        try:
                            await aiofiles.os.remove(full_path)
                            # Only decrement size and add key if remove succeeds
                            current_total_size -= file_size
                            purged_keys_set.add(original_key)
                            logger.info(
                                f"[Size Purge] Removed: {full_path} (key: {original_key}, new total: {current_total_size})"
                            )
                        except FileNotFoundError:
                            logger.warning(
                                f"[Size Purge] File not found (already removed?): {full_path}"
                            )
                        except OSError as remove_err:
                            logger.error(
                                f"[Size Purge] Failed remove {full_path}: {remove_err}"
                            )
                        except Exception as e:
                            logger.error(
                                f"[Size Purge] Unexpected error removing {full_path}: {e}",
                                exc_info=True,
                            )

                    if current_total_size > max_size_bytes:
                        logger.warning(
                            f"[Size Purge] Finished, but size ({current_total_size}) still exceeds target ({max_size_bytes})."
                        )

            except Exception as e:  # Catch errors during the size purge process
                logger.error(f"Error during size purge phase: {e}", exc_info=True)

        final_purged_list = list(purged_keys_set)
        logger.info(
            f"Purge complete. Total unique items removed: {len(final_purged_list)}"
        )
        return final_purged_list
