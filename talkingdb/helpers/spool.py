"""Utilities for safely spooling uploads to local disk."""

import os
import shutil
import tempfile
from typing import Optional, Tuple

from fastapi import HTTPException, UploadFile, status

from talkingdb.helpers.validation import MAX_FILE_SIZE_BYTES, MAX_FILE_SIZE_MB


SPOOL_DIR = os.getenv("TDB_SPOOL_DIR", "/var/tmp/tdb-spool")
MIN_FREE_SPOOL_MB = int(os.getenv("TDB_MIN_FREE_SPOOL_MB", "512"))
SPOOL_CHUNK_BYTES = int(os.getenv("TDB_SPOOL_CHUNK_BYTES", str(1024 * 1024)))
RETRY_AFTER_SECONDS = int(os.getenv("TDB_RETRY_AFTER_SECONDS", "30"))


def assert_spool_capacity(
    spool_dir: Optional[str] = None,
    min_free_mb: Optional[int] = None,
    retry_after_seconds: Optional[int] = None,
) -> None:
    """Ensure sufficient free disk space exists for upload spooling."""
    target = spool_dir or SPOOL_DIR
    floor_mb = min_free_mb if min_free_mb is not None else MIN_FREE_SPOOL_MB
    retry = retry_after_seconds if retry_after_seconds is not None else RETRY_AFTER_SECONDS

    os.makedirs(target, exist_ok=True)
    free_mb = shutil.disk_usage(target).free // (1024 * 1024)
    if free_mb < floor_mb:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "SPOOL_CAPACITY_EXCEEDED",
                "error_code": "SPOOL_CAPACITY_EXCEEDED",
                "message": (
                    f"Insufficient spool space ({free_mb}MB free, "
                    f"{floor_mb}MB required)"
                ),
                "retry_after_seconds": retry,
            },
            headers={"Retry-After": str(retry)},
        )


async def spool_upload(
    file: UploadFile,
    *,
    spool_dir: Optional[str] = None,
    max_size_bytes: Optional[int] = None,
    max_size_mb: Optional[int] = None,
    chunk_size: Optional[int] = None,
) -> Tuple[str, int]:
    """Stream an uploaded file to local disk with bounded memory usage."""
    target = spool_dir or SPOOL_DIR
    cap_bytes = max_size_bytes if max_size_bytes is not None else MAX_FILE_SIZE_BYTES
    cap_mb = max_size_mb if max_size_mb is not None else MAX_FILE_SIZE_MB
    chunk = chunk_size or SPOOL_CHUNK_BYTES

    os.makedirs(target, exist_ok=True)

    tmp = tempfile.NamedTemporaryFile(
        prefix="tdb-upload-", suffix=".docx",
        dir=target, delete=False,
    )
    temp_path = tmp.name
    size = 0
    try:
        while True:
            data = await file.read(chunk)
            if not data:
                break
            size += len(data)
            if size > cap_bytes:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail={
                        "error_code": "FILE_TOO_LARGE",
                        "message": (
                            f"File exceeds the maximum allowed size ({cap_mb}MB)"
                        ),
                        "max_file_size_mb": cap_mb,
                    },
                )
            tmp.write(data)
        tmp.flush()
        return temp_path, size
    except BaseException:
        # 413, client disconnect, disk-full mid-write: never leak the partial.
        tmp.close()
        discard(temp_path)
        raise
    finally:
        if not tmp.closed:
            tmp.close()


def discard(temp_path: Optional[str]) -> None:
    """Delete a spooled file. Idempotent and never raises."""
    if not temp_path:
        return
    try:
        os.unlink(temp_path)
    except OSError:
        pass
