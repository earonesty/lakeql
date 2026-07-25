from __future__ import annotations

import io
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import httpx
from PIL import Image, UnidentifiedImageError


@dataclass(frozen=True)
class FetchedImage:
    image: Image.Image
    bytes_read: int
    attempts: int


class ImageFetchError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool, attempts: int) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.attempts = attempts


class ImageFetcher:
    def __init__(
        self,
        *,
        max_image_bytes: int,
        timeout_seconds: float,
        attempts: int = 3,
    ) -> None:
        self.max_image_bytes = max_image_bytes
        self.attempts = attempts
        self.client = httpx.Client(
            follow_redirects=True,
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
            headers={"User-Agent": "lakeql-semantic-museum/0.1"},
        )

    def close(self) -> None:
        self.client.close()

    def fetch(self, source: str) -> FetchedImage:
        parsed = urlparse(source)
        if parsed.scheme not in {"http", "https"}:
            path = Path(source.removeprefix("file://"))
            size = path.stat().st_size
            if size > self.max_image_bytes:
                raise ImageFetchError(
                    f"image byte budget exceeded ({size} > {self.max_image_bytes})",
                    retryable=False,
                    attempts=1,
                )
            raw = path.read_bytes()
            return FetchedImage(self._decode(raw), len(raw), 1)
        last_error: Exception | None = None
        for attempt in range(1, self.attempts + 1):
            try:
                with self.client.stream("GET", source) as response:
                    response.raise_for_status()
                    length = response.headers.get("content-length")
                    if length is not None and int(length) > self.max_image_bytes:
                        message = (
                            "image content-length exceeds budget "
                            f"({length} > {self.max_image_bytes})"
                        )
                        raise ImageFetchError(
                            message,
                            retryable=False,
                            attempts=attempt,
                        )
                    raw = bytearray()
                    for chunk in response.iter_bytes():
                        raw.extend(chunk)
                        if len(raw) > self.max_image_bytes:
                            raise ImageFetchError(
                                f"image byte budget exceeded ({len(raw)} > {self.max_image_bytes})",
                                retryable=False,
                                attempts=attempt,
                            )
                return FetchedImage(self._decode(bytes(raw)), len(raw), attempt)
            except ImageFetchError:
                raise
            except (httpx.TimeoutException, httpx.NetworkError) as error:
                last_error = error
                if attempt < self.attempts:
                    time.sleep(0.2 * (2 ** (attempt - 1)))
                    continue
            except httpx.HTTPStatusError as error:
                last_error = error
                retryable = error.response.status_code in {408, 409, 425, 429} or (
                    500 <= error.response.status_code < 600
                )
                if retryable and attempt < self.attempts:
                    time.sleep(0.2 * (2 ** (attempt - 1)))
                    continue
                raise ImageFetchError(
                    f"HTTP {error.response.status_code}",
                    retryable=retryable,
                    attempts=attempt,
                ) from error
            except (OSError, UnidentifiedImageError) as error:
                raise ImageFetchError(
                    str(error),
                    retryable=False,
                    attempts=attempt,
                ) from error
        raise ImageFetchError(
            str(last_error) if last_error else "image request failed",
            retryable=True,
            attempts=self.attempts,
        )

    def _decode(self, raw: bytes) -> Image.Image:
        if len(raw) > self.max_image_bytes:
            raise ImageFetchError(
                f"image byte budget exceeded ({len(raw)} > {self.max_image_bytes})",
                retryable=False,
                attempts=1,
            )
        try:
            with Image.open(io.BytesIO(raw)) as image:
                image.load()
                if image.width <= 0 or image.height <= 0:
                    raise ValueError("image has invalid dimensions")
                if image.width * image.height > 100_000_000:
                    raise ValueError("decoded image exceeds 100 million pixels")
                return image.convert("RGB")
        except (OSError, UnidentifiedImageError, ValueError) as error:
            raise ImageFetchError(
                f"image decode failed: {error}",
                retryable=False,
                attempts=1,
            ) from error
