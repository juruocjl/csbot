"""OpenAI endpoint helpers and the gpt-image-2.6-sunburst image tool.

Kept stateless so both the DSH runner and the QQ output guard can share the same
endpoint/proxy decisions. Only explicit loopback HTTP proxies are accepted, and
no function here ever logs or returns the API key.
"""
from __future__ import annotations

import asyncio
import base64
from io import BytesIO
import os
from pathlib import Path
import re
import uuid
from urllib.parse import urlsplit

import httpx
from PIL import Image, UnidentifiedImageError

DEFAULT_OPENAI_PROXY = "http://127.0.0.1:7890"
DEFAULT_OPENAI_MODEL = "gpt-6.2-sol"
DEFAULT_IMAGE_MODEL = "gpt-image-2.6-sunburst"

IMAGE_SIZES = {"1024x1024", "1536x1024", "1024x1536", "auto"}
IMAGE_QUALITIES = {"low", "medium", "high", "auto"}
IMAGE_BACKGROUNDS = {"auto", "opaque", "transparent"}
_RETRYABLE_STATUS = {502, 503, 504, 429}
_RETRYABLE_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.RemoteProtocolError,
    httpx.ReadError,
)
_IMAGE_MAX_BYTES = 16 * 1024 * 1024
_IMAGE_MAX_PIXELS = 16_000_000
_PROXY_ERROR = "Proxy URL must be a loopback HTTP endpoint without credentials or a path"


def validate_loopback_http_proxy(proxy_url: str) -> str:
    raw = str(proxy_url or "").strip()
    if not raw:
        return ""
    parsed = urlsplit(raw)
    try:
        port = parsed.port
    except ValueError:
        raise ValueError(_PROXY_ERROR) from None
    if (
        parsed.scheme != "http"
        or parsed.hostname not in ("127.0.0.1", "::1")
        or not port
        or not 1 <= port <= 65535
        or parsed.username
        or parsed.password
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(_PROXY_ERROR)
    host = "[::1]" if parsed.hostname == "::1" else "127.0.0.1"
    return f"http://{host}:{port}"


def is_openai_endpoint(endpoint: str, provider_override: str = "") -> bool:
    override = str(provider_override or "").strip().lower()
    if override == "openai" or os.getenv("CS_AI_PROVIDER", "").strip().lower() == "openai":
        return True
    if override in {"deepseek", "deepseek-official"}:
        return False
    parsed = urlsplit(str(endpoint or "").strip())
    return (parsed.hostname or "").lower() == "api.openai.com"


def normalize_openai_base_url(endpoint: str) -> str:
    raw = str(endpoint or "").strip().rstrip("/")
    parsed = urlsplit(raw)
    if (parsed.hostname or "").lower() == "api.openai.com" and parsed.path in ("", "/"):
        return raw + "/v1"
    return raw


def resolve_openai_proxy(endpoint: str, configured_proxy: str | None = None) -> str:
    if not is_openai_endpoint(endpoint):
        return ""
    raw = (
        configured_proxy
        if configured_proxy is not None
        else os.getenv("CS_AI_OPENAI_PROXY_URL", DEFAULT_OPENAI_PROXY)
    )
    raw = str(raw or "").strip()
    if not raw:
        return ""
    hostname = (urlsplit(str(endpoint or "").strip()).hostname or "").lower()
    if hostname in {"127.0.0.1", "localhost", "::1"} and os.getenv(
        "CS_AI_FORCE_LOOPBACK_LLM_PROXY", "0"
    ) != "1":
        return ""
    return validate_loopback_http_proxy(raw)


def is_openai_non_reasoning_model(model: str) -> bool:
    name = str(model or "").strip().lower()
    return name.startswith(("gpt-4", "gpt-3.5"))


def _safe_error_text(response: httpx.Response, api_key: str) -> str:
    detail = ""
    try:
        payload = response.json()
        if isinstance(payload, dict):
            error = payload.get("error")
            if isinstance(error, dict):
                detail = str(error.get("message") or error.get("code") or "")
            elif error:
                detail = str(error)
    except ValueError:
        detail = ""
    if not detail:
        detail = str(response.text or "")[:400]
    detail = str(detail).replace(api_key or "", "[REDACTED]")
    detail = re.sub(r"\s+", " ", detail).strip()
    return detail[:160]


def _decode_image(b64_json: str) -> tuple[bytes, str, int, int]:
    try:
        raw = base64.b64decode(b64_json, validate=True)
    except (ValueError, TypeError):
        raise ValueError("OpenAI image generation returned invalid base64") from None
    if not raw or len(raw) > _IMAGE_MAX_BYTES:
        raise ValueError("OpenAI image generation returned an empty or oversized image")
    try:
        with Image.open(BytesIO(raw)) as image:
            image_format = str(image.format or "").upper()
            width, height = image.size
            image.verify()
    except (UnidentifiedImageError, OSError, ValueError):
        raise ValueError("OpenAI image generation returned an unreadable image") from None
    if image_format not in {"PNG", "JPEG", "WEBP"}:
        raise ValueError("OpenAI image generation returned an unsupported image format")
    if width * height > _IMAGE_MAX_PIXELS:
        raise ValueError("OpenAI image generation returned an oversized image")
    return raw, image_format, width, height


async def generate_openai_image(
    *,
    prompt: str,
    api_key: str,
    endpoint: str,
    artifacts_dir: Path,
    model: str = DEFAULT_IMAGE_MODEL,
    proxy_url: str | None = None,
    size: str = "1024x1024",
    quality: str = "medium",
    background: str = "auto",
    caption: str = "",
    timeout: float = 120.0,
) -> dict:
    prompt = str(prompt or "").strip()
    if not 1 <= len(prompt) <= 4000:
        raise ValueError("prompt must be 1-4000 characters")
    if size not in IMAGE_SIZES:
        raise ValueError("unsupported image size")
    if quality not in IMAGE_QUALITIES:
        raise ValueError("unsupported image quality")
    if background not in IMAGE_BACKGROUNDS:
        raise ValueError("unsupported image background")
    safe_caption = str(caption or "").strip()[:120]

    url = f"{normalize_openai_base_url(endpoint)}/images/generations"
    payload = {
        "model": str(model or "").strip() or DEFAULT_IMAGE_MODEL,
        "prompt": prompt,
        "n": 1,
        "size": size,
        "quality": quality,
        "background": background,
        "output_format": "png",
    }
    effective_proxy = resolve_openai_proxy(endpoint, proxy_url)
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    response: httpx.Response | None = None
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(
                proxy=effective_proxy or None,
                timeout=httpx.Timeout(timeout, connect=15.0),
                trust_env=False,
            ) as client:
                response = await client.post(url, json=payload, headers=headers)
        except _RETRYABLE_ERRORS as exc:
            if attempt < 2:
                await asyncio.sleep(0.8 * (2**attempt))
                continue
            raise ValueError(
                f"OpenAI image generation failed after retries ({type(exc).__name__})"
            ) from None

        if response.status_code == 200:
            break
        if response.status_code in _RETRYABLE_STATUS and attempt < 2:
            await asyncio.sleep(0.8 * (2**attempt))
            continue
        raise ValueError(
            f"OpenAI image generation failed (HTTP {response.status_code}): "
            f"{_safe_error_text(response, api_key)}"
        )

    if response is None or response.status_code != 200:
        raise ValueError("OpenAI image generation failed")

    try:
        body = response.json()
    except ValueError:
        raise ValueError("OpenAI image generation returned invalid JSON") from None
    items = body.get("data") if isinstance(body, dict) else None
    if not isinstance(items, list) or not items or not isinstance(items[0], dict):
        raise ValueError("OpenAI image generation returned no image data")
    item = items[0]
    b64_json = item.get("b64_json")
    if not isinstance(b64_json, str) or not b64_json:
        raise ValueError("OpenAI image generation returned no image bytes")
    revised_prompt = item.get("revised_prompt")

    raw, image_format, width, height = _decode_image(b64_json)
    extension = {"PNG": ".png", "JPEG": ".jpg", "WEBP": ".webp"}[image_format]
    artifacts_dir = Path(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    path = artifacts_dir / f"{uuid.uuid4().hex[:8]}-generated{extension}"
    path.write_bytes(raw)

    result = {
        "ok": True,
        "delivered": True,
        "model": payload["model"],
        "size": f"{width}x{height}",
        "quality": quality,
        "background": background,
        "bytes": len(raw),
        "file_path": str(path.resolve()),
        "caption": safe_caption,
    }
    if revised_prompt:
        result["revised_prompt"] = str(revised_prompt)[:500]
    return result
