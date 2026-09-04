"""Bounded HTTP transport for official NEMWeb collector resources."""

from collections.abc import Callable
from dataclasses import dataclass
from http.client import HTTPException
import math
import re
from typing import Any
from urllib.parse import urlsplit
from urllib.error import URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener


_USER_AGENT = "BatteryWatch-Collector/0.1"
_INDEX_URLS = frozenset((
    "https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/",
    "https://www.nemweb.com.au/REPORTS/CURRENT/DispatchIS_Reports/",
    "https://www.nemweb.com.au/REPORTS/CURRENT/Next_Day_Dispatch/",
    "https://www.nemweb.com.au/REPORTS/ARCHIVE/Next_Day_Dispatch/",
))
_CURRENT_RESOURCE_MAX_BYTES = 16 * 1024 * 1024
_ARCHIVE_RESOURCE_MAX_BYTES = 128 * 1024 * 1024
_MONTHLY_ARCHIVE_RESOURCE_MAX_BYTES = 256 * 1024 * 1024
_CURRENT_ARTIFACT_PATH_RE = re.compile(
    r"(?:"
    r"/REPORTS/CURRENT/Dispatch_SCADA/"
    r"PUBLIC_DISPATCHSCADA_[0-9]{12}_[0-9]{1,32}\.zip"
    r"|"
    r"/REPORTS/CURRENT/DispatchIS_Reports/"
    r"PUBLIC_DISPATCHIS_[0-9]{12}_[0-9]{1,32}\.zip"
    r"|"
    r"/REPORTS/CURRENT/Next_Day_Dispatch/"
    r"PUBLIC_NEXT_DAY_DISPATCH_[0-9]{8}_[0-9]{1,32}\.zip"
    r")"
)
_ARCHIVE_ARTIFACT_PATH_RE = re.compile(
    r"(?:"
    r"/REPORTS/ARCHIVE/Dispatch_SCADA/"
    r"PUBLIC_DISPATCHSCADA_[0-9]{8}\.zip"
    r"|"
    r"/REPORTS/ARCHIVE/DispatchIS_Reports/"
    r"PUBLIC_DISPATCHIS_[0-9]{8}\.zip"
    r")"
)
_MONTHLY_ARCHIVE_ARTIFACT_PATH_RE = re.compile(
    r"/REPORTS/ARCHIVE/Next_Day_Dispatch/"
    r"PUBLIC_NEXT_DAY_DISPATCH_[0-9]{6}01\.zip"
)


class NemwebHttpError(ValueError):
    """Safe public failure raised for an unusable NEMWeb response."""


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Any:
        raise NemwebHttpError("unusable NEMWeb response")


_DEFAULT_OPENER = build_opener(_NoRedirectHandler())


@dataclass(frozen=True)
class NemwebHttpResource:
    requested_url: str
    resolved_url: str
    body: bytes
    content_type: str | None
    etag: str | None
    last_modified: str | None


def _safe_optional_header(headers: Any, name: str) -> str | None:
    value = headers.get(name)
    if value is None:
        return None
    if (
        type(value) is not str
        or len(value) > 1024
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise NemwebHttpError("unusable NEMWeb response")
    return value


def fetch_nemweb_resource(
    url: str,
    *,
    max_bytes: int,
    timeout_seconds: float = 30.0,
    opener: Callable[..., Any] = _DEFAULT_OPENER.open,
) -> NemwebHttpResource:
    """Fetch one NEMWeb resource through an injectable HTTPS opener."""

    valid_url = False
    resource_max_bytes = _CURRENT_RESOURCE_MAX_BYTES
    if type(url) is str:
        try:
            parts = urlsplit(url)
        except ValueError as error:
            raise NemwebHttpError("invalid NEMWeb request") from error
        current_artifact = _CURRENT_ARTIFACT_PATH_RE.fullmatch(parts.path) is not None
        archive_artifact = _ARCHIVE_ARTIFACT_PATH_RE.fullmatch(parts.path) is not None
        monthly_archive_artifact = (
            _MONTHLY_ARCHIVE_ARTIFACT_PATH_RE.fullmatch(parts.path) is not None
        )
        valid_url = url in _INDEX_URLS or (
            parts.scheme == "https"
            and parts.netloc == "www.nemweb.com.au"
            and not parts.query
            and not parts.fragment
            and (current_artifact or archive_artifact or monthly_archive_artifact)
        )
        if monthly_archive_artifact:
            resource_max_bytes = _MONTHLY_ARCHIVE_RESOURCE_MAX_BYTES
        elif archive_artifact:
            resource_max_bytes = _ARCHIVE_RESOURCE_MAX_BYTES
    valid_limit = (
        type(max_bytes) is int and 0 < max_bytes <= resource_max_bytes
    )
    valid_timeout = (
        (type(timeout_seconds) is int and 0 < timeout_seconds <= 60)
        or (
            type(timeout_seconds) is float
            and math.isfinite(timeout_seconds)
            and 0 < timeout_seconds <= 60
        )
    )
    if not valid_url or not valid_limit or not valid_timeout:
        raise NemwebHttpError("invalid NEMWeb request")

    try:
        request = Request(
            url,
            headers={
                "User-Agent": _USER_AGENT,
                "Accept": "text/html,application/zip,application/octet-stream",
            },
        )
        with opener(request, timeout=timeout_seconds) as response:
            resolved_url = response.geturl()
            if response.status != 200 or resolved_url != url:
                raise NemwebHttpError("unusable NEMWeb response")

            content_length_header = _safe_optional_header(
                response.headers, "Content-Length"
            )
            declared_length: int | None = None
            if content_length_header is not None:
                if (
                    not content_length_header.isascii()
                    or not content_length_header.isdecimal()
                ):
                    raise NemwebHttpError("unusable NEMWeb response")
                declared_length = int(content_length_header)
                if declared_length > max_bytes:
                    raise NemwebHttpError("unusable NEMWeb response")

            content_encoding = _safe_optional_header(
                response.headers, "Content-Encoding"
            )
            if content_encoding is not None and content_encoding.lower() != "identity":
                raise NemwebHttpError("unusable NEMWeb response")

            content_type = _safe_optional_header(response.headers, "Content-Type")
            etag = _safe_optional_header(response.headers, "ETag")
            last_modified = _safe_optional_header(
                response.headers, "Last-Modified"
            )
            body = response.read(max_bytes + 1)
            if (
                type(body) is not bytes
                or not body
                or len(body) > max_bytes
                or (declared_length is not None and len(body) != declared_length)
            ):
                raise NemwebHttpError("unusable NEMWeb response")
            return NemwebHttpResource(
                requested_url=url,
                resolved_url=resolved_url,
                body=body,
                content_type=content_type,
                etag=etag,
                last_modified=last_modified,
            )
    except NemwebHttpError:
        raise
    except (
        EOFError,
        HTTPException,
        URLError,
        TimeoutError,
        OSError,
        ValueError,
    ) as error:
        raise NemwebHttpError("unusable NEMWeb response") from error
