import enum
import functools
import urllib.parse
from typing import Callable, Concatenate, ParamSpec

import bs4
import requests
import urlextract

from bio_data_harmoniser.core.extraction.pages import Page

P = ParamSpec("P")


def ftp_to_http(url: str) -> str:
    return url.replace("ftp://", "http://")


def is_doi(url: urllib.parse.ParseResult) -> bool:
    return url.netloc in ("www.doi.org", "doi.org")


def clean_url(page: Page, url: str) -> str:
    url = url.strip().strip(".")
    parsed_url = urllib.parse.urlparse(url)
    if is_doi(parsed_url):
        return urllib.parse.urlunparse(
            ("https", "doi.org", parsed_url.path, "", "", "")
        )
    if parsed_url.scheme == "":
        parsed_base = urllib.parse.urlparse(page.url)
        url = urllib.parse.urlunparse(
            (
                parsed_base.scheme,
                parsed_base.netloc,
                parsed_url.path,
                parsed_url.params,
                parsed_url.query,
                parsed_url.fragment,
            )
        )
    return ftp_to_http(url)


def ensure_correct_urls(
    function: Callable[Concatenate[Page, P], list[str]]
) -> Callable[Concatenate[Page, P], list[str]]:
    @functools.wraps(function)
    def wrapper(page: Page, *args: P.args, **kwargs: P.kwargs) -> list[str]:
        return [
            clean_url(page, url)
            for url in function(page, *args, **kwargs)
            if not url.startswith("#") and not url.startswith("mailto:")
        ]

    return wrapper


@ensure_correct_urls
def extract_urls_from_text(page: Page) -> list[str]:
    extractor = urlextract.URLExtract()
    return [url for url in extractor.find_urls(page.text)]


@ensure_correct_urls
def extract_urls_from_html(page: Page) -> list[str]:
    soup = bs4.BeautifulSoup(page.content, "html.parser")
    urls = [url.get("href") for url in soup.find_all("a") if url.get("href")]
    return urls


class UriType(enum.Enum):
    LOCAL_FILE = "local_file"
    REMOTE_FILE_HTTP = "remote_file_http"
    REMOTE_FILE_S3 = "remote_file_s3"
    REMOTE_FILE_GCS = "remote_file_gcs"
    WEBPAGE_HTTP = "webpage_http"
    UNKNOWN_HTTP = "unknown_http"
    UNKNOWN = "unknown"


def get_url_type(url: str) -> UriType:
    parsed_url = urllib.parse.urlparse(url)
    if parsed_url.scheme in ("", "file"):
        return UriType.LOCAL_FILE
    if parsed_url.scheme in ("http", "https"):
        response = requests.head(url, allow_redirects=True)
        if response.status_code != 200:
            return UriType.UNKNOWN_HTTP
        if "text/html" in (response.headers.get("Content-Type") or "").lower():
            return UriType.WEBPAGE_HTTP
        return UriType.REMOTE_FILE_HTTP
    return UriType.UNKNOWN
