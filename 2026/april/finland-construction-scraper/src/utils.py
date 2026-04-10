import re
from urllib.parse import urlparse


def normalize_website(url: str) -> str:
    url = str(url).strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url.rstrip("/")


def get_domain(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc.replace("www.", "").lower()


def clean_phone(phone: str) -> str:
    return re.sub(r"\s+", " ", phone).strip()