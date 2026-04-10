import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from src.config import (
    HEADERS,
    GENERIC_PREFIXES,
    CONTACT_PATHS,
    CITY_KEYWORDS,
    EMAIL_REGEX,
    PHONE_REGEX,
    REQUEST_TIMEOUT,
    SLEEP_SECONDS,
)
from src.utils import normalize_website, get_domain, clean_phone


def fetch_page(url: str) -> str:
    try:
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.text
    except Exception:
        return ""


def extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    return soup.get_text(separator=" ", strip=True)


def extract_emails(text: str) -> list[str]:
    emails = re.findall(EMAIL_REGEX, text, re.IGNORECASE)
    return sorted(set(email.lower() for email in emails))


def is_generic_email(email: str) -> bool:
    local_part = email.split("@")[0].lower()

    if local_part in GENERIC_PREFIXES:
        return True

    return any(local_part.startswith(prefix) for prefix in GENERIC_PREFIXES)


def pick_best_generic_email(emails: list[str], domain: str) -> str:
    generic_emails = [e for e in emails if is_generic_email(e)]

    same_domain = [e for e in generic_emails if e.endswith(domain)]
    if same_domain:
        return same_domain[0]

    if generic_emails:
        return generic_emails[0]

    return ""


def extract_phone(text: str) -> str:
    matches = re.findall(PHONE_REGEX, text)
    if not matches:
        return ""
    return clean_phone(matches[0])


def extract_city(text: str) -> str:
    text_lower = text.lower()
    for city in CITY_KEYWORDS:
        if city.lower() in text_lower:
            return city
    return ""


def scrape_company(company_name: str, website: str) -> dict:
    base_url = normalize_website(website)
    domain = get_domain(base_url)

    all_emails = set()
    phone = ""
    city = ""

    for path in CONTACT_PATHS:
        url = urljoin(base_url + "/", path.lstrip("/"))
        html = fetch_page(url)

        if not html:
            continue

        text = extract_text(html)

        all_emails.update(extract_emails(text))

        if not phone:
            phone = extract_phone(text)

        if not city:
            city = extract_city(text)

        time.sleep(SLEEP_SECONDS)

    best_email = pick_best_generic_email(list(all_emails), domain)

    return {
        "Company Name": company_name,
        "Website URL": base_url,
        "City": city,
        "Generic Corporate Email": best_email,
        "Corporate Phone Number": phone,
    }