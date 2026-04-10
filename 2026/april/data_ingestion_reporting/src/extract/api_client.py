from __future__ import annotations

from typing import Any

import requests


API_URL = "https://remoteok.com/api"


def fetch_jobs() -> list[dict[str, Any]]:
    """
    Fetch raw job data from RemoteOK API.
    """
    response = requests.get(
        API_URL,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise ValueError("Unexpected API response format. Expected a list.")

    return data