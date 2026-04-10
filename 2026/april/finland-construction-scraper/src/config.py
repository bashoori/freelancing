HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FinlandConstructionScraper/1.0)"
}

GENERIC_PREFIXES = {
    "info",
    "contact",
    "office",
    "sales",
    "admin",
    "support",
    "hello",
    "team",
    "asiakaspalvelu",
    "toimisto",
    "myynti"
}

CONTACT_PATHS = [
    "",
    "/contact",
    "/contacts",
    "/about",
    "/company",
    "/yhteystiedot",
    "/ota-yhteytta"
]

CITY_KEYWORDS = [
    "Helsinki", "Espoo", "Tampere", "Vantaa", "Turku", "Oulu",
    "Lahti", "Kuopio", "Jyväskylä", "Pori", "Lappeenranta",
    "Vaasa", "Joensuu"
]

EMAIL_REGEX = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
PHONE_REGEX = r"(\+?\d[\d\s\-\(\)]{6,}\d)"
REQUEST_TIMEOUT = 15
SLEEP_SECONDS = 1