from pathlib import Path
import re
import pandas as pd
from pdf2image import convert_from_path
import pytesseract

INPUT_FILE = Path("input/2020_NASBA_Performance_Book.pdf")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

STATE_PAGES = {1, 2}
TOTAL_PAGES = 2

KNOWN_JURISDICTIONS = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia",
    "Guam", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
    "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
    "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
    "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico",
    "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas",
    "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin",
    "Wyoming"
]


def clean_text(text: str) -> str:
    if text is None:
        return ""
    text = text.replace("\n", " ").strip()
    return re.sub(r"\s+", " ", text)


def year_from_filename(filename: str) -> str:
    match = re.search(r"(20\d{2})", filename)
    return match.group(1) if match else "UNKNOWN"


def is_number_like(value: str) -> bool:
    value = value.strip().replace(",", "").replace(".", "")
    return value.isdigit()


def normalize_integer(value: str) -> str:
    value = value.strip()

    # 2.191 -> 2,191
    if re.fullmatch(r"\d+\.\d{3}", value):
        return value.replace(".", ",")

    return value


def normalize_decimal(value: str) -> str:
    value = value.strip()

    # 295 -> 29.5
    if re.fullmatch(r"\d{3}", value):
        return f"{value[:2]}.{value[2]}"

    return value


def normalize_percent(value: str) -> str:
    value = value.strip()

    # 969% -> 96.9%
    if re.fullmatch(r"\d{3}%", value):
        return f"{value[:2]}.{value[2]}%"

    # 713% is probably 71.3%
    if re.fullmatch(r"\d{3}%", value):
        return f"{value[:2]}.{value[2]}%"

    return value


def crop_table_area(image):
    width, height = image.size
    left = int(width * 0.03)
    top = int(height * 0.10)
    right = int(width * 0.98)
    bottom = int(height * 0.98)
    return image.crop((left, top, right, bottom))


def render_single_page(pdf_path: Path, page_num: int):
    images = convert_from_path(
        pdf_path,
        first_page=page_num,
        last_page=page_num,
        dpi=300,
        grayscale=True,
    )
    return images[0] if images else None


def ocr_page(image) -> str:
    return pytesseract.image_to_string(image, config="--psm 6")


def save_debug_text(page_num: int, text: str) -> None:
    debug_dir = OUTPUT_DIR / "debug_text"
    debug_dir.mkdir(exist_ok=True)
    (debug_dir / f"page_{page_num}.txt").write_text(text, encoding="utf-8")


def recover_jurisdiction_name(raw_name: str) -> str:
    raw_name = clean_text(raw_name)
    raw_lower = raw_name.lower()

    direct_fixes = {
        "indiana": "Indiana",
        "lowa": "Iowa",
        "mississipp\\": "Mississippi",
        "braska": "Nebraska",
        "yada": "Nevada",
        "w hampshire": "New Hampshire",
        "w jersey": "New Jersey",
        "w mexico": "New Mexico",
        "ww york": "New York",
        "rth carolina": "North Carolina",
        "rth dakota": "North Dakota",
        "10": "Ohio",
        "lahoma": "Oklahoma",
        "egon": "Oregon",
        "nnsylvania": "Pennsylvania",
        "lerto rico": "Puerto Rico",
        "1ode island": "Rhode Island",
        "uth carolina": "South Carolina",
        "uth dakota": "South Dakota",
        "nnessee": "Tennessee",
        "xxas": "Texas",
        "xas": "Texas",
        "an": "Utah",
        "rmont": "Vermont",
        "rginia": "Virginia",
        "ashington": "Washington",
        "est virginia": "West Virginia",
        "isconsin": "Wisconsin",
        "yoming": "Wyoming",
    }

    if raw_lower in direct_fixes:
        return direct_fixes[raw_lower]

    for name in KNOWN_JURISDICTIONS:
        if raw_lower == name.lower():
            return name

    suffix_matches = [
        name for name in KNOWN_JURISDICTIONS
        if name.lower().endswith(raw_lower)
    ]
    if len(suffix_matches) == 1:
        return suffix_matches[0]

    contains_matches = [
        name for name in KNOWN_JURISDICTIONS
        if raw_lower in name.lower()
    ]
    if len(contains_matches) == 1:
        return contains_matches[0]

    return raw_name.title()


def extract_values_after_name(parts: list[str], percent_index: int) -> list[str]:
    values = []
    i = 0

    # skip name tokens
    while i < len(parts) and not is_number_like(parts[i]):
        i += 1

    while i < len(parts):
        token = parts[i]

        # merge split decimals like 29 4 -> 29.4
        if (
            i + 1 < len(parts)
            and is_number_like(parts[i])
            and is_number_like(parts[i + 1])
            and re.fullmatch(r"\d{2}", parts[i])
            and re.fullmatch(r"\d", parts[i + 1])
        ):
            values.append(f"{parts[i]}.{parts[i + 1]}")
            i += 2
            continue

        if is_number_like(token) or "%" in token:
            values.append(token)

        i += 1

    return values


def row_needs_review(row: dict) -> str:
    issues = []

    for field in ["Average Score", "Average Age", "Pass Rate"]:
        value = str(row.get(field, ""))
        if re.search(r"[A-Za-z]", value):
            issues.append(f"bad_{field.lower().replace(' ', '_')}")

    if row["Jurisdiction"] not in KNOWN_JURISDICTIONS:
        issues.append("uncertain_jurisdiction")

    if not str(row["Pass Rate"]).endswith("%"):
        issues.append("invalid_pass_rate")

    return ";".join(issues) if issues else ""


def parse_state_lines(text: str, page_num: int) -> list[dict]:
    rows = []
    lines = [clean_text(line) for line in text.splitlines() if clean_text(line)]

    for line in lines:
        line_lower = line.lower()

        if "%" not in line:
            continue

        if "jurisdiction" in line_lower or "summary" in line_lower:
            continue

        parts = line.split()
        if not parts:
            continue

        percent_index = None
        for i, part in enumerate(parts):
            if "%" in part:
                percent_index = i
                break

        if percent_index is None:
            continue

        if percent_index < 5:
            continue

        jurisdiction_parts = []
        for p in parts:
            if is_number_like(p):
                break
            jurisdiction_parts.append(p)

        jurisdiction_raw = " ".join(jurisdiction_parts).strip()
        if not jurisdiction_raw:
            continue

        values = extract_values_after_name(parts, percent_index)

        if len(values) < 7:
            continue

        row = {
            "Jurisdiction": recover_jurisdiction_name(jurisdiction_raw),
            "Candidates Total": normalize_integer(values[0]),
            "Sections Total": normalize_integer(values[1]),
            "Sections FT": normalize_integer(values[2]),
            "Sections RE": normalize_integer(values[3]),
            "Pass Rate": normalize_percent(values[4]),
            "Average Score": normalize_decimal(values[5]),
            "Average Age": normalize_decimal(values[6]),
            "Source Page": page_num,
            "Confidence": "OCR",
        }

        row["Review Flag"] = row_needs_review(row)
        rows.append(row)

    return rows


def deduplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    return df.drop_duplicates(
        subset=["Jurisdiction", "Candidates Total", "Sections Total", "Sections FT", "Sections RE"],
        keep="first"
    ).reset_index(drop=True)


def main():
    if not INPUT_FILE.exists():
        print(f"Missing file: {INPUT_FILE}")
        return

    year = year_from_filename(INPUT_FILE.name)
    state_rows = []

    for page_num in range(1, TOTAL_PAGES + 1):
        if page_num not in STATE_PAGES:
            continue

        print(f"Rendering page {page_num}...")
        image = render_single_page(INPUT_FILE, page_num)

        if image is None:
            print(f"  Could not render page {page_num}")
            continue

        image = crop_table_area(image)

        print(f"  OCR page {page_num}...")
        text = ocr_page(image)
        save_debug_text(page_num, text)

        parsed_rows = parse_state_lines(text, page_num)
        state_rows.extend(parsed_rows)
        print(f"  State rows found: {len(parsed_rows)}")

        del image

    state_df = pd.DataFrame(state_rows)
    state_df = deduplicate_rows(state_df)

    state_output = OUTPUT_DIR / f"State_Level_Candidates_{year}.xlsx"
    state_df.to_excel(state_output, index=False)

    print()
    print(f"Saved: {state_output}")
    print(f"State rows: {len(state_df)}")
    flagged = state_df['Review Flag'].astype(bool).sum() if not state_df.empty else 0
    print(f"Rows needing review: {flagged}")


if __name__ == "__main__":
    main()