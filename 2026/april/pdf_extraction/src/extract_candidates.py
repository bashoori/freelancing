from pathlib import Path
import re
import pandas as pd
from pdf2image import convert_from_path
import pytesseract

INPUT_FILE = Path("input/2020_NASBA_Performance_Book.pdf")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

STATE_PAGES = {1, 2}
UNIVERSITY_START_PAGE = 3
TOTAL_PAGES = 2


def clean_text(text: str) -> str:
    if text is None:
        return ""
    text = text.replace("\n", " ").strip()
    return re.sub(r"\s+", " ", text)


def year_from_filename(filename: str) -> str:
    match = re.search(r"(20\d{2})", filename)
    return match.group(1) if match else "UNKNOWN"


def rotate_if_needed(image, page_num: int):
    if page_num >= UNIVERSITY_START_PAGE:
        return image.rotate(90, expand=True)
    return image


def ocr_page(image) -> str:
    return pytesseract.image_to_string(image, config="--psm 6")


def save_debug_text(page_num: int, text: str) -> None:
    debug_dir = OUTPUT_DIR / "debug_text"
    debug_dir.mkdir(exist_ok=True)
    (debug_dir / f"page_{page_num}.txt").write_text(text, encoding="utf-8")


def parse_state_lines(text: str) -> list[dict]:
    rows = []
    lines = [clean_text(line) for line in text.splitlines() if clean_text(line)]

    for line in lines:
        if "%" not in line:
            continue

        parts = line.split()

        if len(parts) < 8:
            continue

        percent_index = None
        for i, part in enumerate(parts):
            if "%" in part:
                percent_index = i
                break

        if percent_index is None:
            continue

        # We expect:
        # jurisdiction + 4 numeric fields + pass_rate + avg_score + avg_age
        if percent_index < 5:
            continue

        try:
            jurisdiction = " ".join(parts[:percent_index - 4])
            candidates_total = parts[percent_index - 4]
            sections_total = parts[percent_index - 3]
            sections_ft = parts[percent_index - 2]
            sections_re = parts[percent_index - 1]
            pass_rate = parts[percent_index]
            avg_score = parts[percent_index + 1]
            avg_age = parts[percent_index + 2]

            if not jurisdiction.strip():
                continue

            # reject obvious garbage lines
            if any(char.isdigit() for char in jurisdiction) and len(jurisdiction.split()) < 1:
                continue

            rows.append(
                {
                    "Jurisdiction": jurisdiction,
                    "Candidates Total": candidates_total,
                    "Sections Total": sections_total,
                    "Sections FT": sections_ft,
                    "Sections RE": sections_re,
                    "Pass Rate": pass_rate,
                    "Average Score": avg_score,
                    "Average Age": avg_age,
                }
            )
        except Exception:
            continue

    return rows


def parse_university_lines(text: str) -> list[dict]:
    rows = []
    lines = [clean_text(line) for line in text.splitlines() if clean_text(line)]

    pattern = re.compile(
        r"^(?P<school>.+?)\s+"
        r"(?P<candidates_total>[\d,]+)\s+"
        r"(?P<candidates_first>[\d,]+)\s+"
        r"(?P<candidates_repeat>[\d,]+)\s+"
        r"(?P<pass_rate>[\d.]+%)\s+"
        r"(?P<avg_age>[\d.]+)\s+"
        r"(?P<aud_score>[\d.]+)\s+(?P<aud_pass>[\d.]+%)\s+"
        r"(?P<bec_score>[\d.]+)\s+(?P<bec_pass>[\d.]+%)\s+"
        r"(?P<far_score>[\d.]+)\s+(?P<far_pass>[\d.]+%)\s+"
        r"(?P<reg_score>[\d.]+)\s+(?P<reg_pass>[\d.]+%)$"
    )

    for line in lines:
        m = pattern.match(line)
        if m:
            rows.append(
                {
                    "School / University": m.group("school"),
                    "Candidates Total": m.group("candidates_total"),
                    "Candidates First-Time": m.group("candidates_first"),
                    "Candidates Repeat": m.group("candidates_repeat"),
                    "Pass Rate": m.group("pass_rate"),
                    "Average Age": m.group("avg_age"),
                    "AUD Score": m.group("aud_score"),
                    "AUD Pass Rate": m.group("aud_pass"),
                    "BEC Score": m.group("bec_score"),
                    "BEC Pass Rate": m.group("bec_pass"),
                    "FAR Score": m.group("far_score"),
                    "FAR Pass Rate": m.group("far_pass"),
                    "REG Score": m.group("reg_score"),
                    "REG Pass Rate": m.group("reg_pass"),
                }
            )

    return rows


def render_single_page(pdf_path: Path, page_num: int):
    images = convert_from_path(
        pdf_path,
        first_page=page_num,
        last_page=page_num,
        dpi=120,
        grayscale=True,
    )
    return images[0] if images else None


def main():
    if not INPUT_FILE.exists():
        print(f"Missing file: {INPUT_FILE}")
        return

    year = year_from_filename(INPUT_FILE.name)
    state_rows = []
    university_rows = []

    for page_num in range(1, TOTAL_PAGES + 1):
        print(f"Rendering page {page_num}...")
        image = render_single_page(INPUT_FILE, page_num)

        if image is None:
            print(f"  Could not render page {page_num}")
            continue

        image = rotate_if_needed(image, page_num)

        print(f"  OCR page {page_num}...")
        text = ocr_page(image)
        save_debug_text(page_num, text)

        if page_num in STATE_PAGES:
            parsed_rows = parse_state_lines(text)
            for row in parsed_rows:
                row["Source Page"] = page_num
            state_rows.extend(parsed_rows)
            print(f"  State rows found: {len(parsed_rows)}")
        else:
            parsed_rows = parse_university_lines(text)
            for row in parsed_rows:
                row["Source Page"] = page_num
            university_rows.extend(parsed_rows)
            print(f"  University rows found: {len(parsed_rows)}")

        del image

    state_df = pd.DataFrame(state_rows)
    university_df = pd.DataFrame(university_rows)

    state_output = OUTPUT_DIR / f"State_Level_Candidates_{year}.xlsx"
    university_output = OUTPUT_DIR / f"University_Level_Candidates_{year}.xlsx"

    state_df.to_excel(state_output, index=False)
    university_df.to_excel(university_output, index=False)

    print()
    print(f"Saved: {state_output}")
    print(f"Saved: {university_output}")
    print(f"State rows: {len(state_df)}")
    print(f"University rows: {len(university_df)}")


if __name__ == "__main__":
    main()