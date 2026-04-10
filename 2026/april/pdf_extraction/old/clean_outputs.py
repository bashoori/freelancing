from pathlib import Path
import re
import pandas as pd

OUTPUT_DIR = Path("output")

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
    "Wyoming",
]


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", str(value).strip())


def normalize_integer(value: str) -> str:
    value = normalize_whitespace(value)

    # OCR issue: 2.191 -> 2,191
    if re.fullmatch(r"\d+\.\d{3}", value):
        value = value.replace(".", ",")

    return re.sub(r"[^\d,]", "", value)


def normalize_decimal(value: str) -> str:
    value = normalize_whitespace(value)

    # 295 -> 29.5
    if re.fullmatch(r"\d{3}", value):
        return f"{value[:2]}.{value[2]}"

    # 29 4 -> 29.4
    if re.fullmatch(r"\d{2}\s\d", value):
        left, right = value.split()
        return f"{left}.{right}"

    # 37. 5 -> 37.5
    if re.fullmatch(r"\d+\.\s\d", value):
        left, right = value.split()
        return f"{left}{right}"

    return re.sub(r"[^\d.]", "", value)


def normalize_percent(value: str) -> str:
    value = normalize_whitespace(value)

    # 69 5% -> 69.5%
    if re.fullmatch(r"\d{2}\s\d%", value):
        left, right = value.split()
        return f"{left}.{right.replace('%', '')}%"

    # 37. 5% -> 37.5%
    if re.fullmatch(r"\d+\.\s\d%", value):
        left, right = value.split()
        return f"{left}{right}"

    # 606% -> 60.6%
    if re.fullmatch(r"\d{3}%", value):
        return f"{value[:2]}.{value[2]}%"

    return re.sub(r"[^\d.%]", "", value)


def normalize_school_name(value: str) -> str:
    value = normalize_whitespace(value)

    fixes = {
        "oe The University of West Alabama": "The University of West Alabama",
        "Si Troy University": "Troy University",
    }

    return fixes.get(value, value)


def normalize_jurisdiction_name(value: str) -> str:
    value = normalize_whitespace(value)

    fixes = {
        "indiana": "Indiana",
        "lowa": "Iowa",
        "mississipp\\": "Mississippi",
    }

    return fixes.get(value, value)


def ensure_review_flag_column(df: pd.DataFrame, default_value: str = "") -> pd.DataFrame:
    if "Review Flag" not in df.columns:
        df["Review Flag"] = default_value
    else:
        df["Review Flag"] = (
            df["Review Flag"]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace("", default_value)
        )
    return df


def reorder_columns(df: pd.DataFrame, preferred_order: list[str]) -> pd.DataFrame:
    existing_cols = [col for col in preferred_order if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in existing_cols]
    return df[existing_cols + remaining_cols]


def merge_flags(existing: str, generated: str, fallback: str = "") -> str:
    existing = normalize_whitespace(existing) if str(existing).strip() else ""
    generated = normalize_whitespace(generated) if str(generated).strip() else ""

    if existing and generated:
        return f"{existing};{generated}"
    if existing:
        return existing
    if generated:
        return generated
    return fallback


def build_state_review_flag(row: pd.Series) -> str:
    issues = []

    if row.get("Jurisdiction", "") not in KNOWN_JURISDICTIONS:
        issues.append("uncertain_jurisdiction")

    if "Pass Rate" in row.index:
        val = str(row.get("Pass Rate", "")).strip()
        if val and not val.endswith("%"):
            issues.append("bad_pass_rate")

    for field in ["Average Score", "Average Age"]:
        if field in row.index:
            val = str(row.get(field, "")).strip()
            if val and not re.fullmatch(r"\d+\.\d", val):
                issues.append(f"check_{field.lower().replace(' ', '_')}")

    return ";".join(sorted(set(issues)))


def build_university_review_flag(row: pd.Series) -> str:
    issues = []

    school_name = normalize_whitespace(row.get("School / University", ""))
    if not school_name:
        issues.append("missing_school_name")

    numeric_fields = [
        "Candidates Total",
        "Sections Total",
        "Sections FT",
        "Sections RE",
        "Candidates First-Time",
        "Candidates Repeat",
        "AUD Secs",
        "BEC Secs",
        "FAR Secs",
        "REG Secs",
    ]

    for field in numeric_fields:
        if field in row.index:
            value = str(row.get(field, "")).replace(",", "").strip()
            if value and not value.isdigit():
                issues.append(f"bad_{field.lower().replace(' ', '_')}")

    percent_fields = [
        "Pass Rate",
        "AUD Pass Rate",
        "BEC Pass Rate",
        "FAR Pass Rate",
        "REG Pass Rate",
    ]

    for field in percent_fields:
        if field in row.index:
            value = str(row.get(field, "")).strip()
            if value and not value.endswith("%"):
                issues.append(f"bad_{field.lower().replace(' ', '_')}")

    decimal_fields = [
        "Average Score",
        "Average Age",
        "AUD Score",
    ]

    for field in decimal_fields:
        if field in row.index:
            value = str(row.get(field, "")).strip()
            if value and not re.fullmatch(r"\d+\.\d", value):
                issues.append(f"check_{field.lower().replace(' ', '_')}")

    if "Value Count" in row.index:
        try:
            if int(row.get("Value Count", 0)) < 10:
                issues.append("low_confidence_row")
        except Exception:
            issues.append("low_confidence_row")

    return ";".join(sorted(set(issues)))


def clean_state_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy().fillna("")

    if "Jurisdiction" in df.columns:
        df["Jurisdiction"] = df["Jurisdiction"].apply(normalize_jurisdiction_name)
        df = df[df["Jurisdiction"].isin(KNOWN_JURISDICTIONS)]

    integer_cols = ["Candidates Total", "Sections Total", "Sections FT", "Sections RE"]
    for col in integer_cols:
        if col in df.columns:
            df[col] = df[col].apply(normalize_integer)

    if "Pass Rate" in df.columns:
        df["Pass Rate"] = df["Pass Rate"].apply(normalize_percent)

    decimal_cols = ["Average Score", "Average Age"]
    for col in decimal_cols:
        if col in df.columns:
            df[col] = df[col].apply(normalize_decimal)

    df = ensure_review_flag_column(df, default_value="")
    generated_flags = df.apply(build_state_review_flag, axis=1)

    df["Review Flag"] = [
        merge_flags(existing, generated)
        for existing, generated in zip(df["Review Flag"], generated_flags)
    ]

    df["Needs Review"] = df["Review Flag"] != ""

    preferred_order = [
        "Jurisdiction",
        "Candidates Total",
        "Sections Total",
        "Sections FT",
        "Sections RE",
        "Pass Rate",
        "Average Score",
        "Average Age",
        "Source Page",
        "Confidence",
        "Review Flag",
        "Needs Review",
    ]

    return reorder_columns(df.reset_index(drop=True), preferred_order)


def clean_university_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy().fillna("")

    if "School / University" in df.columns:
        df["School / University"] = df["School / University"].apply(normalize_school_name)
        df = df[df["School / University"] != ""]

    if "Raw OCR Line" in df.columns:
        df["Raw OCR Line"] = df["Raw OCR Line"].astype(str).str.strip()

    if "Value Count" in df.columns:
        df["Value Count"] = (
            pd.to_numeric(df["Value Count"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        df["Low Confidence Row"] = df["Value Count"] < 10
    else:
        df["Low Confidence Row"] = False

    integer_cols = [
        "Candidates Total",
        "Candidates First-Time",
        "Candidates Repeat",
        "Sections Total",
        "Sections FT",
        "Sections RE",
        "AUD Secs",
        "BEC Secs",
        "FAR Secs",
        "REG Secs",
    ]
    for col in integer_cols:
        if col in df.columns:
            df[col] = df[col].apply(normalize_integer)

    decimal_cols = [
        "Average Score",
        "Average Age",
        "AUD Score",
    ]
    for col in decimal_cols:
        if col in df.columns:
            df[col] = df[col].apply(normalize_decimal)

    percent_cols = [
        "Pass Rate",
        "AUD Pass Rate",
        "BEC Pass Rate",
        "FAR Pass Rate",
        "REG Pass Rate",
    ]
    for col in percent_cols:
        if col in df.columns:
            df[col] = df[col].apply(normalize_percent)

    # Keep rows that still carry a usable overall pass-rate signal
    if "Pass Rate" in df.columns:
        df = df[df["Pass Rate"].astype(str).str.contains("%", na=False)]

    df = ensure_review_flag_column(df, default_value="")
    generated_flags = df.apply(build_university_review_flag, axis=1)

    df["Review Flag"] = [
        merge_flags(existing, generated, fallback="manual_review")
        for existing, generated in zip(df["Review Flag"], generated_flags)
    ]

    df["Needs Review"] = df["Review Flag"] != ""

    preferred_order = [
        "School / University",
        "Candidates Total",
        "Sections Total",
        "Sections FT",
        "Sections RE",
        "Candidates First-Time",
        "Candidates Repeat",
        "Pass Rate",
        "Average Score",
        "Average Age",
        "AUD Secs",
        "AUD Score",
        "AUD Pass Rate",
        "BEC Secs",
        "BEC Pass Rate",
        "FAR Secs",
        "FAR Pass Rate",
        "REG Secs",
        "REG Pass Rate",
        "Value Count",
        "Low Confidence Row",
        "Source Page",
        "Confidence",
        "Review Flag",
        "Needs Review",
        "Raw OCR Line",
    ]

    return reorder_columns(df.reset_index(drop=True), preferred_order)


def clean_excel_file(input_path: Path, output_path: Path, cleaner_func) -> None:
    df = pd.read_excel(input_path)
    cleaned_df = cleaner_func(df)
    cleaned_df.to_excel(output_path, index=False)


def main():
    state_input = OUTPUT_DIR / "State_Level_Candidates_2020.xlsx"
    university_input = OUTPUT_DIR / "University_Level_Candidates_2020.xlsx"

    state_output = OUTPUT_DIR / "State_Level_Candidates_2020_Clean.xlsx"
    university_output = OUTPUT_DIR / "University_Level_Candidates_2020_Clean.xlsx"

    if state_input.exists():
        clean_excel_file(state_input, state_output, clean_state_dataframe)
        print(f"Saved cleaned state file: {state_output}")
    else:
        print(f"Missing file: {state_input}")

    if university_input.exists():
        clean_excel_file(university_input, university_output, clean_university_dataframe)
        print(f"Saved cleaned university file: {university_output}")
    else:
        print(f"Missing file: {university_input}")


if __name__ == "__main__":
    main()