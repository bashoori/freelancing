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
    "Wyoming"
]


def normalize_integer(value: str) -> str:
    value = str(value).strip()

    # 2.191 -> 2,191
    if re.fullmatch(r"\d+\.\d{3}", value):
        value = value.replace(".", ",")

    return re.sub(r"[^\d,]", "", value)


def normalize_decimal(value: str) -> str:
    value = str(value).strip()
    value = re.sub(r"\s+", " ", value)

    # 295 -> 29.5
    if re.fullmatch(r"\d{3}", value):
        return f"{value[:2]}.{value[2]}"

    # 29 4 -> 29.4
    if re.fullmatch(r"\d{2}\s\d", value):
        left, right = value.split()
        return f"{left}.{right}"

    # keep only digits and decimal point
    return re.sub(r"[^\d.]", "", value)


def normalize_percent(value: str) -> str:
    value = str(value).strip()
    value = re.sub(r"\s+", " ", value)

    # 69 5% -> 69.5%
    if re.fullmatch(r"\d{2}\s\d%", value):
        left, right = value.split()
        return f"{left}.{right.replace('%', '')}%"

    # 37. 5% -> 37.5%
    if re.fullmatch(r"\d{2}\.\s\d%", value):
        left, right = value.split()
        return f"{left}{right}"

    # 606% -> 60.6%
    if re.fullmatch(r"\d{3}%", value):
        return f"{value[:2]}.{value[2]}%"

    return re.sub(r"[^\d.%]", "", value)


def normalize_school_name(value: str) -> str:
    value = str(value).strip()
    value = re.sub(r"\s+", " ", value)

    fixes = {
        "oe The University of West Alabama": "The University of West Alabama",
        "Si Troy University": "Troy University",
    }

    return fixes.get(value, value)


def clean_state_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy().fillna("")

    if "Jurisdiction" in df.columns:
        df["Jurisdiction"] = df["Jurisdiction"].astype(str).str.strip()

        fixes = {
            "indiana": "Indiana",
            "lowa": "Iowa",
            "mississipp\\": "Mississippi",
        }
        df["Jurisdiction"] = df["Jurisdiction"].replace(fixes)

        df = df[df["Jurisdiction"].isin(KNOWN_JURISDICTIONS)]

    for col in ["Candidates Total", "Sections Total", "Sections FT", "Sections RE"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_integer)

    if "Pass Rate" in df.columns:
        df["Pass Rate"] = df["Pass Rate"].apply(normalize_percent)

    for col in ["Average Score", "Average Age"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_decimal)

    if "Review Flag" not in df.columns:
        df["Review Flag"] = ""
    else:
        df["Review Flag"] = df["Review Flag"].astype(str).fillna("").str.strip()

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

    existing_cols = [col for col in preferred_order if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in existing_cols]
    df = df[existing_cols + remaining_cols]

    return df.reset_index(drop=True)


def build_university_review_flag(row: pd.Series) -> str:
    issues = []

    school_name = str(row.get("School / University", "")).strip()
    if not school_name:
        issues.append("missing_school_name")

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

    return ";".join(sorted(set(issues)))


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
        df["Value Count"] = pd.to_numeric(df["Value Count"], errors="coerce").fillna(0).astype(int)
        df = df[df["Value Count"] >= 7]

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

    if "Pass Rate" in df.columns:
        df = df[df["Pass Rate"].astype(str).str.contains("%", na=False)]

    if "Review Flag" not in df.columns:
        df["Review Flag"] = ""
    else:
        df["Review Flag"] = df["Review Flag"].astype(str).fillna("").str.strip()

    generated_flags = df.apply(build_university_review_flag, axis=1)

    def merge_flags(existing: str, generated: str) -> str:
        existing = str(existing).strip()
        generated = str(generated).strip()

        if existing and generated:
            return f"{existing};{generated}"
        if existing:
            return existing
        if generated:
            return generated
        return "manual_review"

    df["Review Flag"] = [
        merge_flags(existing, generated)
        for existing, generated in zip(df["Review Flag"], generated_flags)
    ]

    df["Needs Review"] = True

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
        "Source Page",
        "Confidence",
        "Review Flag",
        "Needs Review",
        "Raw OCR Line",
    ]

    existing_cols = [col for col in preferred_order if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in existing_cols]
    df = df[existing_cols + remaining_cols]

    return df.reset_index(drop=True)


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