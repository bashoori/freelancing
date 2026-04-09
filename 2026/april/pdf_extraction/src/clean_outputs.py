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

    if re.fullmatch(r"\d+\.\d{3}", value):
        value = value.replace(".", ",")

    return re.sub(r"[^\d,]", "", value)


def normalize_decimal(value: str) -> str:
    value = str(value).strip()

    if re.fullmatch(r"\d{3}", value):
        return f"{value[:2]}.{value[2]}"

    if re.fullmatch(r"\d+\s+\d", value):
        left, right = value.split()
        return f"{left}.{right}"

    value = re.sub(r"[^\d.]", "", value)
    return value


def normalize_percent(value: str) -> str:
    value = str(value).strip()

    if re.fullmatch(r"\d{3}%", value):
        return f"{value[:2]}.{value[2]}%"

    return re.sub(r"[^\d.%]", "", value)


def clean_state_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

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

    if "Review Flag" in df.columns:
        df["Review Flag"] = df["Review Flag"].fillna("")

    return df.reset_index(drop=True)


def clean_university_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    if "Pass Rate" in df.columns:
        df = df[df["Pass Rate"].astype(str).str.contains("%", na=False)]

    if "Value Count" in df.columns:
        df = df[df["Value Count"].fillna(0).astype(int) >= 5]

    for col in ["Candidates Total", "Candidates First-Time", "Candidates Repeat"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_integer)

    if "Pass Rate" in df.columns:
        df["Pass Rate"] = df["Pass Rate"].apply(normalize_percent)

    for col in ["Average Age", "AUD Score"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_decimal)

    if "Review Flag" in df.columns:
        df["Review Flag"] = df["Review Flag"].fillna("manual_review")

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