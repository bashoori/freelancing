import pandas as pd
import re

def is_valid_email(email):
    if not isinstance(email, str):
        return False
    email = email.strip()
    pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    return re.match(pattern, email) is not None

def normalize_phone(phone):
    if phone is None:
        return ""
    phone = str(phone).strip()
    if phone.upper() == "N/A":
        return ""
    digits = re.sub(r"\D", "", phone)
    return digits

def clean_data(raw_data):
    df = pd.DataFrame(raw_data)

    for col in ["name", "email", "phone", "city"]:
        if col not in df.columns:
            df[col] = ""

    df["name"] = df["name"].fillna("").astype(str).str.strip()
    df["email"] = df["email"].fillna("").astype(str).str.strip().str.lower()
    df["city"] = df["city"].fillna("").astype(str).str.strip()
    df["phone"] = df["phone"].apply(normalize_phone)

    df = df[df["email"].apply(is_valid_email)]

    df = df[df["name"] != ""]

    df = df.drop_duplicates(subset=["email"])

    df = df.reset_index(drop=True)

    return df

def build_summary(raw_data, clean_df):
    raw_count = len(raw_data)
    clean_count = len(clean_df)

    return {
        "total_raw_records": raw_count,
        "final_clean_records": clean_count,
        "removed_records": raw_count - clean_count
    }