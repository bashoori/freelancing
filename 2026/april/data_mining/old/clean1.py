import pandas as pd

def clean_data(raw_data):
    df = pd.DataFrame(raw_data)

    df = df.drop_duplicates()
    df = df.dropna(subset=["email"])
    df["phone"] = df["phone"].fillna("")
    df["name"] = df["name"].fillna("").str.strip()
    df["email"] = df["email"].fillna("").str.strip().str.lower()
    df["city"] = df["city"].fillna("").str.strip()

    return df

def build_summary(raw_data, clean_df):
    raw_count = len(raw_data)
    clean_count = len(clean_df)
    duplicates_and_invalid_removed = raw_count - clean_count

    return {
        "total_raw_records": raw_count,
        "final_clean_records": clean_count,
        "removed_records": duplicates_and_invalid_removed
    }