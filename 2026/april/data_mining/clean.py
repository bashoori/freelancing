import pandas as pd

def clean_data(raw_data):
    df = pd.DataFrame(raw_data)

    df = df.drop_duplicates()
    df = df.dropna(subset=["email"])
    df["phone"] = df["phone"].fillna("")

    return df