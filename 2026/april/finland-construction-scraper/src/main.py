import pandas as pd

from src.scraper import scrape_company
from src.utils import get_domain


def deduplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df["normalized_domain"] = df["Website URL"].apply(get_domain)
    df = df.drop_duplicates(subset=["normalized_domain"], keep="first")
    return df.drop(columns=["normalized_domain"])


def main() -> None:
    input_file = "data/input/seed_companies.csv"
    output_csv = "data/output/finland_construction_companies_poc.csv"
    output_xlsx = "data/output/finland_construction_companies_poc.xlsx"

    seed_df = pd.read_csv(input_file)

    results = []
    for _, row in seed_df.iterrows():
        result = scrape_company(
            company_name=row["company_name"],
            website=row["website"]
        )
        results.append(result)

    final_df = pd.DataFrame(results)
    final_df = deduplicate_rows(final_df)

    final_df.to_csv(output_csv, index=False)
    final_df.to_excel(output_xlsx, index=False)

    print(final_df)
    print(f"\nSaved: {output_csv}")
    print(f"Saved: {output_xlsx}")


if __name__ == "__main__":
    main()