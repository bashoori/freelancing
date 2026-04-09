from extract import extract_data
from clean import clean_data, build_summary
import os

def run_pipeline():
    raw_data = extract_data()
    clean_df = clean_data(raw_data)
    summary = build_summary(raw_data, clean_df)

    os.makedirs("output", exist_ok=True)
    clean_df.to_csv("output/clean_contacts.csv", index=False)

    print("Pipeline completed.")
    print("Output saved to: output/clean_contacts.csv")
    print("Summary:")
    for key, value in summary.items():
        print(f"{key}: {value}")

if __name__ == "__main__":
    run_pipeline()