from extract import extract_data
from clean import clean_data

def run_pipeline():
    raw_data = extract_data()
    clean_df = clean_data(raw_data)

    clean_df.to_csv("output/clean_contacts.csv", index=False)

    print("Pipeline completed. File saved.")

if __name__ == "__main__":
    run_pipeline()