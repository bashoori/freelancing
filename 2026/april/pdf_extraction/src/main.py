import time
from extract_candidates import main as extract_main
from clean_outputs import main as clean_main


def run_pipeline():
    start_time = time.time()

    print("\nStarting data pipeline...\n")

    # Step 1: Extraction
    print("Step 1: Extracting data from PDF...")
    extract_main()

    # Step 2: Cleaning
    print("\nStep 2: Cleaning extracted data...")
    clean_main()

    end_time = time.time()

    print("\nPipeline completed successfully")
    print(f"Total runtime: {round(end_time - start_time, 2)} seconds\n")


if __name__ == "__main__":
    run_pipeline()