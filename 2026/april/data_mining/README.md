# Data Mining Project – Contact Extraction & Cleaning

## Overview
This project simulates a real-world data mining task: extracting contact information from a source, cleaning it, and delivering a structured dataset ready for use.

The goal is not just to collect data, but to ensure it is accurate, consistent, and usable.

---

## Approach

The pipeline is structured in three steps:

1. **Extract**
   - Collect raw contact records from a source (API, web pages, or database)
   - Designed to handle large volumes of data

2. **Clean**
   - Remove duplicate records
   - Filter out incomplete entries (e.g., missing emails)
   - Standardize fields (name, email, phone)

3. **Load**
   - Export clean data into a CSV file
   - Ensure consistent formatting for downstream use

---


---

## How to Run

```bash
pip install -r requirements.txt
python main.py
```

save in:
output/clean_contacts.csv
