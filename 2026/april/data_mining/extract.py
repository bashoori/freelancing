import json

def extract_data(file_path="sample_raw_data.json"):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)