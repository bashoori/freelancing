# Finland Construction Companies Scraper

Small proof of concept scraper for collecting public company-level contact data from Finnish construction company websites.

## Output fields

- Company Name
- Website URL
- City
- Generic Corporate Email
- Corporate Phone Number

## Project structure

- `data/input/` contains seed company websites
- `data/output/` contains exported CSV and Excel files
- `src/` contains the scraper logic

## Run

```bash
python -m src.main