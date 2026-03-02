#!/usr/bin/env python3
"""
csvimport.py - Import and transform CSV files for multiple organizations, with flexible format mapping and duplicate removal support.

Usage:
    python csvimport.py \
        --input-files INPUT1.csv,INPUT2.csv \
        --org ORG \
        [ --output OUTPUT.csv ]\
        [ --input-format FORMAT ] \
        [ --output-format FORMAT ] \
        [ --config CONFIG ]

Options:
  --input-files INPUT1.csv,INPUT2.csv  Comma-separated list of input CSV files
  --output OUTPUT.csv                  Path to output (transformed) CSV file
  --input-format FORMAT                Input format specification (e.g., column order or names)
  --output-format FORMAT               Output format specification (e.g., column order or names)
  --config CONFIG                      Optional config file for organization-specific formats
  --org ORG                            Organization name (for config lookup)

"""


import argparse
import csv
import sys
import yaml
import logging
from typing import List, Dict, Optional
import os

# Google Sheets API imports
try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    gspread = None
    Credentials = None

def load_config(config_path: Optional[str]) -> Dict:
    if not config_path:
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_format(config: Dict, org: Optional[str], key: str, cli_format: Optional[List[str]]) -> Optional[List[str]]:
    if cli_format:
        return cli_format
    if org and config.get("organizations", {}).get(org, {}).get(key):
        return config["organizations"][org][key]
    return None


def parse_format(format_str: Optional[str]) -> Optional[List[str]]:
    if not format_str:
        return None
    # Accept comma-separated or YAML/JSON list
    if format_str.startswith("["):
        return yaml.safe_load(format_str)
    return [col.strip() for col in format_str.split(",")]

# --- Duplicate removal logic ---
def remove_duplicates(transformed_rows: List[Dict], existing_entries: List[Dict], key_columns: List[str], logger: logging.Logger) -> List[Dict]:
    logger.debug(f"Deduplication: key_columns={key_columns}")
    logger.debug(f"Input rows: {len(transformed_rows)}, Existing entries: {len(existing_entries)}")
    # Log sample key tuples for inspection
    for i, entry in enumerate(existing_entries[:5]):
        key = tuple(str(entry.get(col, "")) for col in key_columns)
        logger.debug(f"Sample existing key {i}: {key}")
    for i, row in enumerate(transformed_rows[:5]):
        key = tuple(str(row.get(col, "")) for col in key_columns)
        logger.debug(f"Sample input key {i}: {key}")
    existing_keys = set()
    for entry in existing_entries:
        key = tuple(str(entry.get(col, "")) for col in key_columns)
        existing_keys.add(key)
    result = []
    for row in transformed_rows:
        key = tuple(str(row.get(col, "")) for col in key_columns)
        if key not in existing_keys:
            result.append(row)
        else:
            logger.info(f"Duplicate found and removed: {key}")
    logger.info(f"Total duplicates removed: {len(transformed_rows) - len(result)}")
    return result

# --- Google Sheets integration ---
def fetch_sheet_entries(sheet_id: str, worksheet_name: str, creds_path: str, logger: logging.Logger) -> List[Dict]:
    if not gspread or not Credentials:
        logger.error("gspread or google-auth not installed. Cannot fetch Google Sheets entries.")
        raise ImportError("gspread and google-auth must be installed for Google Sheets integration.")
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    try:
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    except Exception as e:
        logger.error(f"Failed to load Google credentials file '{creds_path}': {e}")
        raise
    try:
        client = gspread.authorize(creds)
    except Exception as e:
        logger.error(f"Failed to authorize Google Sheets client: {e}")
        raise
    try:
        sheet = client.open_by_key(sheet_id)
    except Exception as e:
        logger.error(f"Failed to open Google Sheet with ID '{sheet_id}': {e}")
        raise
    try:
        worksheet = sheet.worksheet(worksheet_name)
    except Exception as e:
        logger.error(f"Failed to open worksheet '{worksheet_name}' in Google Sheet: {e}")
        raise
    try:
        rows = worksheet.get_all_records()
    except Exception as e:
        logger.error(f"Failed to fetch records from worksheet '{worksheet_name}': {e}")
        raise
    logger.info(f"Fetched {len(rows)} entries from Google Sheet '{worksheet_name}' (ID: {sheet_id})")
    # Always backup Google Sheet before update
    import csv, os, datetime
    backup_dir = os.path.join(os.getcwd(), "backups")
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"{worksheet_name}_backup_{timestamp}.csv")
    if rows:
        with open(backup_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        logger.info(f"Google Sheet backed up to: {backup_path}")
    else:
        logger.info(f"No rows to backup from Google Sheet '{worksheet_name}'")
    return rows

# --- CSV transformation ---
def transform_csv(input_path: str, output_path: str, input_format: List[str], output_format: List[str], existing_entries: Optional[List[Dict]] = None, key_columns: Optional[List[str]] = None, logger: Optional[logging.Logger] = None):
    with open(input_path, "r", encoding="utf-8-sig") as infile:
        reader = csv.DictReader(infile)
        transformed_rows = []
        for row in reader:
            # Special transform rules for excepted org: split Amount into Debit/Credit
            if (
                'Debit' in output_format and 'Credit' in output_format and 'Amount' in input_format and 'Credit Debit Indicator' in input_format
            ):
                debit = row['Amount'] if row.get('Credit Debit Indicator') == 'Debit' else ''
                credit = row['Amount'] if row.get('Credit Debit Indicator') == 'Credit' else ''
                new_row = {}
                for col in output_format:
                    if col == 'Debit':
                        new_row['Debit'] = debit
                    elif col == 'Credit':
                        new_row['Credit'] = credit
                    elif col == 'Posting Date':
                        # Use Posting Date from input
                        new_row['Posting Date'] = row.get('Posting Date', '')
                    else:
                        new_row[col] = row.get(col, '')
            else:
                new_row = {col: row.get(col, "") for col in output_format}
            transformed_rows.append(new_row)
    # Remove duplicates if existing_entries and key_columns are provided
    if existing_entries and key_columns and logger:
        transformed_rows = remove_duplicates(transformed_rows, existing_entries, key_columns, logger)
    return transformed_rows

# --- Logging setup ---
def setup_logging(debug: bool, log_file: str = "csvimport.log") -> logging.Logger:
    logger = logging.getLogger("csvimport")
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.debug_mode = debug
    for h in list(logger.handlers):
        logger.removeHandler(h)
    import os
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    # Ensure parent directory exists for log file
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    if debug:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

# --- Main CLI ---
def main():
    parser = argparse.ArgumentParser(description="Import and transform CSV files for multiple organizations.")
    parser.add_argument("--input-files", required=True, help="Comma-separated list of input CSV files")
    parser.add_argument("--output", required=False, help="Optional path to output CSV file (for debug/troubleshooting)")
    parser.add_argument("--input-format", help="Input format (comma-separated or YAML/JSON list)")
    parser.add_argument("--output-format", help="Output format (comma-separated or YAML/JSON list)")
    parser.add_argument("--config", help="Optional config file for organization formats (default: confs/csvimport.conf)")
    parser.add_argument("--org", help="Organization name for config lookup")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging to STDOUT")
    parser.add_argument("--log-file", default="logs/csvimport.log", help="Log file path (default: logs/csvimport.log)")
    parser.add_argument("--existing-csv", help="Path to CSV file with existing entries for duplicate removal")
    parser.add_argument("--existing-sheet-id", help="Google Sheet ID for existing entries (for duplicate removal)")
    parser.add_argument("--existing-sheet-name", help="Worksheet name in Google Sheet for existing entries (deprecated, use --sheet-name)")
    parser.add_argument("--sheet-name", help="Worksheet name to use for Google Sheet operations (overrides config)")
    parser.add_argument("--google-creds", help="Path to Google service account credentials JSON file")
    parser.add_argument("--key-columns", help="Comma-separated list of columns to use for duplicate detection")
    args = parser.parse_args()

    logger = setup_logging(args.debug, args.log_file)
    input_files = [f.strip() for f in args.input_files.split(",")]
    logger.info(f"Starting csvimport for input files: {input_files}, output: {args.output}")

    # Default config path if not specified
    config_path = args.config if args.config else "confs/csvimport.conf"
    config = load_config(config_path)
    input_format = get_format(config, args.org, "input_format", parse_format(args.input_format))
    output_format = get_format(config, args.org, "output_format", parse_format(args.output_format))

    # --- Google integration: get creds, sheet id, sheet name from CLI, config, or env ---
    def get_param(cli_val, config_dict, config_key, env_var):
        if cli_val:
            return cli_val
        if config_dict and config_key in config_dict:
            return config_dict[config_key]
        return os.environ.get(env_var)

    # Config structure example:
    # google:
    #   creds: /path/to/creds.json
    #   sheet_id: ...
    #   sheet_name: ...
    google_config = config.get('google', {}) if config else {}
    sheet_id = get_param(args.existing_sheet_id, google_config, 'sheet_id', 'GOOGLE_SHEET_ID')
    # Determine sheet_name: CLI > org config > global config > env
    org_config = config.get('organizations', {}).get(args.org, {}) if config and args.org else {}
    sheet_name = args.sheet_name or org_config.get('sheet_name') or get_param(args.existing_sheet_name, google_config, 'sheet_name', 'GOOGLE_SHEET_NAME')
    creds_path = get_param(args.google_creds, google_config, 'creds', 'GOOGLE_CREDS')

    if not input_format or not output_format:
        logger.error("Input and output formats must be specified via CLI or config.")
        print("Error: Input and output formats must be specified via CLI or config.", file=sys.stderr)
        sys.exit(2)

    if input_format != output_format:
        logger.info(f"Transforming CSV with input format: {input_format} and output format: {output_format}")
    existing_entries = None
    key_columns = None
    org_config = config.get('organizations', {}).get(args.org, {}) if config and args.org else {}
    if args.key_columns:
        key_columns = [col.strip() for col in args.key_columns.split(",")]
    elif org_config.get('key_fields'):
        key_columns = [str(col).strip() for col in org_config['key_fields']]
        logger.info(f"Using key_fields from config for organization '{args.org}': {key_columns}")
    # Support duplicate removal from CSV or Google Sheet
    if key_columns:
        if args.existing_csv:
            with open(args.existing_csv, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                existing_entries = [row for row in reader]
            logger.info(f"Loaded {len(existing_entries)} existing entries from CSV for duplicate removal.")
        elif sheet_id and sheet_name and creds_path:
            try:
                existing_entries = fetch_sheet_entries(sheet_id, sheet_name, creds_path, logger)
            except Exception as e:
                logger.error(f"Failed to fetch Google Sheet entries: {e}")
                print(f"Error: Failed to fetch Google Sheet entries: {e}", file=sys.stderr)
                print("Troubleshooting tips:", file=sys.stderr)
                print("- Check that your credentials file is a valid Google service account JSON.", file=sys.stderr)
                print("- Ensure the file path, sheet ID, and worksheet name are correct.", file=sys.stderr)
                print("- Make sure the service account has access to the target sheet.", file=sys.stderr)
                sys.exit(3)
        else:
            logger.info("No existing entries source provided for duplicate removal.")
    # Always deduplicate, even if formats are the same
    # Transform and deduplicate rows
    if input_format == output_format:
        rows = []
        for input_path in input_files:
            with open(input_path, "r", encoding="utf-8-sig") as infile:
                reader = csv.DictReader(infile)
                rows.extend([row for row in reader])
        if existing_entries and key_columns:
            deduped_rows = remove_duplicates(rows, existing_entries, key_columns, logger)
        else:
            deduped_rows = rows
    else:
        # For transformation, merge all input files before processing
        all_rows = []
        for input_path in input_files:
            with open(input_path, "r", encoding="utf-8-sig") as infile:
                reader = csv.DictReader(infile)
                all_rows.extend([row for row in reader])
        # Write merged rows to a temp file for transform_csv
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w+", delete=False, newline="", encoding="utf-8") as temp_in:
            writer = csv.DictWriter(temp_in, fieldnames=input_format)
            writer.writeheader()
            for row in all_rows:
                extra_fields = set(row.keys()) - set(input_format)
                if extra_fields:
                    print(
                        f"Error: CSV contains fields not in input_format: {', '.join(sorted(extra_fields))}\n"
                        f"Check the 'input_format' list for org '{args.org}' in your config file ({args.config}).",
                        file=sys.stderr
                    )
                    sys.exit(2)
                writer.writerow(row)
            temp_in_path = temp_in.name
        deduped_rows = transform_csv(temp_in_path, args.output, input_format, output_format, existing_entries, key_columns, logger)

    # Google Sheets integration: append deduplicated data and sort
    if sheet_name and sheet_id and creds_path:
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            creds = Credentials.from_service_account_file(creds_path, scopes=[
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ])
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(sheet_id)
            worksheet = sh.worksheet(sheet_name)
            # Support extra columns from org config
            extra_columns = org_config.get('extra_columns', [])
            rows_to_insert = []
            for row in deduped_rows:
                base_row = [row.get(col, '') for col in output_format]
                full_row = base_row + list(extra_columns)
                rows_to_insert.append(full_row)
            if rows_to_insert:
                worksheet.insert_rows(rows_to_insert, row=2, value_input_option='USER_ENTERED')
                logger.info(f"Deduplicated data inserted at top of Google Sheet '{sheet_name}'. ({len(rows_to_insert)} rows)")
            else:
                logger.info(f"No new rows to insert into Google Sheet '{sheet_name}'.")
            # Reverse sort by column A (descending)
            worksheet.sort((1, 'des'))
            logger.info(f"Sheet '{sheet_name}' sorted by column A descending.")
            print(f"Deduplicated data appended and sorted in Google Sheet '{sheet_name}'.")
        except Exception as e:
            logger.error(f"Failed to append/sort data in Google Sheet: {e}")
            print(f"Error: Failed to append/sort data in Google Sheet: {e}", file=sys.stderr)
            sys.exit(4)
    elif getattr(args, 'output', None):
        # Fallback: Write deduplicated data to output CSV only if --output is provided
        with open(args.output, "w", encoding="utf-8", newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=output_format)
            writer.writeheader()
            for row in deduped_rows:
                extra_fields = set(row.keys()) - set(output_format)
                if extra_fields:
                    print(
                        f"Error: Row contains fields not in output_format: {', '.join(sorted(extra_fields))}\n"
                        f"Check the 'output_format' list for org '{args.org}' in your config file ({args.config}).",
                        file=sys.stderr
                    )
                    sys.exit(2)
                writer.writerow(row)
        logger.info(f"Deduplicated data written to {args.output}.")
        print(f"Deduplicated data written to {args.output}.")

if __name__ == "__main__":
    main()
