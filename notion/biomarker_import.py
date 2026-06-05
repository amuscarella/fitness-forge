#!/usr/bin/env python3
"""
Bloodwork CSV → Notion Importer
Usage:
    python import_bloodwork.py --csv My_main_table_cat_sorted.csv --token secret_xxx --db_id 1a2b3c4d5e6f7g8h9i0j
"""

import argparse
import re
from datetime import datetime
import pandas as pd
from notion_client import Client
from dateutil import parser as date_parser

def parse_reference_range(ref_str):
    """Parse reference range string into (low, high) tuple."""
    if not ref_str or pd.isna(ref_str):
        return None, None
    ref_str = str(ref_str).strip()
    if ref_str.upper() in ['TBD', 'NEGATIVE', '']:
        return None, None

    # Handle upper limit only: <x or ≤x
    match = re.match(r'[≤<]\s*([\d.]+)', ref_str)
    if match:
        return None, float(match.group(1))

    # Handle range: x - y
    match = re.match(r'([\d.]+)\s*[-–]\s*([\d.]+)', ref_str)
    if match:
        return float(match.group(1)), float(match.group(2))

    return None, None


def parse_value(val):
    """Convert pretty_value to float or None."""
    if pd.isna(val):
        return None
    val_str = str(val).strip().replace(',', '')
    if val_str.lower() == 'negative':
        return None
    try:
        return float(val_str)
    except ValueError:
        return None


def determine_status(value, low, high, bettertobe):
    """Determine High / Normal / Low status."""
    if value is None:
        return "TBD"
    if low is not None and value < low:
        return "Low"
    if high is not None and value > high:
        return "High"
    return "Normal"


def import_bloodwork(csv_path: str, notion_token: str, database_id: str):
    client = Client(auth=notion_token)
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows from CSV")

    success = 0
    errors = 0

    for idx, row in df.iterrows():
        try:
            value = parse_value(row['pretty_value'])
            low, high = parse_reference_range(row['reference_range'])
            status = determine_status(value, low, high, row.get('bettertobe'))

            # Parse date (handles "4/30 2026" and "10/28 2025")
            date_str = str(row['collection_date']).strip()
            if ' ' in date_str and not date_str.endswith(('2025', '2026', '2027')):
                date_str += " 2026"  # fallback
            parsed_date = date_parser.parse(date_str).date()

            # Build properties matching your Notion Bloodwork database
            properties = {
                "Date": {"date": {"start": parsed_date.isoformat()}},
                "Biomarker": {"title": [{"text": {"content": str(row['display_name'])}}]},
                "Your Value": {"number": value} if value is not None else {"number": None},
                "Unit": {"rich_text": [{"text": {"content": str(row['unit']) if pd.notna(row['unit']) else ""}}]},
                "Ref Low": {"number": low} if low is not None else {"number": None},
                "Ref High": {"number": high} if high is not None else {"number": None},
                "Status": {"select": {"name": status}},
                "Notes": {"rich_text": [{"text": {"content": f"Category: {row.get('category', '')} | Slug: {row.get('slug', '')}"}}]},
            }

            client.pages.create(parent={"database_id": database_id}, properties=properties)
            success += 1

            if success % 50 == 0:
                print(f"Imported {success} / {len(df)} rows...")

        except Exception as e:
            errors += 1
            print(f"Error on row {idx} ({row['display_name']}): {e}")

    print(f"\n✅ Done! Successfully imported: {success}")
    if errors:
        print(f"⚠️  Errors: {errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to your bloodwork CSV")
    parser.add_argument("--token", required=True, help="Notion Integration Token")
    parser.add_argument("--db_id", required=True, help="Bloodwork Database ID")
    args = parser.parse_args()

    import_bloodwork(args.csv, args.token, args.db_id)