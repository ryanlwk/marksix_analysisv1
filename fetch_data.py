#!/usr/bin/env python3
"""
Fetch historical Hong Kong Mark Six results.
Saves cleaned data as history.csv with columns: date, n1, n2, n3, n4, n5, n6, special_number (Extra number).
"""

import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

TIMEOUT = 15
OUTPUT_FILE = Path(__file__).resolve().parent / "history.csv"

LOTTOLYZER_URL = "https://en.lottolyzer.com/history/hong-kong/mark-six"
ICELAM_JSON = "https://raw.githubusercontent.com/icelam/mark-six-data-visualization/master/data/all.json"
WILLIAMMW_CSV = "https://raw.githubusercontent.com/williammw/marksixheatmap/master/MarkSix.csv"


def fetch_from_icelam() -> pd.DataFrame | None:
    """Download and parse icelam/mark-six-data-visualization all.json."""
    try:
        r = requests.get(ICELAM_JSON, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"Icelam JSON failed: {e}", file=sys.stderr)
        return None
    
    if not data or not isinstance(data, list):
        print("Icelam JSON empty or invalid format", file=sys.stderr)
        return None
    
    rows = []
    for item in data:
        try:
            date = item.get("date")
            no = item.get("no")
            sno = item.get("sno", "")
            
            if not date or not no or len(no) != 6:
                continue
            
            special = None
            if str(sno).strip().isdigit():
                special = int(sno)
            
            rows.append({
                "date": date,
                "n1": int(no[0]), "n2": int(no[1]), "n3": int(no[2]),
                "n4": int(no[3]), "n5": int(no[4]), "n6": int(no[5]),
                "special_number": special,
            })
        except (TypeError, ValueError, KeyError):
            continue
    
    if not rows:
        return None
    
    return pd.DataFrame(rows)


def fetch_from_williammw_csv() -> pd.DataFrame | None:
    """Download and parse williammw/marksixheatmap MarkSix.csv."""
    try:
        df = pd.read_csv(WILLIAMMW_CSV, timeout=TIMEOUT)
    except Exception as e:
        print(f"Williammw CSV failed: {e}", file=sys.stderr)
        return None
    
    df.columns = df.columns.str.strip()
    
    date_col = "Date" if "Date" in df.columns else None
    if not date_col:
        return None
    
    num_cols = []
    if "Winning Number 1" in df.columns:
        num_cols = ["Winning Number 1", "2", "3", "4", "5", "6"]
    
    extra_col = None
    for col in df.columns:
        if "Extra" in col:
            extra_col = col
            break
    
    if not all(c in df.columns for c in num_cols):
        return None
    
    renames = {date_col: "date"}
    for i, c in enumerate(num_cols):
        renames[c] = f"n{i+1}"
    if extra_col:
        renames[extra_col] = "special_number"
    
    df = df.rename(columns=renames)
    
    needed = ["date", "n1", "n2", "n3", "n4", "n5", "n6"]
    if not all(c in df.columns for c in needed):
        return None
    
    out = df[needed + (["special_number"] if "special_number" in df.columns else [])].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    
    for c in ["n1", "n2", "n3", "n4", "n5", "n6"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    
    if "special_number" in out.columns:
        out["special_number"] = pd.to_numeric(out["special_number"], errors="coerce")
    
    out = out.dropna(subset=["date", "n1", "n2", "n3", "n4", "n5", "n6"])
    
    return out if len(out) > 0 else None


def fetch_from_lottolyzer() -> pd.DataFrame | None:
    """Scrape en.lottolyzer.com for Mark Six results (includes 2026 data)."""
    try:
        r = requests.get(LOTTOLYZER_URL, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
    except requests.RequestException as e:
        print(f"Lottolyzer scrape failed: {e}", file=sys.stderr)
        return None
    
    rows = []
    
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 3:
                continue
            
            draw_num = cells[0].get_text(strip=True)
            date_text = cells[1].get_text(strip=True)
            winning_text = cells[2].get_text(strip=True)
            extra_text = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            
            if not re.match(r"\d{2}/\d{3}", draw_num):
                continue
            
            if not re.match(r"\d{4}-\d{2}-\d{2}", date_text):
                continue
            
            nums = [int(x.strip()) for x in winning_text.split(",") if x.strip().isdigit()]
            if len(nums) != 6:
                continue
            
            special = None
            if extra_text.strip().isdigit():
                special = int(extra_text.strip())
            
            rows.append({
                "date": date_text,
                "n1": nums[0], "n2": nums[1], "n3": nums[2],
                "n4": nums[3], "n5": nums[4], "n6": nums[5],
                "special_number": special,
            })
    
    if not rows:
        return None
    
    return pd.DataFrame(rows)


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize columns and types."""
    df = df.copy()
    
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    
    for c in ["n1", "n2", "n3", "n4", "n5", "n6"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    
    if "special_number" in df.columns:
        df["special_number"] = pd.to_numeric(df["special_number"], errors="coerce").astype("Int64")
    else:
        df["special_number"] = pd.NA
    
    df = df.dropna(subset=["date", "n1", "n2", "n3", "n4", "n5", "n6"])
    df = df.drop_duplicates(subset=["date"], keep="first")
    df = df.sort_values("date", ascending=False).reset_index(drop=True)
    
    return df[["date", "n1", "n2", "n3", "n4", "n5", "n6", "special_number"]]


def analyze_frequencies(df: pd.DataFrame) -> None:
    """Display frequency analysis of numbers in the dataset."""
    from collections import Counter
    
    print(f"\n{'='*60}")
    print(f"FREQUENCY ANALYSIS - {len(df)} draws")
    print(f"Date range: {df['date'].iloc[0]} to {df['date'].iloc[-1]}")
    print(f"{'='*60}\n")
    
    all_nums = []
    for col in ['n1', 'n2', 'n3', 'n4', 'n5', 'n6']:
        all_nums.extend(df[col].tolist())
    
    freq = Counter(all_nums)
    
    print("TOP 10 MOST FREQUENT NUMBERS (Main 6):")
    print("-" * 40)
    for num, count in freq.most_common(10):
        print(f"  Number {num:2d}: {count:2d} times")
    
    all_possible = set(range(1, 50))
    appeared = set(freq.keys())
    never_appeared = sorted(all_possible - appeared)
    
    print(f"\nNUMBERS THAT NEVER APPEARED ({len(never_appeared)} total):")
    print("-" * 40)
    if never_appeared:
        print(f"  {', '.join(map(str, never_appeared))}")
    else:
        print("  All numbers 1-49 appeared at least once")
    
    print("\nLEAST FREQUENT NUMBERS (Bottom 10):")
    print("-" * 40)
    for num, count in freq.most_common()[-10:]:
        print(f"  Number {num:2d}: {count:2d} times")
    
    special_nums = df['special_number'].dropna().astype(int).tolist()
    if special_nums:
        special_freq = Counter(special_nums)
        print(f"\nTOP 10 MOST FREQUENT EXTRA NUMBERS:")
        print("-" * 40)
        for num, count in special_freq.most_common(10):
            print(f"  Extra {num:2d}: {count:2d} times")
    
    print(f"\n{'='*60}\n")


def ask_data_range() -> int:
    """Ask user for 1, 3, or 6 months of data. Returns number of months."""
    while True:
        try:
            choice = input("Data range: 1 month, 3 months, or 6 months? (1/3/6): ").strip()
            if choice in ("1", "3", "6"):
                return int(choice)
            print("Please enter 1, 3, or 6.", file=sys.stderr)
        except (EOFError, KeyboardInterrupt):
            print("\nDefaulting to 6 months.", file=sys.stderr)
            return 6


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Hong Kong Mark Six lottery history")
    parser.add_argument(
        "--months",
        type=int,
        choices=[1, 3, 6],
        help="Number of months of data to fetch (1, 3, or 6). If not set, will prompt."
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Display frequency analysis after fetching data."
    )
    args = parser.parse_args()
    
    df = None
    source_name = None
    
    for name, fetcher in [
        ("Lottolyzer", fetch_from_lottolyzer),
        ("icelam JSON", fetch_from_icelam),
        ("williammw CSV", fetch_from_williammw_csv),
    ]:
        try:
            df = fetcher()
            if df is not None and len(df) > 0:
                source_name = name
                print(f"Loaded {len(df)} rows from {name}.", file=sys.stderr)
                break
        except Exception as e:
            print(f"{name} error: {e}", file=sys.stderr)
    
    if df is None or len(df) == 0:
        print("No data obtained from any source.", file=sys.stderr)
        sys.exit(1)
    
    df = clean_df(df)
    
    if args.months:
        months = args.months
    else:
        months = ask_data_range()
    
    cutoff = pd.Timestamp.now() - pd.DateOffset(months=months)
    df["_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["_dt"] >= cutoff].drop(columns=["_dt"]).reset_index(drop=True)
    
    if len(df) == 0:
        print(f"No draws in the selected {months}-month range; writing header-only CSV.", file=sys.stderr)
    
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    
    print(f"Saved to {OUTPUT_FILE} ({len(df)} rows).", file=sys.stderr)
    
    if args.analyze and len(df) > 0:
        analyze_frequencies(df)


if __name__ == "__main__":
    main()
