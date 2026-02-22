#!/usr/bin/env python3
"""
Fetch and analyze Hong Kong Mark Six results.
Supports incremental updates and frequency analysis.
Saves data as history.csv with columns: date, n1, n2, n3, n4, n5, n6, special_number (Extra number).
"""

import argparse
import json
import re
import sys
from pathlib import Path
from collections import Counter

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


def load_existing_history() -> tuple[pd.DataFrame | None, str | None]:
    """Load existing history.csv if it exists. Returns (DataFrame, latest_date)."""
    if not OUTPUT_FILE.exists():
        return None, None
    
    try:
        df = pd.read_csv(OUTPUT_FILE)
        if len(df) == 0:
            return None, None
        
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        latest_date = df["date"].max()
        return df, latest_date
    except Exception as e:
        print(f"Warning: Could not read existing history.csv: {e}", file=sys.stderr)
        return None, None


def display_history(df: pd.DataFrame) -> None:
    """Display draw history in a formatted table."""
    print(f"\n{'='*60}")
    print(f"DRAW HISTORY - {len(df)} draws")
    print(f"Date range: {df['date'].iloc[0]} to {df['date'].iloc[-1]}")
    print(f"{'='*60}\n")
    print(f"{'Date':<12} {'Numbers':<25} {'Extra':<5}")
    print("-" * 60)
    
    for _, row in df.iterrows():
        nums = f"{row['n1']:2d}, {row['n2']:2d}, {row['n3']:2d}, {row['n4']:2d}, {row['n5']:2d}, {row['n6']:2d}"
        extra = f"{row['special_number']}" if pd.notna(row['special_number']) else "N/A"
        print(f"{row['date']:<12} {nums:<25} {extra:<5}")
    
    print()


def analyze_frequencies(df: pd.DataFrame) -> None:
    """Display frequency analysis of numbers in the dataset."""
    print(f"{'='*60}")
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


def ask_user_action() -> int:
    """Ask user what they want to do. Returns 1, 2, or 3."""
    print("\nWhat would you like to do?")
    print("1. Update history (fetch new results)")
    print("2. Show history and analysis (from existing data)")
    print("3. Both (update and show)")
    
    while True:
        try:
            choice = input("\nEnter choice (1/2/3): ").strip()
            if choice in ("1", "2", "3"):
                return int(choice)
            print("Please enter 1, 2, or 3.", file=sys.stderr)
        except (EOFError, KeyboardInterrupt):
            print("\nDefaulting to option 1 (update history).", file=sys.stderr)
            return 1


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
        help="Number of months for statistics (1, 3, or 6). If not set, will prompt."
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Display frequency analysis (equivalent to choosing option 3)."
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Only show statistics from existing data (equivalent to choosing option 2)."
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Force full data refresh instead of incremental update."
    )
    args = parser.parse_args()
    
    existing_df, latest_date = load_existing_history()
    
    if args.stats_only:
        action = 2
    elif args.analyze:
        action = 3
    elif args.months is not None or args.force_refresh:
        action = 1
    else:
        action = ask_user_action()
    
    if action == 2:
        if existing_df is None:
            print("No existing history.csv found. Please fetch data first (option 1).", file=sys.stderr)
            sys.exit(1)
        
        if args.months:
            months = args.months
        else:
            months = ask_data_range()
        
        cutoff = pd.Timestamp.now() - pd.DateOffset(months=months)
        existing_df["_dt"] = pd.to_datetime(existing_df["date"], errors="coerce")
        filtered_df = existing_df[existing_df["_dt"] >= cutoff].drop(columns=["_dt"]).reset_index(drop=True)
        
        if len(filtered_df) == 0:
            print(f"No draws in the selected {months}-month range.", file=sys.stderr)
            sys.exit(0)
        
        display_history(filtered_df)
        analyze_frequencies(filtered_df)
        return
    
    if action in (1, 3):
        if existing_df is not None and not args.force_refresh:
            print(f"Existing data found. Latest date: {latest_date}", file=sys.stderr)
            print("Fetching incremental updates...", file=sys.stderr)
            
            df_new = fetch_from_lottolyzer()
            if df_new is None or len(df_new) == 0:
                print("Could not fetch new data from Lottolyzer.", file=sys.stderr)
                sys.exit(1)
            
            df_new = clean_df(df_new)
            df_new["_dt"] = pd.to_datetime(df_new["date"], errors="coerce")
            latest_dt = pd.to_datetime(latest_date)
            new_rows = df_new[df_new["_dt"] > latest_dt].drop(columns=["_dt"])
            
            if len(new_rows) == 0:
                print(f"No new results since {latest_date}. Data is up to date.", file=sys.stderr)
                df = existing_df
            else:
                print(f"Found {len(new_rows)} new draws since {latest_date}.", file=sys.stderr)
                df = pd.concat([existing_df, new_rows], ignore_index=True)
                df = df.drop_duplicates(subset=["date"], keep="first")
                df = df.sort_values("date", ascending=False).reset_index(drop=True)
                df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
                print(f"Updated {OUTPUT_FILE} (now {len(df)} total rows).", file=sys.stderr)
        else:
            if args.force_refresh:
                print("Force refresh: downloading all data...", file=sys.stderr)
            else:
                print("No existing data. Fetching full history...", file=sys.stderr)
            
            df = None
            for name, fetcher in [
                ("Lottolyzer", fetch_from_lottolyzer),
                ("icelam JSON", fetch_from_icelam),
                ("williammw CSV", fetch_from_williammw_csv),
            ]:
                try:
                    df = fetcher()
                    if df is not None and len(df) > 0:
                        print(f"Loaded {len(df)} rows from {name}.", file=sys.stderr)
                        break
                except Exception as e:
                    print(f"{name} error: {e}", file=sys.stderr)
            
            if df is None or len(df) == 0:
                print("No data obtained from any source.", file=sys.stderr)
                sys.exit(1)
            
            df = clean_df(df)
            df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
            print(f"Saved to {OUTPUT_FILE} ({len(df)} rows).", file=sys.stderr)
        
        if action == 3:
            if args.months:
                months = args.months
            else:
                months = ask_data_range()
            
            cutoff = pd.Timestamp.now() - pd.DateOffset(months=months)
            df["_dt"] = pd.to_datetime(df["date"], errors="coerce")
            filtered_df = df[df["_dt"] >= cutoff].drop(columns=["_dt"]).reset_index(drop=True)
            
            if len(filtered_df) == 0:
                print(f"No draws in the selected {months}-month range.", file=sys.stderr)
            else:
                display_history(filtered_df)
                analyze_frequencies(filtered_df)


if __name__ == "__main__":
    main()
