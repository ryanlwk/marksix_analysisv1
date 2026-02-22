# Mark Six History

Fetch and clean historical Hong Kong Mark Six lottery results into a local CSV.

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) for dependency management (install: `pip install uv` or see [Astral docs](https://docs.astral.sh/uv/))

## Installation

```bash
cd mark_six_history
uv sync
```

## Usage

### Interactive Mode

Run without arguments for interactive menu:

```bash
uv run python fetch_data.py
```

You'll see:
```
What would you like to do?
1. Update history (fetch new results)
2. Show statistics (analyze existing data)
3. Both (update and analyze)
```

**Option 1 - Update History:**
- **First run**: Downloads all available data (50+ draws) and saves to `history.csv`
- **Subsequent runs**: Only fetches NEW results since last update (incremental)
- Fast and efficient - no re-downloading old data

**Option 2 - Show Statistics:**
- Analyzes existing `history.csv` without fetching
- Choose 1/3/6 months for analysis period
- No network required

**Option 3 - Both:**
- Updates history with new results
- Then displays frequency analysis

### Command-Line Mode

Skip the menu with CLI flags:

```bash
# Update history only (incremental)
uv run python fetch_data.py --months 6

# Statistics only (no fetch)
uv run python fetch_data.py --stats-only --months 3

# Update and analyze
uv run python fetch_data.py --months 6 --analyze

# Force full refresh (re-download all data)
uv run python fetch_data.py --force-refresh
```

### Frequency Analysis Output

Statistics show:
- Top 10 most frequent numbers (main 6 balls)
- Numbers that never appeared in the period
- Least frequent numbers (bottom 10)
- Most frequent Extra numbers

Alternatively, if you added the script entrypoint:

```bash
uv run fetch-mark-six --months 1
```

## Data sources

The script tries, in order:

1. **Lottolyzer** (en.lottolyzer.com) – comprehensive Mark Six results including latest 2026 draws  
2. **icelam/mark-six-data-visualization** (GitHub) – full history JSON from 1993 to 2025  
3. **williammw/marksixheatmap** (GitHub) – CSV with historical draws  

## Output

- **File:** `history.csv` (in the project directory)  
- **Columns:** `date`, `n1`, `n2`, `n3`, `n4`, `n5`, `n6`, `special_number` (Extra number)  
- Only the selected range (1 / 3 / 6 months) is included.
- Sorted by date (newest first).

## License

MIT
