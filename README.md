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

Run the fetcher (will prompt for data range if not specified):

```bash
uv run python fetch_data.py
```

Choose when prompted:

- **1** – last 1 month of draws  
- **3** – last 3 months  
- **6** – last 6 months  

To skip the prompt (e.g. in scripts or automation):

```bash
uv run python fetch_data.py --months 3
```

Supported values: `1`, `3`, `6`.

### Frequency Analysis

Add `--analyze` to display statistics after fetching:

```bash
uv run python fetch_data.py --months 6 --analyze
```

This shows:
- Top 10 most frequent numbers
- Numbers that never appeared in the selected period
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
