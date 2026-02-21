# Aternos Suffix Scanner

A multi-threaded Python scanner for finding existing Aternos server suffixes.

## Features
- Thread-local cloudscraper session reuse
- Round-robin proxy rotation with auto-eviction
- Adaptive 429 rate-limit backoff
- Bounded task queue (constant memory footprint)
- Retries with exponential backoff on transient errors
- Progress tracking via tqdm
- Dry-run mode for testing

## Setup

1. Install dependencies:
   ```cmd
   pip install -r requirements.txt
   ```
2. Create a `proxies.txt` file in the same directory and add your proxies (one per line, e.g., `ip:port`).

## Usage

Run the scanner:
```cmd
python scan.py
```

To test threading and queue logic without making live HTTP requests:
```cmd
python scan.py --dry-run
```
