"""
Aternos Multi-Threaded Scanner
===================================================
Improvements over the original:
  1. Thread-local cloudscraper session reuse (HTTP keep-alive)
  2. Round-robin proxy rotation with failure tracking & eviction
  3. Adaptive 429 rate-limit backoff per thread
  4. Bounded task queue (producer-consumer, ~10k items max)
  5. tqdm progress bar (no stdout lock contention)
  6. Retry with exponential backoff for transient errors
  7. Single global lock for counters
  8. Graceful shutdown via threading.Event
  9. --dry-run flag for testing without live requests
"""

import cloudscraper
import time
import sys
import os
import threading
import itertools
import string
import random
import queue
import argparse
from collections import defaultdict, deque

try:
    from tqdm import tqdm
except ImportError:
    print("tqdm not found. Install it:  pip install tqdm")
    sys.exit(1)

# ── ANSI Colors ──────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# ── Configuration ────────────────────────────────────────────────────────────
BASE_URL = "https://add.aternos.org/"
PROXY_MAX_FAILURES = 3       # evict proxy after this many consecutive failures
RETRY_LIMIT = 3              # max retries per suffix on transient errors
QUEUE_MAX_SIZE = 10_000      # bounded task queue size
MAX_BACKOFF = 30.0           # max per-thread 429 backoff (seconds)

# ── Global State ─────────────────────────────────────────────────────────────
found_count = 0
counter_lock = threading.Lock()
file_lock = threading.Lock()
shutdown_event = threading.Event()


# ═══════════════════════════════════════════════════════════════════════════════
#  1. Thread-local cloudscraper reuse
# ═══════════════════════════════════════════════════════════════════════════════
thread_local = threading.local()

def get_scraper():
    """Return a per-thread cloudscraper instance (lazy-created)."""
    if not hasattr(thread_local, "scraper"):
        thread_local.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
    return thread_local.scraper


# ═══════════════════════════════════════════════════════════════════════════════
#  2. Proxy Manager — round-robin rotation + failure tracking
# ═══════════════════════════════════════════════════════════════════════════════
class ProxyManager:
    """Thread-safe proxy pool with round-robin rotation and auto-eviction."""

    def __init__(self, proxy_file="proxies.txt"):
        self._lock = threading.Lock()
        self._proxies: deque[str] = deque()
        self._failures: dict[str, int] = defaultdict(int)
        self._evicted = 0
        self._load(proxy_file)

    # ── loading ──────────────────────────────────────────────────────────
    def _load(self, path: str):
        try:
            with open(path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        self._proxies.append(line)
        except FileNotFoundError:
            pass

    # ── public API ───────────────────────────────────────────────────────
    @property
    def total_loaded(self) -> int:
        return len(self._proxies) + self._evicted

    @property
    def alive_count(self) -> int:
        with self._lock:
            return len(self._proxies)

    @property
    def evicted_count(self) -> int:
        return self._evicted

    def get_next(self) -> dict | None:
        """Return the next proxy dict (round-robin) or None if pool empty."""
        with self._lock:
            if not self._proxies:
                return None
            proxy = self._proxies[0]
            self._proxies.rotate(-1)          # move head to tail
            return {"http": proxy, "https": proxy}

    def mark_failure(self, proxy_dict: dict | None):
        """Increment failure counter; evict after PROXY_MAX_FAILURES."""
        if proxy_dict is None:
            return
        addr = proxy_dict.get("http", "")
        with self._lock:
            self._failures[addr] += 1
            if self._failures[addr] >= PROXY_MAX_FAILURES:
                try:
                    self._proxies.remove(addr)
                    self._evicted += 1
                except ValueError:
                    pass

    def mark_success(self, proxy_dict: dict | None):
        """Reset failure counter on success."""
        if proxy_dict is None:
            return
        addr = proxy_dict.get("http", "")
        with self._lock:
            self._failures[addr] = 0


# ═══════════════════════════════════════════════════════════════════════════════
#  3 + 6.  check_server w/ retry + rate-limit awareness
# ═══════════════════════════════════════════════════════════════════════════════
def check_server(base_name: str, suffix: str, proxy_mgr: ProxyManager,
                 *, dry_run: bool = False):
    """
    Try to reach the Aternos add-server URL.
    Returns (found: bool, status_tag: str, location: str|None).
    Retries up to RETRY_LIMIT times on transient errors with backoff.
    """
    target = f"{base_name}-{suffix}"
    url = f"{BASE_URL}{target}"
    scraper = get_scraper()

    for attempt in range(1, RETRY_LIMIT + 1):
        if shutdown_event.is_set():
            return False, "shutdown", None

        proxy = proxy_mgr.get_next()

        # ── dry-run mode (for testing) ───────────────────────────────
        if dry_run:
            time.sleep(random.uniform(0.001, 0.005))
            # Simulate occasional 302 find (~0.01%)
            if random.random() < 0.0001:
                return True, target, "https://aternos.org/server/example"
            return False, None, None

        try:
            kwargs = {"allow_redirects": False, "timeout": 10}
            if proxy:
                kwargs["proxies"] = proxy

            response = scraper.get(url, **kwargs)

            if response.status_code == 302:
                location = response.headers.get("Location", "Unknown Location")
                proxy_mgr.mark_success(proxy)
                return True, target, location

            if response.status_code == 429:
                proxy_mgr.mark_failure(proxy)
                return False, "429", None

            if response.status_code == 403:
                proxy_mgr.mark_failure(proxy)
                return False, "403", None

            # Any other status → not found
            proxy_mgr.mark_success(proxy)
            return False, None, None

        except Exception:
            proxy_mgr.mark_failure(proxy)
            if attempt < RETRY_LIMIT:
                time.sleep(min(2 ** attempt, 8))   # 2s, 4s, 8s
                continue
            return False, "error", None

    return False, "error", None


# ═══════════════════════════════════════════════════════════════════════════════
#  4 + 7 + 8.  Worker (consumer) thread
# ═══════════════════════════════════════════════════════════════════════════════
def worker(base_name: str, proxy_mgr: ProxyManager, task_q: queue.Queue,
           pbar: tqdm, *, dry_run: bool = False):
    """Consumer: pull suffixes from the queue and scan them."""
    global found_count
    local_delay = 0.0          # adaptive 429 backoff

    while not shutdown_event.is_set():
        try:
            suffix = task_q.get(timeout=0.5)
        except queue.Empty:
            continue

        if suffix is None:       # sentinel → stop
            task_q.task_done()
            break

        # ── adaptive delay from previous 429 ─────────────────────────
        if local_delay > 0:
            time.sleep(local_delay)

        found, status, location = check_server(
            base_name, suffix, proxy_mgr, dry_run=dry_run
        )

        # ── adjust backoff ───────────────────────────────────────────
        if status == "429":
            local_delay = min(local_delay * 2 + 1.0, MAX_BACKOFF)
        else:
            local_delay = max(0.0, local_delay - 0.5)

        # ── record results ───────────────────────────────────────────
        if found:
            with counter_lock:
                found_count += 1
            with file_lock:
                with open("found.txt", "a") as f:
                    f.write(f"{status}\n")
            pbar.write(f"{GREEN}[+] FOUND: {status} -> {location}{RESET}")

        pbar.update(1)
        pbar.set_postfix(found=found_count, proxies=proxy_mgr.alive_count,
                         refresh=False)
        task_q.task_done()


# ═══════════════════════════════════════════════════════════════════════════════
#  Producer thread
# ═══════════════════════════════════════════════════════════════════════════════
def producer(task_q: queue.Queue, num_workers: int):
    """Generate all 36^4 suffixes and push them into the bounded queue."""
    chars = string.ascii_lowercase + string.digits
    for combo in itertools.product(chars, repeat=4):
        if shutdown_event.is_set():
            break
        task_q.put("".join(combo))       # blocks if queue full → backpressure

    # Send one sentinel per worker to signal shutdown
    for _ in range(num_workers):
        task_q.put(None)


# ═══════════════════════════════════════════════════════════════════════════════
#  Main entry
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    global found_count

    parser = argparse.ArgumentParser(description="Aternos server scanner")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate requests without hitting the real server")
    args = parser.parse_args()

    # ── Banner ────────────────────────────────────────────────────────
    print(f"\n{BOLD}{CYAN}+----------------------------------------------+")
    print(f"|   Aternos Multi-Threaded Scanner  (v2.0)    |")
    print(f"+----------------------------------------------+{RESET}\n")

    if args.dry_run:
        print(f"{YELLOW}[DRY-RUN MODE] No real HTTP requests will be made.{RESET}\n")

    # ── User input ────────────────────────────────────────────────────
    base_name = input("Enter base user name (e.g., Engn46): ").strip()
    if not base_name:
        print(f"{RED}Base name cannot be empty.{RESET}")
        return

    proxy_mgr = ProxyManager("proxies.txt")
    print(f"{YELLOW}Loaded {proxy_mgr.total_loaded} proxies.{RESET}")

    if proxy_mgr.alive_count == 0 and not args.dry_run:
        print(f"{RED}WARNING: No proxies found! You will likely be rate-limited.{RESET}")
        if input("Continue without proxies? (y/n): ").lower() != "y":
            return

    try:
        max_threads = int(input(
            "Enter number of threads (10-50 without proxies, 100+ with): "
        ))
    except ValueError:
        max_threads = 10

    max_threads = max(1, max_threads)
    total = 36 ** 4   # 1,679,616 combinations

    print(f"\n{GREEN}Starting scan with {max_threads} threads...")
    print(f"Total combinations: {total:,}")
    print(f"Queue size limit:   {QUEUE_MAX_SIZE:,}")
    print(f"Proxy eviction:     after {PROXY_MAX_FAILURES} failures")
    print(f"Retry limit:        {RETRY_LIMIT} per suffix{RESET}")
    print(f"Press Ctrl+C to stop.\n")

    # ── Task queue & progress bar ─────────────────────────────────────
    task_q = queue.Queue(maxsize=QUEUE_MAX_SIZE)
    pbar = tqdm(total=total, desc="Scanning", unit="req",
                bar_format=("{l_bar}{bar}| {n_fmt}/{total_fmt} "
                            "[{elapsed}<{remaining}, {rate_fmt}{postfix}]"),
                dynamic_ncols=True)

    start_time = time.time()

    # ── Launch threads ────────────────────────────────────────────────
    threads: list[threading.Thread] = []

    # Producer
    prod = threading.Thread(target=producer, args=(task_q, max_threads),
                            daemon=True, name="producer")
    prod.start()

    # Consumers
    for i in range(max_threads):
        t = threading.Thread(
            target=worker,
            args=(base_name, proxy_mgr, task_q, pbar),
            kwargs={"dry_run": args.dry_run},
            daemon=True,
            name=f"worker-{i}",
        )
        t.start()
        threads.append(t)

    # ── Wait for completion or Ctrl+C ─────────────────────────────────
    try:
        for t in threads:
            while t.is_alive():
                t.join(timeout=0.5)
    except KeyboardInterrupt:
        print(f"\n{RED}[!] Stopping scan...{RESET}")
        shutdown_event.set()
        # Drain the queue so workers can exit
        while not task_q.empty():
            try:
                task_q.get_nowait()
                task_q.task_done()
            except queue.Empty:
                break
        # Push sentinels so blocked workers wake up
        for _ in range(max_threads):
            try:
                task_q.put_nowait(None)
            except queue.Full:
                pass

        for t in threads:
            t.join(timeout=3)

    pbar.close()

    # ── Summary ───────────────────────────────────────────────────────
    elapsed = time.time() - start_time
    checked = pbar.n
    rate = checked / elapsed if elapsed > 0 else 0

    print(f"\n{BOLD}{CYAN}=== Scan Summary ==={RESET}")
    print(f"  Checked:   {checked:,} / {total:,}")
    print(f"  Found:     {GREEN}{found_count}{RESET}")
    print(f"  Elapsed:   {elapsed:.1f}s")
    print(f"  Speed:     {rate:.2f} req/s")
    print(f"  Proxies:   {proxy_mgr.alive_count} alive, "
          f"{proxy_mgr.evicted_count} evicted "
          f"(of {proxy_mgr.total_loaded} loaded)")
    if found_count > 0:
        print(f"  Results saved to {GREEN}found.txt{RESET}")
    print()


if __name__ == "__main__":
    main()