# FastSync

High-throughput, resumable copy/move for **millions of tiny files** between Windows drives
(e.g. `E:\` → `D:\`), with a live terminal dashboard.

## Setup

```powershell
# Python 3.8+ (tested on 3.11)
python -m pip install -r requirements.txt     # just: rich
```

## Usage

```powershell
# Copy E:\ to D:\  (resumable; identical files are skipped without being read)
python fastsync.py E:\ D:\

# 32 workers (good for SSD / UASP enclosures)
python fastsync.py E:\ D:\ -t 32

# Into a subfolder, with post-copy size verification
python fastsync.py E:\ "D:\Backups\E-drive" --verify

# Move: delete each source file after a verified copy, then prune empty folders
python fastsync.py E:\ D:\ --move

# Replace destination files only when the source is newer
python fastsync.py E:\ D:\ --overwrite newer

# Preview only
python fastsync.py E:\ D:\ --dry-run

# Extra excludes (on top of System Volume Information, $RECYCLE.BIN, desktop.ini)
python fastsync.py E:\ D:\ -x Thumbs.db -x "*.tmp" -x "Temp\*"
```

> **Quoting pitfall:** never write `"E:\"` in quotes. Windows reads `\"` as an escaped quote.
> Use `E:\` unquoted, or `"E:\\"`. FastSync detects this mistake and tells you.

Ctrl+C once = graceful stop (in-flight files finish, re-run resumes). Twice = force quit.

## Options

| Option | Default | Meaning |
|---|---|---|
| `-t, --threads` | 16 | Transfer workers. 32 for SSDs; 4–8 for spinning USB HDDs (seek thrash). |
| `--scan-threads` | 8 | Parallel directory enumeration threads. |
| `--overwrite` | `never` | `never` keeps differing dest files (logged as conflicts); `newer`; `changed`. |
| `--mtime-tolerance` | 2.0 s | FAT32/exFAT timestamp granularity. |
| `--move` | off | Delete source after verified copy (also deletes sources already identical at dest). |
| `--verify` | off | Re-stat dest and compare size to source. |
| `--no-atomic` | off | Skip temp-file + rename (one less metadata op per file). |
| `--dry-run` | off | Report only. |
| `-x, --exclude` | — | Extra glob(s) matched on name or relative path. |
| `--retry-delay` | 0.5 s | Pause before the single retry. |
| `--max-queue` | 1,000,000 | Pending-job cap (bounds RAM). `0` = unlimited. |
| `--rate-window` | 30 s | Moving-average window for MB/s, files/s and ETA. |
| `--log-dir` | `.` | Where `fastsync_errors_*.log` / `fastsync_conflicts_*.log` go. |
| `--no-alt-screen` | off | Draw inline instead of in the alternate screen buffer. |

Exit codes: `0` ok, `1` completed with errors, `2` bad arguments, `3` aborted (e.g. disk full), `130` interrupted.

## How it is fast

* **No file opens to decide skips.** `os.scandir` returns size + mtime straight from
  `FindNextFileW`; the destination is indexed once into a hash map.
* **Native `CopyFileW`** copies data, timestamps and attributes in one kernel call and
  releases the GIL, so 16–32 threads keep the USB command queue full.
* **Parallel directory walking** for both index and scan; scanning and copying overlap.
* **Directory-creation cache** avoids a `stat`/`mkdir` per file.
* **Atomic writes** (`name.fastsync-partial` → rename) mean an interrupted run never leaves a
  truncated file under the real name; leftovers are deleted on the next run.
* **Long paths** (> 260 chars) are supported via the `\\?\` prefix.

## Notes

* Memory: ~250 bytes per destination file for the index (≈250 MB per million files),
  released progressively as the source scan matches entries.
* Symlinks/junctions are skipped (counted) to avoid loops.
* A disk-full error aborts the run cleanly instead of failing millions of files.
* FAT32 stores local time; after a DST change, timestamps can shift by exactly 1 hour.
  If a re-run wants to recopy everything, use `--mtime-tolerance 3602`.
