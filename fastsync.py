#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastSync - high-throughput differential copy / move for Windows.

Built for moving millions of tiny files between external drives (USB HDD/SSD),
where per-file latency, not bandwidth, is the bottleneck.

Pipeline
--------
1. Index destination  : parallel directory walk -> {relpath: (size, mtime_ns)}.
                        Leftover *.fastsync-partial files from interrupted runs are removed.
2. Scan source        : parallel directory walk; each file is compared against the index
                        *without opening it*. Identical files (same size + mtime within
                        tolerance) are skipped; new files are queued.
3. Transfer           : N worker threads drain the queue using the native Win32
                        CopyFileW API (copies data + timestamps + attributes in one call),
                        writing to a temp name and atomically renaming into place.
4. Summary            : execution table + error / conflict logs.

Directory stat data comes for free from FindFirstFile/FindNextFile via os.scandir,
so neither source nor destination files are opened during comparison.
"""
from __future__ import annotations

import argparse
import errno
import fnmatch
import os
import queue
import shutil
import stat
import sys
import threading
import time
from collections import deque
from datetime import datetime

try:
    from rich import box
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress_bar import ProgressBar
    from rich.table import Table
    from rich.text import Text
except ImportError:  # pragma: no cover
    sys.stderr.write(
        "FastSync requires the 'rich' package.\n"
        "Install it with:  python -m pip install rich\n"
    )
    sys.exit(2)

__version__ = "1.0.0"

# --------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------
IS_WINDOWS = os.name == "nt"

EXCLUDED_DIR_NAMES = frozenset({"system volume information", "$recycle.bin"})
EXCLUDED_FILE_NAMES = frozenset({"desktop.ini"})
TMP_SUFFIX = ".fastsync-partial"

FILE_ATTRIBUTE_NORMAL = 0x80
FILE_ATTRIBUTE_REPARSE_POINT = 0x400
DISK_FULL_WINERRORS = frozenset({39, 112})  # ERROR_HANDLE_DISK_FULL, ERROR_DISK_FULL

ACTION_COPY = 0
ACTION_DELETE_SOURCE = 1

# --------------------------------------------------------------------------------------
# Native file operations
# --------------------------------------------------------------------------------------
if IS_WINDOWS:
    import ctypes
    from ctypes import wintypes

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    _CopyFileW = _kernel32.CopyFileW
    _CopyFileW.argtypes = (wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.BOOL)
    _CopyFileW.restype = wintypes.BOOL

    _SetFileAttributesW = _kernel32.SetFileAttributesW
    _SetFileAttributesW.argtypes = (wintypes.LPCWSTR, wintypes.DWORD)
    _SetFileAttributesW.restype = wintypes.BOOL

    def native_copy(src: str, dst: str) -> None:
        """Copy data, timestamps and attributes in a single kernel call (GIL released)."""
        if not _CopyFileW(src, dst, False):
            err = ctypes.get_last_error()
            # winerror arg makes Python map to the right OSError subclass/errno.
            raise OSError(0, ctypes.FormatError(err).strip(), src, err, dst)

    def clear_attributes(path: str) -> None:
        """Remove READONLY/HIDDEN/SYSTEM so the file can be replaced or deleted."""
        _SetFileAttributesW(path, FILE_ATTRIBUTE_NORMAL)

else:  # Portable fallback (useful for testing on non-Windows hosts)

    def native_copy(src: str, dst: str) -> None:
        shutil.copy2(src, dst)

    def clear_attributes(path: str) -> None:
        try:
            os.chmod(path, stat.S_IREAD | stat.S_IWRITE)
        except OSError:
            pass


def force_remove(path: str) -> None:
    """Delete a file, clearing read-only/hidden/system attributes if needed."""
    try:
        os.remove(path)
    except FileNotFoundError:
        return
    except PermissionError:
        clear_attributes(path)
        os.remove(path)


def is_disk_full(exc: BaseException) -> bool:
    return isinstance(exc, OSError) and (
        getattr(exc, "winerror", None) in DISK_FULL_WINERRORS or exc.errno == errno.ENOSPC
    )


# --------------------------------------------------------------------------------------
# Path / formatting helpers
# --------------------------------------------------------------------------------------
def normalize_root(path: str) -> str:
    path = path.strip()
    if len(path) == 2 and path[1] == ":":  # "E:" -> "E:\" (otherwise it means CWD on E:)
        path += "\\"
    return os.path.abspath(path)


def to_extended(path: str) -> str:
    """Prefix with \\\\?\\ so paths longer than MAX_PATH (260) work."""
    if not IS_WINDOWS or path.startswith("\\\\?\\"):
        return path
    if path.startswith("\\\\"):
        return "\\\\?\\UNC\\" + path[2:]
    return "\\\\?\\" + path


def plain_path(path: str) -> str:
    """Strip the extended-length prefix for display."""
    if path.startswith("\\\\?\\UNC\\"):
        return "\\\\" + path[8:]
    if path.startswith("\\\\?\\"):
        return path[4:]
    return path


def human_bytes(n: float) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0 or unit == "TB":
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.2f} {unit}"
        n /= 1024.0
    return f"{n:,.2f} PB"


def human_time(seconds) -> str:
    if seconds is None or seconds != seconds or seconds == float("inf"):
        return "--:--:--"
    seconds = int(max(0, seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}d {hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def left_truncate(text: str, width: int) -> str:
    """Keep the end of a path (the file name) visible."""
    if width <= 1 or len(text) <= width:
        return text
    return "\u2026" + text[-(width - 1):]


def fmt_mtime(ns: int) -> str:
    try:
        return datetime.fromtimestamp(ns / 1e9).isoformat(sep=" ", timespec="seconds")
    except (OverflowError, OSError, ValueError):
        return str(ns)


# --------------------------------------------------------------------------------------
# Statistics & rate measurement
# --------------------------------------------------------------------------------------
class Stats:
    """Shared counters. Writers hold `lock`; the UI reads without locking (ints are atomic)."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.dest_files = 0
        self.dest_dirs = 0
        self.scanned_files = 0
        self.scanned_bytes = 0
        self.queued_files = 0  # files that need transferring
        self.queued_bytes = 0
        self.processed_files = 0  # queued files finished (success or failure)
        self.processed_bytes = 0
        self.copied_files = 0
        self.copied_bytes = 0
        self.skipped_files = 0
        self.skipped_bytes = 0
        self.conflict_files = 0
        self.conflict_bytes = 0
        self.failed_files = 0
        self.failed_bytes = 0
        self.excluded = 0
        self.special = 0  # symlinks / junctions / reparse points
        self.scan_errors = 0
        self.src_deleted = 0
        self.src_delete_failed = 0


class RateMeter:
    """Moving-window throughput (files/s and bytes/s)."""

    def __init__(self, window: float) -> None:
        self.window = window
        self.samples: deque = deque()

    def sample(self, files: int, nbytes: int) -> None:
        now = time.monotonic()
        self.samples.append((now, files, nbytes))
        while len(self.samples) > 2 and now - self.samples[0][0] > self.window:
            self.samples.popleft()

    def rates(self):
        if len(self.samples) < 2:
            return 0.0, 0.0
        t0, f0, b0 = self.samples[0]
        t1, f1, b1 = self.samples[-1]
        dt = t1 - t0
        if dt <= 0:
            return 0.0, 0.0
        return (f1 - f0) / dt, (b1 - b0) / dt


# --------------------------------------------------------------------------------------
# Parallel directory walker
# --------------------------------------------------------------------------------------
class ParallelWalker:
    """
    Multi-threaded breadth-first directory walker built on os.scandir.

    High-latency USB devices respond much faster with several outstanding
    directory enumerations than with a single-threaded os.walk.
    """

    def __init__(self, root, threads, stop_event, dir_filter, file_cb, dir_cb, error_cb, special_cb):
        self.root = root
        self.threads = max(1, threads)
        self.stop_event = stop_event
        self.dir_filter = dir_filter
        self.file_cb = file_cb
        self.dir_cb = dir_cb
        self.error_cb = error_cb
        self.special_cb = special_cb
        self._q: queue.Queue = queue.Queue()
        self._pending = 0
        self._lock = threading.Lock()

    def run(self) -> None:
        self._pending = 1
        self._q.put("")
        workers = [
            threading.Thread(target=self._work, name=f"walk-{i}", daemon=True)
            for i in range(self.threads)
        ]
        for w in workers:
            w.start()
        for w in workers:
            w.join()

    def _work(self) -> None:
        while True:
            rel = self._q.get()
            if rel is None:
                return
            try:
                if not self.stop_event.is_set():
                    self._scan(rel)
            except Exception as exc:  # noqa: BLE001 - never kill the walker
                self.error_cb(rel or ".", exc)
            finally:
                with self._lock:
                    self._pending -= 1
                    if self._pending == 0:
                        for _ in range(self.threads):
                            self._q.put(None)

    def _scan(self, rel: str) -> None:
        path = os.path.join(self.root, rel) if rel else self.root
        with os.scandir(path) as it:
            for entry in it:
                if self.stop_event.is_set():
                    return
                name = entry.name
                child = os.path.join(rel, name) if rel else name
                try:
                    st = entry.stat(follow_symlinks=False)  # cached from FindNextFile on Windows
                except OSError as exc:
                    self.error_cb(child, exc)
                    continue
                attrs = getattr(st, "st_file_attributes", 0)
                if (attrs & FILE_ATTRIBUTE_REPARSE_POINT) or stat.S_ISLNK(st.st_mode):
                    self.special_cb(child)
                    continue
                if stat.S_ISDIR(st.st_mode):
                    if not self.dir_filter(child, name):
                        continue
                    if self.dir_cb is not None:
                        self.dir_cb(child)
                    with self._lock:
                        self._pending += 1
                    self._q.put(child)
                elif stat.S_ISREG(st.st_mode):
                    self.file_cb(child, name, st)


# --------------------------------------------------------------------------------------
# Sync engine
# --------------------------------------------------------------------------------------
class FastSync:
    def __init__(self, args: argparse.Namespace, console: Console) -> None:
        self.args = args
        self.console = console
        self.src_display = normalize_root(args.source)
        self.dst_display = normalize_root(args.dest)
        self.src_root = to_extended(self.src_display)
        self.dst_root = to_extended(self.dst_display)
        self.threads = args.threads
        self.tol_ns = int(args.mtime_tolerance * 1_000_000_000)
        self.excludes = [p.lower().replace("/", "\\") for p in args.exclude]

        self.stats = Stats()
        self.meter = RateMeter(args.rate_window)
        self.stop_event = threading.Event()
        self.stop_reason: str | None = None
        self.fatal = False
        self.phase = "Starting"
        self.scan_complete = False

        self.dest_index: dict = {}
        self.known_dirs: set = set()
        self.jobs: queue.Queue = queue.Queue(maxsize=max(0, args.max_queue))
        self.worker_status: list = [None] * self.threads

        self.failures: list = []
        self._log_lock = threading.Lock()
        self._err_fh = None
        self._conf_fh = None
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_dir = os.path.abspath(args.log_dir)
        self.error_log_path = os.path.join(log_dir, f"fastsync_errors_{stamp}.log")
        self.conflict_log_path = os.path.join(log_dir, f"fastsync_conflicts_{stamp}.log")

        self.start_time = 0.0
        self.copy_start: float | None = None
        self.end_time: float | None = None

    # ---------------------------------------------------------------- control
    def request_stop(self, reason: str, fatal: bool = False) -> None:
        if not self.stop_event.is_set():
            self.stop_reason = reason
            self.fatal = fatal
            self.stop_event.set()

    # ---------------------------------------------------------------- logging
    def _open_log(self, path: str, header: str):
        fh = open(path, "a", encoding="utf-8", newline="\n")
        fh.write(f"# FastSync {__version__} - {header}\n")
        fh.write(f"# Source: {self.src_display}\n# Destination: {self.dst_display}\n")
        return fh

    def _log_failure(self, op: str, rel: str, exc: BaseException) -> None:
        msg = f"{type(exc).__name__}: {exc}"
        line_path = os.path.join(self.src_display, rel) if rel not in ("", ".") else self.src_display
        if op == "DEST-SCAN":
            line_path = os.path.join(self.dst_display, rel) if rel not in ("", ".") else self.dst_display
        with self._log_lock:
            try:
                if self._err_fh is None:
                    self._err_fh = self._open_log(self.error_log_path, "error log")
                    self._err_fh.write("# timestamp\toperation\tpath\terror\n")
                self._err_fh.write(
                    f"{datetime.now().isoformat(timespec='seconds')}\t{op}\t{line_path}\t{msg}\n"
                )
                self._err_fh.flush()
            except OSError:
                pass
            if len(self.failures) < 1000:
                self.failures.append((op, line_path, msg))

    def _log_conflict(self, rel, size, mtime, dsize, dmtime) -> None:
        with self._log_lock:
            try:
                if self._conf_fh is None:
                    self._conf_fh = self._open_log(
                        self.conflict_log_path,
                        f"conflicts (destination differs, kept due to --overwrite {self.args.overwrite})",
                    )
                    self._conf_fh.write("# relative_path\tsource(size, mtime)\tdestination(size, mtime)\n")
                self._conf_fh.write(
                    f"{rel}\t{size}, {fmt_mtime(mtime)}\t{dsize}, {fmt_mtime(dmtime)}\n"
                )
            except OSError:
                pass

    def _close_logs(self) -> None:
        with self._log_lock:
            for fh in (self._err_fh, self._conf_fh):
                if fh is not None:
                    try:
                        fh.close()
                    except OSError:
                        pass

    # ---------------------------------------------------------------- filters
    def _matches_exclude(self, rel: str, name: str) -> bool:
        if not self.excludes:
            return False
        lname, lrel = name.lower(), rel.lower()
        return any(fnmatch.fnmatchcase(lname, p) or fnmatch.fnmatchcase(lrel, p) for p in self.excludes)

    def _src_dir_filter(self, rel: str, name: str) -> bool:
        if name.lower() in EXCLUDED_DIR_NAMES or self._matches_exclude(rel, name):
            with self.stats.lock:
                self.stats.excluded += 1
            return False
        return True

    def _dst_dir_filter(self, rel: str, name: str) -> bool:
        return name.lower() not in EXCLUDED_DIR_NAMES

    def _on_special(self, rel: str) -> None:
        with self.stats.lock:
            self.stats.special += 1

    def _ignore_special(self, rel: str) -> None:
        pass

    def _scan_error(self, op: str):
        def handler(rel, exc):
            with self.stats.lock:
                self.stats.scan_errors += 1
            self._log_failure(op, rel, exc)

        return handler

    # ---------------------------------------------------------------- phase 1: destination index
    def _index_dest_file(self, rel: str, name: str, st) -> None:
        if name.endswith(TMP_SUFFIX):  # leftover from an interrupted run
            if not self.args.dry_run:
                try:
                    force_remove(os.path.join(self.dst_root, rel))
                except OSError:
                    pass
            return
        self.dest_index[rel.lower()] = (st.st_size, st.st_mtime_ns)
        with self.stats.lock:
            self.stats.dest_files += 1

    def _index_dest_dir(self, rel: str) -> None:
        self.known_dirs.add(rel.lower())
        with self.stats.lock:
            self.stats.dest_dirs += 1

    # ---------------------------------------------------------------- phase 2: source scan
    def _on_source_file(self, rel: str, name: str, st) -> None:
        s = self.stats
        lname = name.lower()
        if lname in EXCLUDED_FILE_NAMES or lname.endswith(TMP_SUFFIX) or self._matches_exclude(rel, name):
            with s.lock:
                s.excluded += 1
            return

        size, mtime = st.st_size, st.st_mtime_ns
        existing = self.dest_index.pop(rel.lower(), None)  # pop: frees memory as we go

        if existing is None:
            with s.lock:
                s.scanned_files += 1
                s.scanned_bytes += size
                s.queued_files += 1
                s.queued_bytes += size
            self._enqueue((ACTION_COPY, rel, size, mtime, False))
            return

        dsize, dmtime = existing
        if dsize == size and abs(dmtime - mtime) <= self.tol_ns:
            with s.lock:
                s.scanned_files += 1
                s.scanned_bytes += size
                s.skipped_files += 1
                s.skipped_bytes += size
            if self.args.move:
                self._enqueue((ACTION_DELETE_SOURCE, rel, size, mtime, False))
            return

        policy = self.args.overwrite
        if policy == "changed" or (policy == "newer" and mtime > dmtime + self.tol_ns):
            with s.lock:
                s.scanned_files += 1
                s.scanned_bytes += size
                s.queued_files += 1
                s.queued_bytes += size
            self._enqueue((ACTION_COPY, rel, size, mtime, True))
        else:
            with s.lock:
                s.scanned_files += 1
                s.scanned_bytes += size
                s.conflict_files += 1
                s.conflict_bytes += size
            self._log_conflict(rel, size, mtime, dsize, dmtime)

    def _enqueue(self, job) -> None:
        # Bounded queue: block (with stop checks) instead of exhausting RAM.
        while not self.stop_event.is_set():
            try:
                self.jobs.put(job, timeout=0.5)
                return
            except queue.Full:
                continue

    # ---------------------------------------------------------------- phase 3: transfer
    def _ensure_parent(self, rel: str) -> None:
        parent = os.path.dirname(rel)
        if not parent:
            return
        key = parent.lower()
        if key in self.known_dirs:
            return
        os.makedirs(os.path.join(self.dst_root, parent), exist_ok=True)
        self.known_dirs.add(key)

    def _with_retry(self, op: str, rel: str, fn, before_retry=None) -> bool:
        """Run fn; on failure retry exactly once, then log and skip (non-blocking for others)."""
        for attempt in (1, 2):
            try:
                fn()
                return True
            except OSError as exc:
                if is_disk_full(exc):
                    self._log_failure(op, rel, exc)
                    self.request_stop("Destination disk is full", fatal=True)
                    return False
                if attempt == 1:
                    if before_retry is not None:
                        try:
                            before_retry()
                        except OSError:
                            pass
                    time.sleep(self.args.retry_delay)
                    continue
                self._log_failure(op, rel, exc)
                return False
        return False

    def _copy_file(self, rel: str, size: int, overwrite: bool) -> bool:
        src = os.path.join(self.src_root, rel)
        dst = os.path.join(self.dst_root, rel)
        atomic = not self.args.no_atomic
        tmp = dst + TMP_SUFFIX if atomic else dst
        verify = self.args.verify

        def attempt() -> None:
            self._ensure_parent(rel)
            native_copy(src, tmp)
            if verify:
                got = os.stat(tmp).st_size
                want = os.stat(src).st_size
                if got != want:
                    raise OSError(errno.EIO, f"verification failed: copied {got} bytes, source has {want}", tmp)
            if atomic:
                os.replace(tmp, dst)

        def before_retry() -> None:
            parent = os.path.dirname(rel)
            if parent:
                self.known_dirs.discard(parent.lower())
            if atomic:
                try:
                    force_remove(tmp)
                except OSError:
                    pass
            if os.path.lexists(dst):
                clear_attributes(dst)  # read-only / hidden destination blocks replace

        ok = self._with_retry("COPY", rel, attempt, before_retry)
        if not ok and atomic:
            try:
                force_remove(tmp)
            except OSError:
                pass
        return ok

    def _delete_source(self, rel: str, verify_against_dest: bool) -> bool:
        src = os.path.join(self.src_root, rel)
        dst = os.path.join(self.dst_root, rel)

        def attempt() -> None:
            if verify_against_dest:
                ssz, dsz = os.stat(src).st_size, os.stat(dst).st_size
                if ssz != dsz:
                    raise OSError(errno.EIO, f"refusing to delete source: size {ssz} != destination {dsz}", src)
            force_remove(src)

        ok = self._with_retry("DELETE-SOURCE", rel, attempt, lambda: clear_attributes(src))
        with self.stats.lock:
            if ok:
                self.stats.src_deleted += 1
            else:
                self.stats.src_delete_failed += 1
        return ok

    def _process_copy(self, rel: str, size: int, overwrite: bool) -> None:
        s = self.stats
        if self.args.dry_run:
            ok = True
        else:
            ok = self._copy_file(rel, size, overwrite)
        with s.lock:
            s.processed_files += 1
            s.processed_bytes += size
            if ok:
                s.copied_files += 1
                s.copied_bytes += size
            else:
                s.failed_files += 1
                s.failed_bytes += size
        if ok and self.args.move and not self.args.dry_run and not self.stop_event.is_set():
            self._delete_source(rel, verify_against_dest=True)
        elif ok and self.args.move and self.args.dry_run:
            with s.lock:
                s.src_deleted += 1

    def _worker(self, idx: int) -> None:
        while True:
            job = self.jobs.get()
            if job is None:
                return
            if self.stop_event.is_set():
                continue  # drain without processing
            action, rel, size, _mtime, overwrite = job
            self.worker_status[idx] = (rel, size, time.monotonic(), action)
            try:
                if action == ACTION_COPY:
                    self._process_copy(rel, size, overwrite)
                elif self.args.dry_run:
                    with self.stats.lock:
                        self.stats.src_deleted += 1
                else:
                    self._delete_source(rel, verify_against_dest=False)
            except Exception as exc:  # noqa: BLE001 - a bad file must never kill the pool
                self._log_failure("WORKER", rel, exc)
                if action == ACTION_COPY:
                    with self.stats.lock:
                        self.stats.processed_files += 1
                        self.stats.processed_bytes += size
                        self.stats.failed_files += 1
                        self.stats.failed_bytes += size
            finally:
                self.worker_status[idx] = None

    # ---------------------------------------------------------------- move cleanup
    def _prune_empty_source_dirs(self) -> None:
        dirs = []
        for dirpath, dirnames, _files in os.walk(self.src_root, onerror=lambda e: None):
            keep = []
            for d in dirnames:
                full = os.path.join(dirpath, d)
                rel = os.path.relpath(full, self.src_root)
                if d.lower() in EXCLUDED_DIR_NAMES or self._matches_exclude(rel, d):
                    continue
                try:
                    st = os.lstat(full)
                except OSError:
                    continue
                if getattr(st, "st_file_attributes", 0) & FILE_ATTRIBUTE_REPARSE_POINT or stat.S_ISLNK(st.st_mode):
                    continue
                keep.append(d)
                dirs.append(full)
            dirnames[:] = keep
        # Children always have longer paths than their parents -> deepest first.
        for d in sorted(dirs, key=len, reverse=True):
            if self.stop_event.is_set():
                return
            try:
                os.rmdir(d)
            except OSError:
                pass

    # ---------------------------------------------------------------- pipeline
    def _pipeline(self) -> None:
        workers = []
        try:
            # Phase 1: destination index
            self.phase = "Indexing destination"
            if not self.args.dry_run:
                os.makedirs(self.dst_root, exist_ok=True)
            if os.path.isdir(self.dst_root):
                ParallelWalker(
                    self.dst_root, self.args.scan_threads, self.stop_event,
                    self._dst_dir_filter, self._index_dest_file, self._index_dest_dir,
                    self._scan_error("DEST-SCAN"), self._ignore_special,
                ).run()
            if self.stop_event.is_set():
                return

            # Phase 2 + 3: source scan feeding the transfer pool concurrently
            self.phase = "Scanning + transferring"
            self.copy_start = time.monotonic()
            for i in range(self.threads):
                t = threading.Thread(target=self._worker, args=(i,), name=f"copy-{i}", daemon=True)
                t.start()
                workers.append(t)

            ParallelWalker(
                self.src_root, self.args.scan_threads, self.stop_event,
                self._src_dir_filter, self._on_source_file, None,
                self._scan_error("SCAN"), self._on_special,
            ).run()
            self.scan_complete = True
            self.dest_index.clear()
            self.phase = "Transferring"
        except Exception as exc:  # noqa: BLE001
            self._log_failure("INTERNAL", "", exc)
            self.request_stop(f"Internal error: {exc!r}", fatal=True)
        finally:
            for _ in workers:
                self.jobs.put(None)
            for t in workers:
                t.join()

        try:
            if self.args.move and not self.args.dry_run and not self.stop_event.is_set():
                self.phase = "Removing empty source folders"
                self._prune_empty_source_dirs()
        except Exception as exc:  # noqa: BLE001
            self._log_failure("PRUNE", "", exc)
        finally:
            self.end_time = time.monotonic()
            self.phase = "Stopped" if self.stop_event.is_set() else "Finished"

    # ---------------------------------------------------------------- UI
    def _eta(self, fps: float, bps: float):
        s = self.stats
        rem_files = max(0, s.queued_files - s.processed_files)
        rem_bytes = max(0, s.queued_bytes - s.processed_bytes)
        if self.scan_complete and rem_files == 0:
            return 0.0, rem_files, rem_bytes
        estimates = []
        if fps > 0:
            estimates.append(rem_files / fps)
        if bps > 0 and rem_bytes > 0:
            estimates.append(rem_bytes / bps)
        # Tiny files are op-bound, big files are bandwidth-bound: take the conservative one.
        return (max(estimates) if estimates else None), rem_files, rem_bytes

    def _render(self):
        s = self.stats
        now = time.monotonic()
        elapsed = (self.end_time or now) - self.start_time
        width = self.console.size.width
        height = self.console.size.height
        indexing = self.phase in ("Starting", "Indexing destination")
        scanning = not indexing and not self.scan_complete and self.end_time is None

        # Header -------------------------------------------------------------
        title = Text()
        title.append(" FastSync ", style="bold black on cyan")
        title.append(" MOVE " if self.args.move else " COPY ",
                     style="bold white on dark_magenta" if self.args.move else "bold white on blue")
        if self.args.dry_run:
            title.append(" DRY RUN ", style="bold black on yellow")
        title.append(f"  {self.src_display}  ->  {self.dst_display}", style="bold")
        sub = Text.assemble(
            ("Phase: ", "dim"), (self.phase, "bold cyan"),
            ("   Workers: ", "dim"), (str(self.threads), "bold"),
            ("   Overwrite: ", "dim"), (self.args.overwrite, "bold"),
            ("   Elapsed: ", "dim"), (human_time(elapsed), "bold"),
        )
        if self.stop_reason:
            sub.append(f"   ! {self.stop_reason}", style="bold red")
        header = Panel(Group(title, sub), border_style="cyan", box=box.HEAVY)

        # Progress -----------------------------------------------------------
        bars = Table.grid(padding=(0, 1), expand=True)
        bars.add_column(width=6, style="bold")
        bars.add_column(ratio=1)
        bars.add_column(justify="right", width=7)
        bars.add_column(justify="right", min_width=34)
        if indexing:
            bars.add_row("Index", ProgressBar(total=None, pulse=True, pulse_style="cyan"), "",
                         f"{s.dest_files:,} files / {s.dest_dirs:,} folders")
            bars.add_row("", Text("Scanning destination to build skip index...", style="dim"), "", "")
        else:
            files_done = s.copied_files + s.skipped_files + s.conflict_files + s.failed_files
            bytes_done = s.copied_bytes + s.skipped_bytes + s.conflict_bytes + s.failed_bytes
            plus = "+" if scanning else ""
            fpct = 100.0 * files_done / s.scanned_files if s.scanned_files else 0.0
            bpct = 100.0 * bytes_done / s.scanned_bytes if s.scanned_bytes else 0.0
            done_style = "green" if self.end_time else "cyan"
            bars.add_row("Files", ProgressBar(total=max(1, s.scanned_files), completed=files_done,
                                              complete_style=done_style, finished_style="green"),
                         f"{fpct:5.1f}%", f"{files_done:,} / {s.scanned_files:,}{plus}")
            bars.add_row("Data", ProgressBar(total=max(1, s.scanned_bytes), completed=bytes_done,
                                             complete_style="magenta", finished_style="green"),
                         f"{bpct:5.1f}%", f"{human_bytes(bytes_done)} / {human_bytes(s.scanned_bytes)}{plus}")
        progress = Panel(bars, title="Progress", title_align="left", border_style="blue")

        # Throughput ---------------------------------------------------------
        fps, bps = self.meter.rates()
        eta, rem_files, rem_bytes = self._eta(fps, bps)
        copy_elapsed = ((self.end_time or now) - self.copy_start) if self.copy_start else 0.0
        avg_bps = s.copied_bytes / copy_elapsed if copy_elapsed > 0 else 0.0
        eta_txt = "computing..." if indexing else human_time(eta)
        if scanning and eta is not None:
            eta_txt = f">= {eta_txt} (scan running)"

        perf = Table.grid(padding=(0, 2))
        perf.add_column(style="dim")
        perf.add_column(style="bold")
        perf.add_row("Throughput", f"{bps / 1048576:,.2f} MB/s")
        perf.add_row("Operations", f"{fps:,.1f} files/s")
        perf.add_row("Average", f"{avg_bps / 1048576:,.2f} MB/s")
        perf.add_row("Remaining", f"{rem_files:,} files / {human_bytes(rem_bytes)}")
        perf.add_row("ETA", Text(eta_txt, style="bold yellow"))
        perf_panel = Panel(perf, title=f"Throughput ({int(self.args.rate_window)}s avg)",
                           title_align="left", border_style="magenta")

        counts = Table.grid(padding=(0, 2))
        counts.add_column(style="dim")
        counts.add_column(justify="right")
        counts.add_row("Scanned", f"[bold]{s.scanned_files:,}[/]")
        counts.add_row("Copied" if not self.args.dry_run else "Would copy",
                       f"[bold green]{s.copied_files:,}[/]  ({human_bytes(s.copied_bytes)})")
        counts.add_row("Skipped (identical)", f"[bold cyan]{s.skipped_files:,}[/]")
        counts.add_row("Conflicts (kept)", f"[bold yellow]{s.conflict_files:,}[/]")
        counts.add_row("Errors", f"[bold red]{s.failed_files + s.scan_errors + s.src_delete_failed:,}[/]")
        if self.args.move:
            counts.add_row("Source deleted", f"[bold]{s.src_deleted:,}[/]")
        counts_panel = Panel(counts, title="Counters", title_align="left", border_style="green")

        middle = Table.grid(expand=True, padding=(0, 1))
        middle.add_column(ratio=1)
        middle.add_column(ratio=1)
        middle.add_row(perf_panel, counts_panel)

        # Workers ------------------------------------------------------------
        snapshot = list(self.worker_status)
        active = [(i, st) for i, st in enumerate(snapshot) if st is not None]
        max_rows = max(3, height - 24)
        wt = Table(box=box.SIMPLE_HEAD, expand=True, show_edge=False, pad_edge=False)
        wt.add_column("#", justify="right", width=3, style="dim")
        wt.add_column("Current file", ratio=1, no_wrap=True, overflow="ellipsis")
        wt.add_column("Size", justify="right", width=11)
        wt.add_column("Time", justify="right", width=7)
        path_w = max(10, width - 34)
        for i, (rel, size, t0, action) in active[:max_rows]:
            label = rel if action == ACTION_COPY else f"[delete source] {rel}"
            dur = now - t0
            wt.add_row(str(i + 1), Text(left_truncate(label, path_w)), human_bytes(size),
                       Text(f"{dur:5.1f}s", style="red" if dur > 10 else ""))
        if not active:
            wt.add_row("", Text("idle" if not indexing else "waiting for destination index...", style="dim"), "", "")
        hidden = len(active) - max_rows
        caption = f"... and {hidden} more active" if hidden > 0 else None
        if caption:
            wt.caption = caption
        workers_panel = Panel(wt, title=f"Workers [{len(active)}/{self.threads} active]",
                              title_align="left", border_style="yellow")

        return Group(header, progress, middle, workers_panel)

    # ---------------------------------------------------------------- run
    def run(self) -> int:
        self.start_time = time.monotonic()
        coordinator = threading.Thread(target=self._pipeline, name="pipeline", daemon=True)
        coordinator.start()
        interval = 1.0 / max(0.5, self.args.refresh)
        interrupts = 0

        with Live(self._render(), console=self.console, auto_refresh=False,
                  screen=not self.args.no_alt_screen, transient=False,
                  redirect_stdout=False, redirect_stderr=False) as live:
            while True:
                try:
                    if not coordinator.is_alive():
                        break
                    time.sleep(interval)  # time.sleep is Ctrl+C-interruptible on Windows
                    if self.copy_start is not None:
                        self.meter.sample(self.stats.processed_files, self.stats.processed_bytes)
                    live.update(self._render(), refresh=True)
                except KeyboardInterrupt:
                    interrupts += 1
                    if interrupts == 1:
                        self.request_stop("Interrupted by user - finishing in-flight files (Ctrl+C again to force quit)")
                    else:
                        live.stop()
                        self._close_logs()
                        self.console.print("[bold red]Force quit.[/] Partial temp files will be cleaned on the next run.")
                        os._exit(130)
            live.update(self._render(), refresh=True)

        self._close_logs()
        return self._summary()

    # ---------------------------------------------------------------- summary
    def _summary(self) -> int:
        s = self.stats
        a = self.args
        end = self.end_time or time.monotonic()
        elapsed = end - self.start_time
        copy_elapsed = (end - self.copy_start) if self.copy_start else 0.0
        errors = s.failed_files + s.scan_errors + s.src_delete_failed
        not_processed = max(0, s.queued_files - s.processed_files)

        if self.fatal:
            status, style, code = f"ABORTED - {self.stop_reason}", "bold red", 3
        elif self.stop_event.is_set():
            status, style, code = "INTERRUPTED", "bold yellow", 130
        elif errors:
            status, style, code = "COMPLETED WITH ERRORS", "bold yellow", 1
        else:
            status, style, code = "COMPLETED", "bold green", 0
        if a.dry_run and code == 0:
            status += " (dry run - nothing written)"

        t = Table(title="FastSync - Execution Summary", box=box.ROUNDED, title_style="bold cyan",
                  show_header=True, header_style="bold")
        t.add_column("Metric", style="dim")
        t.add_column("Value", justify="right")
        t.add_row("Status", Text(status, style=style))
        t.add_row("Mode", ("MOVE" if a.move else "COPY") + (" / dry run" if a.dry_run else ""))
        t.add_row("Source", self.src_display)
        t.add_row("Destination", self.dst_display)
        t.add_row("Elapsed time", human_time(elapsed))
        t.add_row("Destination files indexed", f"{s.dest_files:,}")
        t.add_row("Source files scanned", f"{s.scanned_files:,}  ({human_bytes(s.scanned_bytes)})")
        t.add_row("Files copied" if not a.dry_run else "Files that would be copied",
                  f"[green]{s.copied_files:,}[/]")
        t.add_row("Data moved" if not a.dry_run else "Data that would be moved",
                  f"[green]{human_bytes(s.copied_bytes)}[/]")
        t.add_row("Skipped (identical)", f"[cyan]{s.skipped_files:,}[/]  ({human_bytes(s.skipped_bytes)})")
        t.add_row("Conflicts kept (dest differs)", f"[yellow]{s.conflict_files:,}[/]")
        t.add_row("Failed copies", f"[red]{s.failed_files:,}[/]")
        t.add_row("Scan errors", f"[red]{s.scan_errors:,}[/]")
        if a.move:
            t.add_row("Source files deleted", f"{s.src_deleted:,}")
            t.add_row("Source delete failures", f"[red]{s.src_delete_failed:,}[/]")
        if not_processed:
            t.add_row("Not processed (stopped)", f"[yellow]{not_processed:,}[/]")
        t.add_row("Excluded (system/pattern)", f"{s.excluded:,}")
        t.add_row("Links/junctions skipped", f"{s.special:,}")
        if copy_elapsed > 0:
            t.add_row("Average throughput", f"{s.copied_bytes / copy_elapsed / 1048576:,.2f} MB/s")
            t.add_row("Average file rate", f"{s.copied_files / copy_elapsed:,.1f} files/s")
        t.add_row("Error log", self.error_log_path if self._err_fh is not None else "(no errors)")
        if self._conf_fh is not None:
            t.add_row("Conflict log", self.conflict_log_path)
        self.console.print()
        self.console.print(t)

        if self.failures:
            ft = Table(title=f"Failed paths (first {min(20, len(self.failures))} of {errors:,})",
                       box=box.SIMPLE_HEAD, title_style="bold red", expand=True)
            ft.add_column("Operation", style="dim", no_wrap=True)
            ft.add_column("Path", overflow="fold")
            ft.add_column("Error", overflow="fold", style="red")
            for op, path, msg in self.failures[:20]:
                ft.add_row(op, path, msg)
            self.console.print(ft)
        if s.conflict_files and a.overwrite == "never":
            self.console.print(
                "[yellow]Tip:[/] destination files that differ were kept. "
                "Re-run with [bold]--overwrite newer[/] or [bold]--overwrite changed[/] to replace them."
            )
        return code


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
EPILOG = r"""
examples:
  Copy everything from E:\ to D:\ (resumable, skips identical files):
      python fastsync.py E:\ D:\

  32 workers, copy into a sub-folder, verify sizes:
      python fastsync.py E:\ D:\Backup\E-drive -t 32 --verify

  Move (delete source after a verified copy), replace dest files only if source is newer:
      python fastsync.py E:\ D:\ --move --overwrite newer

  Preview without writing anything:
      python fastsync.py E:\ D:\ --dry-run

  NOTE: do not write "E:\" in quotes - Windows treats \" as an escaped quote.
        Use E:\ unquoted, or "E:\\" / "D:\My Files" (no trailing backslash).
"""


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="fastsync",
        description="High-throughput differential copy/move for millions of small files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EPILOG,
    )
    p.add_argument("source", help="source folder or drive root, e.g. E:\\")
    p.add_argument("dest", help="destination folder or drive root, e.g. D:\\")
    p.add_argument("-t", "--threads", type=int, default=16,
                   help="transfer worker threads (default 16; try 32 for SSDs, 8 for spinning HDDs)")
    p.add_argument("--scan-threads", type=int, default=8,
                   help="directory enumeration threads (default 8)")
    p.add_argument("--move", action="store_true",
                   help="delete each source file after it is copied and size-verified "
                        "(also deletes sources already identical at destination), then prune empty folders")
    p.add_argument("--overwrite", choices=("never", "newer", "changed"), default="never",
                   help="when dest exists but differs: never (default, keep dest), "
                        "newer (replace if source mtime is newer), changed (always replace)")
    p.add_argument("--mtime-tolerance", type=float, default=2.0, metavar="SEC",
                   help="timestamp tolerance in seconds when comparing (default 2.0 for FAT32/exFAT)")
    p.add_argument("-x", "--exclude", action="append", default=[], metavar="GLOB",
                   help="extra exclude pattern matched against name or relative path (repeatable), "
                        "e.g. -x Thumbs.db -x \"*.tmp\" -x \"Temp\\*\"")
    p.add_argument("--dry-run", action="store_true", help="scan and report only; write nothing")
    p.add_argument("--verify", action="store_true",
                   help="stat the copied file and compare its size with the source")
    p.add_argument("--no-atomic", action="store_true",
                   help="write directly to the final name (one less metadata op per file; "
                        "an interrupted file may remain partial)")
    p.add_argument("--retry-delay", type=float, default=0.5, metavar="SEC",
                   help="pause before the single retry of a failed file (default 0.5)")
    p.add_argument("--max-queue", type=int, default=1_000_000,
                   help="max pending transfer jobs held in RAM (0 = unlimited, default 1,000,000)")
    p.add_argument("--rate-window", type=float, default=30.0, metavar="SEC",
                   help="moving-average window for throughput/ETA (default 30)")
    p.add_argument("--refresh", type=float, default=4.0, help="dashboard refreshes per second (default 4)")
    p.add_argument("--log-dir", default=".", help="where error/conflict logs are written (default: current dir)")
    p.add_argument("--no-alt-screen", action="store_true",
                   help="draw the dashboard inline instead of in the alternate screen buffer")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return p.parse_args(argv)


def validate(args: argparse.Namespace) -> str | None:
    for label, value in (("source", args.source), ("destination", args.dest)):
        if '"' in value:
            return (f'The {label} argument contains a quote character ({value!r}).\n'
                    'Windows treats \\" as an escaped quote - write E:\\ unquoted or use "E:\\\\".')
    if not 1 <= args.threads <= 512:
        return "--threads must be between 1 and 512"
    if not 1 <= args.scan_threads <= 128:
        return "--scan-threads must be between 1 and 128"
    if args.mtime_tolerance < 0 or args.retry_delay < 0:
        return "--mtime-tolerance and --retry-delay must be >= 0"
    if args.rate_window < 2:
        return "--rate-window must be >= 2 seconds"

    src = normalize_root(args.source)
    dst = normalize_root(args.dest)
    if not os.path.isdir(to_extended(src)):
        return f"Source folder does not exist or is not accessible: {src}"
    ns, nd = os.path.normcase(src), os.path.normcase(dst)
    ns_slash, nd_slash = ns.rstrip("\\/") + os.sep, nd.rstrip("\\/") + os.sep
    if ns == nd or nd.startswith(ns_slash) or ns.startswith(nd_slash):
        return f"Source and destination must not overlap:\n  {src}\n  {dst}"
    try:
        os.makedirs(os.path.abspath(args.log_dir), exist_ok=True)
    except OSError as exc:
        return f"Cannot create log directory {args.log_dir}: {exc}"
    return None


def main(argv=None) -> int:
    args = parse_args(argv)
    console = Console(highlight=False)
    problem = validate(args)
    if problem:
        console.print(f"[bold red]Error:[/] {problem}")
        return 2
    return FastSync(args, console).run()


if __name__ == "__main__":
    sys.exit(main())
