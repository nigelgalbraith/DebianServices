#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""rsync_backup.py (template).

rsync_backup.py

Purpose
-------
Run structured, per-job rsync snapshots (Timeshift-like), typically during
system shutdown (poweroff only), with full control over backup location,
naming, folder protection, retention, and frequency.

This script is designed to be executed:
- Manually by an administrator, or
- Automatically via a systemd oneshot service bound to poweroff.target


Key Features
------------
- Multiple backup jobs defined in a single JSON config file
- Rsync snapshot style using hardlinks (--link-dest) for efficiency
- Atomic snapshot creation: sync into a .tmp directory then rename
- Per-job backup frequency control (Immediately/Daily/Weekly/Monthly)
- Manual runs always create a new snapshot regardless of frequency
- Per-job retention policies (keep newest N snapshots)
- Optional folder ownership/permission enforcement on backup destinations
- Safe dry-run mode when executed under a non-production filename
- Simple, timestamped stdout logging


Dry-Run Mode
------------
If the script filename does NOT match the production script name
(e.g. running a template or copied version), the script automatically
runs in dry-run mode.

In dry-run mode:
- No snapshot directories are created or deleted
- Rsync runs in --dry-run mode
- All actions are logged as "[DRY-RUN]" entries


Snapshot Design
---------------
Each run creates a new snapshot directory under:

  backup_root / job_key / <snapshot_name>

Snapshots are created using rsync with --link-dest pointing at the previous
snapshot directory, so unchanged files are hardlinked (fast and space efficient),
while changed files are copied.

Note:
- Deletions in the source are naturally reflected because each snapshot starts
  as a new destination; files absent from source will simply not appear in the
  new snapshot.
- If you want to exclude folders (e.g. caches), use the job's 'exclude' list.


Configuration
-------------
Config is JSON with a non-empty 'backup_jobs' object. Each job may specify:
- source_dir (required)
- backup_root (required)
- keep (optional; default 14)
- snapshot_prefix (optional; default "home")
- backupFrequency (optional; default "Immediately")
- exclude (optional list of rsync exclude patterns)
- rsync_extra_args (optional list of extra rsync args)
- protected_folder (optional: owner/group/permissions)


Logging
-------
All actions are logged with timestamps to stdout (systemd can capture logs).

"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List


# =====================
# CONSTANTS
# =====================
SCRIPT_NAME = "rsync_backup.py"
PROD_CONFIG = "/etc/rsync_backup_config.json"
TEST_CONFIG_NAME = "rsync_backup_config-template.json"

BACKUP_WORD = "backup"
TS_FMT = "%Y-%m-%d_%H-%M-%S"
FREQ_VALUES = {"immediately", "daily", "weekly", "monthly"}


# =====================
# DATA MODELS
# =====================
@dataclass
class Job:
    """Hold one rsync snapshot job."""
    key: str
    source_dir: Path
    backup_root: Path
    keep: int
    snapshot_prefix: str
    backup_frequency: str
    backup_group: Optional[str]
    backup_users: List[str]
    exclude: List[str]
    rsync_extra_args: List[str]
    protect_owner: Optional[str]
    protect_group: Optional[str]
    protect_perms: Optional[str]

# =====================
# LOGGING & CONFIG
# =====================
def log_message(message: str) -> None:
    """Print a message with timestamp (YYYY-MM-DD HH:MM:SS)."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} : {message}", flush=True)


def get_config_and_mode(script_filename: str, prod_script_name: str, prod_config: str, test_config_name: str) -> tuple[Path, bool]:
    """Return (config_path, dry_run) based on script name."""
    if script_filename == prod_script_name:
        return Path(prod_config), False
    script_path = Path(__file__).resolve()
    return script_path.with_name(test_config_name), True


def load_json(path: Path) -> Dict[str, Any]:
    """Load JSON file as dict."""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a JSON object: {path}")
    return data


# =====================
# SYSTEMD HELPERS
# =====================
def shutdown_in_progress() -> bool:
    """Return True only when the system is actually shutting down."""
    proc = subprocess.run(["systemctl", "is-system-running"], capture_output=True, text=True)
    state = proc.stdout.strip().lower()
    return state == "stopping"


# =====================
# FILESYSTEM HELPERS
# =====================
def ensure_backup_group(group: str, users: list[str], dry_run: bool) -> None:
    """Ensure a group exists and required users are members of it."""
    grp_check = subprocess.run(["getent", "group", group], capture_output=True, text=True)

    if grp_check.returncode == 0:
        if dry_run:
            log_message(f"[DRY-RUN] Group already exists: {group}")
    else:
        if dry_run:
            log_message(f"[DRY-RUN] Would create group: {group}")
        else:
            log_message(f"Creating group: {group}")
            proc = subprocess.run(["groupadd", group], capture_output=True, text=True)
            if proc.returncode != 0:
                log_message(f"[WARN] groupadd {group} failed: {proc.stderr.strip()}")
                return

    for user in sorted({u.strip() for u in users if isinstance(u, str) and u.strip()}):
        usr_check = subprocess.run(["id", "-nG", user], capture_output=True, text=True)
        if usr_check.returncode != 0:
            log_message(f"[WARN] User does not exist: {user}")
            continue

        if group in usr_check.stdout.split():
            continue

        if dry_run:
            log_message(f"[DRY-RUN] Would add user '{user}' to group '{group}'")
            continue

        log_message(f"Adding user '{user}' to group '{group}'")
        proc = subprocess.run(["usermod", "-aG", group, user], capture_output=True, text=True)
        if proc.returncode != 0:
            log_message(f"[WARN] usermod failed for {user}: {proc.stderr.strip()}")


def apply_folder_protection(folder: Path, owner: str, group: str, perms: str, dry_run: bool) -> None:
    """Apply ownership and permissions to a folder."""
    if dry_run:
        log_message(f"[DRY-RUN] Would mkdir -p {folder}")
        log_message(f"[DRY-RUN] Would chown {owner}:{group} {folder}")
        log_message(f"[DRY-RUN] Would chmod {perms} {folder}")
        return

    folder.mkdir(parents=True, exist_ok=True)

    chown = subprocess.run(["chown", f"{owner}:{group}", str(folder)], capture_output=True, text=True)
    if chown.returncode != 0:
        log_message(f"[WARN] chown failed on {folder}: {chown.stderr.strip()}")

    chmod = subprocess.run(["chmod", perms, str(folder)], capture_output=True, text=True)
    if chmod.returncode != 0:
        log_message(f"[WARN] chmod failed on {folder}: {chmod.stderr.strip()}")

    log_message(f"Protected folder applied: {folder}")


# =====================
# RETENTION & FREQUENCY
# =====================
def get_latest_snapshot(dest_folder: Path, name_prefix: str) -> Optional[Path]:
    """Return newest snapshot dir matching name_prefix_, or None."""
    if not dest_folder.exists():
        return None

    candidates: List[Path] = []
    for p in dest_folder.iterdir():
        if not p.is_dir():
            continue
        if p.name.endswith(".tmp"):
            continue
        if not p.name.startswith(name_prefix + "_"):
            continue
        candidates.append(p)

    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def get_latest_snapshot_mtime(dest_folder: Path, name_prefix: str) -> Optional[datetime]:
    """Return datetime of newest snapshot dir matching name_prefix_, or None."""
    latest = get_latest_snapshot(dest_folder, name_prefix)
    if not latest:
        return None
    return datetime.fromtimestamp(latest.stat().st_mtime)


def should_run_backup(*, frequency: str, latest_backup_time: Optional[datetime], now: datetime, manual_trigger: bool) -> bool:
    """Return True if a new snapshot should be created."""
    if manual_trigger:
        return True
    if frequency == "immediately":
        return True
    if latest_backup_time is None:
        return True
    if frequency == "daily":
        return latest_backup_time.date() != now.date()
    if frequency == "weekly":
        return latest_backup_time.isocalendar()[:2] != now.isocalendar()[:2]
    if frequency == "monthly":
        return (latest_backup_time.year, latest_backup_time.month) != (now.year, now.month)
    return True


def enforce_retention(dest_folder: Path, keep: int, name_prefix: str, dry_run: bool) -> None:
    """Keep newest keep snapshot dirs matching name_prefix_; delete older ones."""
    if keep <= 0:
        return
    if not dest_folder.exists():
        log_message(f"[INFO] Retention skipped (folder does not exist): {dest_folder}")
        return

    candidates: List[Path] = []
    for p in dest_folder.iterdir():
        if not p.is_dir():
            continue
        if p.name.endswith(".tmp"):
            continue
        if not p.name.startswith(name_prefix + "_"):
            continue
        candidates.append(p)

    snapshots = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
    for p in snapshots[keep:]:
        if dry_run:
            log_message(f"[DRY-RUN] Would delete old snapshot: {p}")
        else:
            try:
                subprocess.run(["rm", "-rf", str(p)], check=False, capture_output=True, text=True)
                log_message(f"Deleted old snapshot: {p}")
            except Exception as e:
                log_message(f"[WARN] Failed to delete {p}: {e!r}")


# =====================
# RSYNC HELPERS
# =====================
def rsync_snapshot(source_dir: Path, snapshot_path: Path, previous_snapshot: Optional[Path], exclude: List[str], extra_args: List[str], dry_run: bool) -> bool:
    """Create an rsync snapshot into snapshot_path (atomic via .tmp then rename)."""
    tmp_path = snapshot_path.with_name(snapshot_path.name + ".tmp")

    rsync_cmd: List[str] = ["rsync", "-aHAX", "--numeric-ids", "--delete-delay", "--partial", "--inplace"]

    # Excludes
    for pat in exclude:
        rsync_cmd.extend(["--exclude", pat])

    # Hardlink to previous snapshot for unchanged files
    if previous_snapshot:
        rsync_cmd.extend(["--link-dest", str(previous_snapshot)])

    # Extra args (user-defined)
    rsync_cmd.extend(extra_args)

    if dry_run:
        rsync_cmd.append("--dry-run")

    # Ensure destination exists (tmp)
    if dry_run:
        log_message(f"[DRY-RUN] Would mkdir -p {tmp_path}")
    else:
        tmp_path.mkdir(parents=True, exist_ok=True)

    # Ensure source has trailing slash (copy contents into dest)
    src = str(source_dir.resolve()) + "/"
    dst = str(tmp_path.resolve()) + "/"

    rsync_cmd.extend([src, dst])

    log_message(f"{'[DRY-RUN] ' if dry_run else ''}Running: {' '.join(rsync_cmd)}")
    proc = subprocess.run(rsync_cmd, capture_output=True, text=True)

    if proc.returncode != 0:
        log_message(f"[ERROR] rsync failed (rc={proc.returncode})")
        if proc.stdout.strip():
            log_message(f"[ERROR] rsync stdout: {proc.stdout.strip()}")
        if proc.stderr.strip():
            log_message(f"[ERROR] rsync stderr: {proc.stderr.strip()}")

        if not dry_run:
            try:
                subprocess.run(["rm", "-rf", str(tmp_path)], check=False, capture_output=True, text=True)
            except Exception:
                pass
        return False

    # Atomic rename tmp -> final snapshot folder
    if dry_run:
        log_message(f"[DRY-RUN] Would rename {tmp_path} -> {snapshot_path}")
        return True

    try:
        tmp_path.replace(snapshot_path)
        return True
    except Exception as e:
        log_message(f"[ERROR] Failed to finalize snapshot rename: {e!r}")
        try:
            subprocess.run(["rm", "-rf", str(tmp_path)], check=False, capture_output=True, text=True)
        except Exception:
            pass
        return False


# =====================
# MAIN
# =====================
def main() -> int:
    """Run rsync snapshots for all jobs in config."""
    script_filename = Path(__file__).name
    cfg_path, dry_run = get_config_and_mode(script_filename, SCRIPT_NAME, PROD_CONFIG, TEST_CONFIG_NAME)

    log_message(f"home_rsync_backup starting (dry_run={dry_run})")
    log_message(f"Config: {cfg_path}")
    log_message(
        f"systemd state: "
        f"{subprocess.run(['systemctl','is-system-running'], capture_output=True, text=True).stdout.strip()}"
    )

    is_shutdown_trigger = shutdown_in_progress()
    if is_shutdown_trigger:
        log_message("TRIGGER: SHUTDOWN")
    else:
        log_message("TRIGGER: MANUAL/NORMAL")

    if not cfg_path.exists():
        log_message(f"[ERROR] Config not found: {cfg_path}")
        return 2

    try:
        cfg = load_json(cfg_path)
    except Exception as e:
        log_message(f"[ERROR] Failed to load config: {e!r}")
        return 2

    # Parse and validate jobs
    try:
        jobs_cfg = cfg.get("backup_jobs", {})
        if not isinstance(jobs_cfg, dict) or not jobs_cfg:
            raise ValueError("Config must contain a non-empty 'backup_jobs' object.")

        jobs: List[Job] = []
        for key, spec in jobs_cfg.items():
            if not isinstance(spec, dict):
                raise ValueError(f"Job '{key}' must be an object.")

            src = spec.get("source_dir")
            root = spec.get("backup_root")
            keep = spec.get("keep", 14)

            if not isinstance(src, str) or not src.strip():
                raise ValueError(f"Job '{key}' missing/invalid 'source_dir'.")
            if not isinstance(root, str) or not root.strip():
                raise ValueError(f"Job '{key}' missing/invalid 'backup_root'.")
            if not isinstance(keep, int):
                raise ValueError(f"Job '{key}' has non-integer 'keep'.")

            snapshot_prefix = spec.get("snapshot_prefix", "home")
            if not isinstance(snapshot_prefix, str) or not snapshot_prefix.strip():
                snapshot_prefix = "home"

            freq_raw = spec.get("backupFrequency", "Immediately")
            freq = freq_raw.strip().lower() if isinstance(freq_raw, str) else "immediately"
            if freq not in FREQ_VALUES:
                raise ValueError(
                    f"Job '{key}' has invalid backupFrequency '{freq_raw}'. "
                    f"Allowed: {sorted(FREQ_VALUES)}"
                )

            exclude_list: List[str] = []
            exclude_raw = spec.get("exclude", [])
            if isinstance(exclude_raw, list):
                for item in exclude_raw:
                    if isinstance(item, str) and item.strip():
                        exclude_list.append(item.strip())

            extra_args_list: List[str] = []
            extra_raw = spec.get("rsync_extra_args", [])
            if isinstance(extra_raw, list):
                for item in extra_raw:
                    if isinstance(item, str) and item.strip():
                        extra_args_list.append(item.strip())

            backup_group_raw = spec.get("backupGroup")
            backup_group = backup_group_raw.strip() if isinstance(backup_group_raw, str) else None
            backup_group = backup_group if backup_group else None
            backup_users_raw = spec.get("backupUsers", [])
            backup_users: List[str] = []
            if isinstance(backup_users_raw, list):
                backup_users = [u.strip() for u in backup_users_raw if isinstance(u, str) and u.strip()]


            protected = spec.get("protected_folder", {})
            protect_owner = None
            protect_group = None
            protect_perms = None
            if isinstance(protected, dict):
                o = protected.get("owner")
                g = protected.get("group")
                p = protected.get("permissions")
                protect_owner = o if isinstance(o, str) and o.strip() else None
                protect_group = g if isinstance(g, str) and g.strip() else None
                protect_perms = p if isinstance(p, str) and p.strip() else None

            jobs.append(
                Job(
                    key=key,
                    source_dir=Path(src),
                    backup_root=Path(root),
                    keep=keep,
                    snapshot_prefix=snapshot_prefix.strip(),
                    backup_frequency=freq,
                    backup_group=backup_group,
                    backup_users=backup_users,
                    exclude=exclude_list,
                    rsync_extra_args=extra_args_list,
                    protect_owner=protect_owner,
                    protect_group=protect_group,
                    protect_perms=protect_perms,
                )
            )

    except Exception as e:
        log_message(f"[ERROR] Failed to parse jobs: {e!r}")
        return 2

    # Execute jobs
    processed = 0
    now = datetime.now()

    for job in jobs:
        try:
            log_message(f"==> Job '{job.key}'")
            log_message(f"Source: {job.source_dir}")

            dest_folder = job.backup_root / job.key

            # Ensure backup group + membership (even if rsync is skipped)
            if job.backup_group:
                ensure_backup_group(job.backup_group, job.backup_users, dry_run)

            # Always apply folder protection when configured (even if rsync is skipped)
            if (not dry_run) and job.protect_owner and job.protect_group and job.protect_perms:
                apply_folder_protection(
                    dest_folder,
                    job.protect_owner,
                    job.protect_group,
                    job.protect_perms,
                    dry_run=False,
                )

                st = subprocess.run(
                    ["stat", "-c", "%U:%G %a %n", str(dest_folder)],
                    capture_output=True,
                    text=True,
                )
                if st.returncode == 0:
                    log_message(f"Protected folder result: {st.stdout.strip()}")
                else:
                    log_message(f"[WARN] stat failed on {dest_folder}: {st.stderr.strip()}")

            log_message(f"Dest:   {dest_folder}")

            # Frequency check:
            # - Manual trigger: ALWAYS run backup regardless of frequency
            # - Shutdown trigger: run only if frequency says it's due
            manual_trigger = not is_shutdown_trigger

            name_prefix = f"{job.key}_{job.snapshot_prefix}_{BACKUP_WORD}"
            latest_time = get_latest_snapshot_mtime(dest_folder, name_prefix)

            if not should_run_backup(frequency=job.backup_frequency, latest_backup_time=latest_time, now=now, manual_trigger=manual_trigger):
                log_message(
                    f"[SKIP] Frequency '{job.backup_frequency}' not due. "
                    f"Latest snapshot: {latest_time.strftime('%Y-%m-%d %H:%M:%S') if latest_time else 'None'}"
                )
                continue

            # Validate source exists
            if not job.source_dir.exists():
                log_message(f"[ERROR] Source does not exist: {job.source_dir}")
                continue

            # Determine previous snapshot directory for link-dest
            previous_snapshot = get_latest_snapshot(dest_folder, name_prefix)

            # Create snapshot dir name
            ts = now.strftime(TS_FMT)
            snapshot_name = f"{name_prefix}_{ts}"
            snapshot_path = dest_folder / snapshot_name

            ok = rsync_snapshot(job.source_dir, snapshot_path, previous_snapshot, job.exclude, job.rsync_extra_args, dry_run)

            if ok:
                log_message(f"[OK] Created snapshot: {snapshot_path}")
                enforce_retention(dest_folder, job.keep, name_prefix, dry_run)
                processed += 1
            else:
                log_message("[FAIL] Snapshot creation failed.")

        except KeyboardInterrupt:
            log_message("Interrupted by user.")
            return 130
        except Exception as e:
            log_message(f"[ERROR] Job '{job.key}' failed: {e!r}")

    log_message(f"DONE: {processed}/{len(jobs)} job(s) processed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
