#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
config_backup.py

Purpose
-------
Run structured configuration backups to compressed archives based on jobs
defined in a JSON configuration file.

This script is intended to run either:

• Manually by an administrator  
• Automatically via a systemd oneshot service attached to poweroff.target  

Each configured job can back up a directory to a compressed archive while
optionally stopping services, enforcing folder permissions, and applying
retention rules.


Key Features
------------
• Multiple backup jobs defined in one JSON config file  
• Supports tar.gz and zip archive formats  
• Optional systemd service stop/start per job (e.g. plexmediaserver)  
• Automatic restart of services unless shutdown is in progress  
• Per-job retention policies (keep newest N backups)  
• Optional backup destination ownership and permission enforcement  
• Per-job backup frequency control (Immediately / Daily / Weekly / Monthly)  
• Manual execution always performs a backup regardless of frequency  
• Safe dry-run mode for testing  
• Simple timestamped logging


Execution Safety
----------------
The script determines **live vs dry-run mode** using two checks:

1) Explicit argument

    --dry-run

   This always forces dry-run mode and uses the template config located
   next to the script.

2) Script filename check

   If the script filename is exactly:

       config_backup.py

   the script runs in **live mode** and uses:

       /etc/config_backup_config.json

   If executed under any other filename, the script automatically switches
   to **dry-run mode** and uses:

       config_backup_config-template.json


Dry-Run Mode
------------
In dry-run mode:

• No files are created or deleted  
• No services are stopped or started  
• No system configuration changes occur  
• All actions are logged with "[DRY-RUN]" messages  

This allows safe testing of configuration and logic.


Shutdown Behavior
-----------------
When the script runs during system shutdown:

• Services configured in a job are stopped if currently running  
• Backups are created  
• Services are **NOT restarted** because the system is shutting down  

When run manually:

• Services are restarted after backup if they were stopped.


Backup Frequency
----------------
Jobs may specify:

    backupFrequency

Supported values (case-insensitive):

• Immediately  
• Daily  
• Weekly  
• Monthly  

Behavior:

Manual execution:
    Frequency checks are ignored and backups always run.

Shutdown execution:
    A backup runs only if the newest existing archive is older than the
    configured frequency window.


Configuration
-------------
Configuration is loaded from JSON and must define a top-level object:

    "backup_jobs"

Each job can specify:

• source_dir          – directory to back up  
• backup_root         – destination root folder  
• keep                – number of archives to retain  
• archive_prefix      – filename prefix for generated archives  
• format              – archive format (tar.gz or zip)  
• backupFrequency     – Immediately / Daily / Weekly / Monthly  

Optional fields:

• stop_service        – systemd service to stop before backup  
• timeout_stop_sec    – seconds to wait for service to stop  
• backupGroup         – group to ensure exists for backup access  
• backupUsers         – users added to backup group  
• protected_folder    – enforce owner/group/permissions on destination  

Example job:

{
  "backup_jobs": {
    "plex_config": {
      "source_dir": "/var/lib/plexmediaserver",
      "backup_root": "/mnt/backups",
      "keep": 14,
      "archive_prefix": "plex",
      "format": "tar.gz",
      "backupFrequency": "Daily",
      "stop_service": "plexmediaserver"
    }
  }
}


Logging
-------
All operations log timestamped messages to stdout.

Logging to files is normally handled by systemd using:

    StandardOutput
    StandardError

This avoids duplicate logging and keeps the script simple.
"""

from __future__ import annotations

import json
import subprocess
import zipfile
import os
import sys
import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List


# =====================
# CONSTANTS
# =====================
SCRIPT_NAME = "config_backup.py"
PROD_CONFIG = "/etc/config_backup_config.json"
TEST_CONFIG_NAME = "config_backup_config-template.json"
BACKUP_WORD = "backup"
TS_FMT = "%Y-%m-%d_%H-%M-%S"
ARCHIVE_FORMATS = {"tar.gz", "zip"}
FREQ_VALUES = {"immediately", "daily", "weekly", "monthly"}
ARG_DESCRIPTION = "Run structured, per-job backups to compressed archives, typically during system shutdown (poweroff only)."


# =====================
# DATA MODELS
# =====================
@dataclass
class Job:
    """Hold one backup job."""
    key: str
    source_dir: Path
    backup_root: Path
    keep: int
    archive_prefix: str
    fmt: str
    stop_service: Optional[str]
    timeout_stop_sec: int
    backup_frequency: str
    backup_group: Optional[str]
    backup_users: List[str]
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


def load_json(path: Path) -> Dict[str, Any]:
    """Load JSON file as dict."""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a JSON object: {path}")
    return data

def parse_args(description: str):
    parser = argparse.ArgumentParser(
        description=description
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show actions without executing"
    )
    return parser.parse_args()


# =====================
# SYSTEMD HELPERS
# =====================
def systemctl_is_active(service: str) -> bool:
    """Return True if service is active."""
    proc = subprocess.run(["systemctl", "is-active", service], capture_output=True, text=True)
    return proc.returncode == 0 and proc.stdout.strip() == "active"


def systemctl_start(service: str, dry_run: bool) -> None:
    """Start a systemd service (best-effort)."""
    if dry_run:
        log_message(f"[DRY-RUN] Would start service: {service}")
        return
    log_message(f"Starting service: {service}")
    proc = subprocess.run(["systemctl", "start", service], capture_output=True, text=True)
    if proc.returncode != 0:
        log_message(f"[WARN] systemctl start {service} failed: {proc.stderr.strip()}")


def shutdown_in_progress() -> bool:
    """Return True only when the system is actually shutting down."""
    proc = subprocess.run(["systemctl", "is-system-running"], capture_output=True, text=True)
    state = proc.stdout.strip().lower()
    return state == "stopping"


def systemctl_stop(service: str, timeout_sec: int, dry_run: bool) -> None:
    """Stop a systemd service (best-effort) without break."""
    if dry_run:
        log_message(f"[DRY-RUN] Would stop service: {service}")
        return
    log_message(f"Stopping service: {service}")
    proc = subprocess.run(["systemctl", "stop", service], capture_output=True, text=True)
    if proc.returncode != 0:
        log_message(f"[WARN] systemctl stop {service} failed: {proc.stderr.strip()}")
        return
    for _ in range(max(1, timeout_sec)):
        st = subprocess.run(["systemctl", "is-active", service], capture_output=True, text=True)
        state = st.stdout.strip()
        if st.returncode != 0 or state in ("inactive", "failed", "unknown"):
            return
    log_message(f"[WARN] Service still active after {timeout_sec}s: {service}")


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
    chown = subprocess.run(
        ["chown", f"{owner}:{group}", str(folder)],
        capture_output=True,
        text=True,
    )
    if chown.returncode != 0:
        log_message(f"[WARN] chown failed on {folder}: {chown.stderr.strip()}")
    chmod = subprocess.run(
        ["chmod", perms, str(folder)],
        capture_output=True,
        text=True,
    )
    if chmod.returncode != 0:
        log_message(f"[WARN] chmod failed on {folder}: {chmod.stderr.strip()}")
    log_message(f"Protected folder applied: {folder}")


# =====================
# ARCHIVE HELPERS
# =====================
def zip_directory(source_dir: Path, archive_path: Path, dry_run: bool) -> bool:
    """Create zip archive of source_dir at archive_path atomically."""
    tmp_path = archive_path.with_suffix(archive_path.suffix + ".tmp")
    if dry_run:
        log_message(f"[DRY-RUN] Would zip '{source_dir}' -> '{archive_path}'")
        return True
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in source_dir.rglob("*"):
                if p.is_dir():
                    continue
                zf.write(p, Path(source_dir.name) / p.relative_to(source_dir))
        tmp_path.replace(archive_path)
        return True
    except Exception as e:
        log_message(f"[ERROR] zip failed: {e!r}")
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return False


def tar_directory(source_dir: Path, archive_path: Path, dry_run: bool) -> bool:
    """Create tar.gz archive of source_dir at archive_path atomically."""
    tmp_path = archive_path.with_suffix(archive_path.suffix + ".tmp")
    if dry_run:
        log_message(f"[DRY-RUN] Would archive '{source_dir}' -> '{archive_path}'")
        return True
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [
            "tar",
            "-czf",
            str(tmp_path),
            "-C",
            str(source_dir.parent),
            str(source_dir.name),
        ],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        log_message(f"[ERROR] tar failed (rc={proc.returncode})")
        if proc.stdout.strip():
            log_message(f"[ERROR] tar stdout: {proc.stdout.strip()}")
        if proc.stderr.strip():
            log_message(f"[ERROR] tar stderr: {proc.stderr.strip()}")
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return False
    tmp_path.replace(archive_path)
    return True


# =====================
# RETENTION & FREQUENCY
# =====================
def enforce_retention(folder: Path, keep: int, prefix: str, ext: str, dry_run: bool) -> None:
    """Keep newest keep archives matching prefix; delete older ones."""
    if keep <= 0:
        return
    if not folder.exists():
        log_message(f"[INFO] Retention skipped (folder does not exist): {folder}")
        return
    candidates: List[Path] = []
    for p in folder.iterdir():
        if not p.is_file():
            continue
        if not p.name.endswith("." + ext):
            continue
        if not p.name.startswith(prefix + "_"):
            continue
        candidates.append(p)
    archives = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
    for p in archives[keep:]:
        if dry_run:
            log_message(f"[DRY-RUN] Would delete old archive: {p}")
        else:
            try:
                p.unlink()
                log_message(f"Deleted old archive: {p}")
            except Exception as e:
                log_message(f"[WARN] Failed to delete {p}: {e!r}")


def get_latest_archive_mtime(dest_folder: Path, prefix: str, ext: str) -> Optional[datetime]:
    """Return datetime of newest matching archive in dest_folder, or None."""
    if not dest_folder.exists():
        return None
    candidates: List[Path] = []
    for p in dest_folder.iterdir():
        if not p.is_file():
            continue
        if p.name.endswith(".tmp"):
            continue
        if not p.name.endswith("." + ext):
            continue
        if not p.name.startswith(prefix + "_"):
            continue
        candidates.append(p)

    if not candidates:
        return None
    newest = max(candidates, key=lambda p: p.stat().st_mtime)
    return datetime.fromtimestamp(newest.stat().st_mtime)


def should_run_backup(*, frequency: str, latest_backup_time: Optional[datetime], now: datetime, manual_trigger: bool) -> bool:
    """Return True if a new backup should be created."""
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


# =====================
# MAIN
# =====================
def main() -> int:
    """Run backups for all jobs in config."""
    # Decide config + dry-run based on script filename or arguments
    args = parse_args(ARG_DESCRIPTION)
    script_basename = os.path.basename(sys.argv[0])

    if args.dry_run:
        cfg_path = Path(__file__).resolve().with_name(TEST_CONFIG_NAME)
        dry_run = True
        log_message(
            f"Argument '--dry-run' detected — "
            f"running in DRY-RUN with test config '{cfg_path}'."
        )
    elif script_basename == SCRIPT_NAME:
        cfg_path = Path(PROD_CONFIG)
        dry_run = False
    else:
        cfg_path = Path(__file__).resolve().with_name(TEST_CONFIG_NAME)
        dry_run = True
        log_message(
            f"Script name '{script_basename}' != '{SCRIPT_NAME}' — "
            f"running in DRY-RUN with test config '{cfg_path}'."
        )
    log_message(f"Using config: {cfg_path}")
    log_message(f"Dry run: {dry_run}")

    log_message(f"config_backup starting (dry_run={dry_run})")
    log_message(f"Config: {cfg_path}")
    log_message(
        f"systemd state: "
        f"{subprocess.run(['systemctl','is-system-running'], capture_output=True, text=True).stdout.strip()}"
    )

    # Determine trigger mode.
    # - If systemd is in 'stopping', we treat this run as shutdown-triggered
    # - Otherwise, we treat it as manual/normal
    is_shutdown_trigger = shutdown_in_progress()
    if is_shutdown_trigger:
        log_message("TRIGGER: SHUTDOWN")
    else:
        log_message("TRIGGER: MANUAL/NORMAL")

    # Load configuration file
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
            fmt = spec.get("format", "tar.gz")
            if fmt not in ARCHIVE_FORMATS:
                raise ValueError(
                    f"Job '{key}' unsupported format '{fmt}'. Supported: {sorted(ARCHIVE_FORMATS)}"
                )
            archive_prefix = str(spec.get("archive_prefix", "config"))
            stop_service_raw = spec.get("stop_service")
            stop_service = stop_service_raw.strip() if isinstance(stop_service_raw, str) else None
            stop_service = stop_service if stop_service else None
            timeout_stop_sec = int(spec.get("timeout_stop_sec", 30))
            # Backup frequency (case-insensitive)
            freq_raw = spec.get("backupFrequency", "Immediately")
            freq = freq_raw.strip().lower() if isinstance(freq_raw, str) else "immediately"
            if freq not in FREQ_VALUES:
                raise ValueError(
                    f"Job '{key}' has invalid backupFrequency '{freq_raw}'. "
                    f"Allowed: {sorted(FREQ_VALUES)}"
                )
            # Backup groups and users
            backup_group_raw = spec.get("backupGroup")
            backup_group = backup_group_raw.strip() if isinstance(backup_group_raw, str) else None
            backup_group = backup_group if backup_group else None
            backup_users_raw = spec.get("backupUsers", [])
            backup_users: List[str] = []
            if isinstance(backup_users_raw, list):
                backup_users = [u.strip() for u in backup_users_raw if isinstance(u, str) and u.strip()]
            # File protection
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
                    archive_prefix=archive_prefix,
                    fmt=fmt,
                    stop_service=stop_service,
                    timeout_stop_sec=timeout_stop_sec,
                    backup_frequency=freq,
                    backup_group=backup_group,
                    backup_users=backup_users,
                    protect_owner=protect_owner,
                    protect_group=protect_group,
                    protect_perms=protect_perms,
                )
            )
    except Exception as e:
        log_message(f"[ERROR] Failed to parse jobs: {e!r}")
        return 2
    # Execute backup jobs
    processed = 0
    for job in jobs:
        try:
            log_message(f"==> Job '{job.key}'")
            log_message(f"Source: {job.source_dir}")
            dest_folder = job.backup_root / job.key
            # Ensure backup group + membership (even if backup is skipped)
            if job.backup_group:
                ensure_backup_group(job.backup_group, job.backup_users, dry_run)
            # Always apply folder protection when configured (even if backup is skipped)
            if (not dry_run) and job.protect_owner and job.protect_group and job.protect_perms:
                apply_folder_protection(
                    dest_folder,
                    job.protect_owner,
                    job.protect_group,
                    job.protect_perms,
                    dry_run=dry_run,
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
            now = datetime.now()
            manual_trigger = not is_shutdown_trigger
            archive_prefix_full = f"{job.key}_{job.archive_prefix}_{BACKUP_WORD}"
            latest = get_latest_archive_mtime(dest_folder, archive_prefix_full, job.fmt)
            if not should_run_backup(
                frequency=job.backup_frequency,
                latest_backup_time=latest,
                now=now,
                manual_trigger=manual_trigger,
            ):
                log_message(
                    f"[SKIP] Frequency '{job.backup_frequency}' not due. "
                    f"Latest backup: {latest.strftime('%Y-%m-%d %H:%M:%S') if latest else 'None'}"
                )
                continue
            was_active = False
            # Optional stop service (only if it was running)
            if job.stop_service:
                was_active = systemctl_is_active(job.stop_service)
                if was_active:
                    systemctl_stop(job.stop_service, job.timeout_stop_sec, dry_run)
                else:
                    log_message(f"Service already inactive: {job.stop_service}")
            # Validate source exists
            if not job.source_dir.exists():
                log_message(f"[ERROR] Source does not exist: {job.source_dir}")
                continue
            # Create archive (atomic .tmp then rename)
            ts = now.strftime(TS_FMT)
            archive_name = f"{job.key}_{job.archive_prefix}_{BACKUP_WORD}_{ts}.{job.fmt}"
            archive_path = dest_folder / archive_name
            if job.fmt == "zip":
                ok = zip_directory(job.source_dir, archive_path, dry_run)
            else:
                ok = tar_directory(job.source_dir, archive_path, dry_run)
            if ok:
                log_message(f"[OK] Created archive: {archive_path}")

                # Retention: keep newest N archives for this job/prefix/format
                enforce_retention(
                    dest_folder,
                    job.keep,
                    f"{job.key}_{job.archive_prefix}_{BACKUP_WORD}",
                    job.fmt,
                    dry_run,
                )
                processed += 1
            else:
                log_message("[FAIL] Archive creation failed.")
            # Restart service only if:
            # - job requested stop/start AND it was active when we started
            # - NOT during shutdown
            if job.stop_service and was_active:
                if is_shutdown_trigger:
                    log_message(f"Shutdown in progress; not restarting service: {job.stop_service}")
                else:
                    systemctl_start(job.stop_service, dry_run)
        except KeyboardInterrupt:
            log_message("Interrupted by user.")
            return 130
        except Exception as e:
            log_message(f"[ERROR] Job '{job.key}' failed: {e!r}")
    # Final summary
    log_message(f"DONE: {processed}/{len(jobs)} job(s) processed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
