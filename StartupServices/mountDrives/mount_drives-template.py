#!/usr/bin/env python3
"""
mount_drives.py

Mount filesystem devices by label at system startup using a JSON configuration
file. The script ensures drives are mounted at the correct mount points and
can optionally enforce permissions on protected root folders and create
Freedesktop-compliant per-user Trash directories on mounted filesystems.

This script is designed to be safe for both automated service execution and
manual testing. It supports a dry-run mode that logs actions without making
any system changes.

Features
--------
• Mount drives using stable device labels (/dev/disk/by-label)
• Automatically create mount points if missing
• Protect root-level directories by enforcing ownership and permissions
• Create per-user trash directories (.Trash-UID) on mounted filesystems
• Dry-run mode for safe testing
• JSON-based configuration for easy management
• Designed for systemd service execution at boot

Execution Safety
----------------
The script includes two safety mechanisms to prevent accidental execution:

1. Script Name Check
   Only the correctly named production script (mount_drives.py) performs
   real operations. If the script name differs, the script automatically
   runs in dry-run mode using the test configuration.

2. --dry-run Argument
   Passing the --dry-run flag forces dry-run mode regardless of script name.

Dry-run mode logs all actions without executing any system commands.

Usage
-----

Normal execution (production):
    python3 mount_drives.py

Explicit dry-run test:
    python3 mount_drives.py --dry-run

Template/testing script:
    python3 mount_drives-template.py

Configuration
-------------

Production configuration:
    /etc/mount_drives_config.json

Dry-run / test configuration:
    mount_drives_config-template.json

Example configuration structure:

{
    "MOUNT_OPTS": "defaults",
    "DRIVES": {
        "DATA": "/mnt/data",
        "BACKUP": "/mnt/backup"
    },
    "PROTECTED_ROOT_FOLDERS": [
        {
            "path": "/mnt/data/shared",
            "owner": "root",
            "group": "root",
            "permissions": "755"
        }
    ],
    "TRASH_USERS": [
        "user1",
        "user2"
    ]
}

Operation Overview
------------------

1. Load configuration file
2. Verify device labels exist
3. Check if mount points are already mounted
4. Mount drives if needed
5. Apply ownership and permissions to protected root folders
6. Create per-user trash directories on mounted filesystems
7. Log all actions

Logging
-------

All actions are logged to stdout with timestamps. When run under a systemd
service, these logs will appear in the system journal.
"""

import os
import sys
import json
import subprocess
import pwd
import argparse
from datetime import datetime

CONFIG_FILE = "/etc/mount_drives_config.json"
CONFIG_TEST = "mount_drives_config-template.json"
SCRIPT_NAME = "mount_drives.py"
ARG_DESCRIPTION = "Mount filesystem devices by label at system startup, with optional dry-run mode."

# =====================
# LOGGING
# =====================
def log_message(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} : {message}")


# =====================
# COMMAND WRAPPER (RESPECTS DRY-RUN)
# =====================
def exec_cmd(args, dry_run: bool = False) -> int:
    if dry_run:
        log_message("DRY-RUN: " + " ".join(args))
        return 0
    return subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode

# =====================
# HELPERS
# =====================
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


def check_mount(label: str, mount_point: str) -> int:
    """
    Check whether a labeled device is mounted.
    Returns:
        0 -> already mounted
        1 -> device not found
        2 -> not mounted yet
    """
    dev = f"/dev/disk/by-label/{label}"
    if not os.path.exists(dev):
        log_message(f"ERROR: device with label '{label}' not found.")
        return 1
    rc = subprocess.run(
        ["mountpoint", "-q", mount_point],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    ).returncode
    if rc == 0:
        log_message(f"Already mounted: {mount_point} (label {label}).")
        return 0
    return 2


def mount_drive(label: str, mount_point: str, opts: str, dry_run: bool = False) -> bool:
    dev = f"/dev/disk/by-label/{label}"
    if not os.path.isdir(mount_point):
        if dry_run:
            log_message(f"DRY-RUN: mkdir -p {mount_point}")
        else:
            os.makedirs(mount_point, exist_ok=True)
    log_message(f"Mounting {dev} ({label}) to {mount_point} ...")
    rc = exec_cmd(["mount", "-o", opts, dev, mount_point], dry_run=dry_run)
    if rc == 0:
        log_message(f"SUCCESS: Mounted {label} at {mount_point}.")
        return True
    else:
        log_message(f"FAIL: Could not mount {label} at {mount_point}.")
        return False


def protect_root_folder(path: str, owner: str, group: str, mode: str, dry_run: bool = False) -> bool:
    """Protect a top-level folder by enforcing ownership + permissions."""
    if not os.path.isdir(path):
        log_message(f"Protected root folder not found, skipping: {path}")
        return False
    rc1 = exec_cmd(["chown", f"{owner}:{group}", path], dry_run=dry_run)
    rc2 = exec_cmd(["chmod", mode, path], dry_run=dry_run)
    if rc1 == 0 and rc2 == 0:
        log_message(f"Protected root folder set: {path} ({owner}:{group}, {mode})")
        return True
    log_message(f"Failed to protect root folder: {path}")
    return False


def ensure_trash_for_user(mount_point: str, username: str, dry_run: bool = False) -> bool:
    """
    Create a Freedesktop trash directory on a mounted filesystem:
      <mount>/.Trash-<uid>/{files,info}
    Owned by the user with 700 perms.
    """
    try:
        pw = pwd.getpwnam(username)
    except KeyError:
        log_message(f"[TRASH] User not found, skipping: {username}")
        return False
    uid = pw.pw_uid
    gid = pw.pw_gid
    trash_root = os.path.join(mount_point, f".Trash-{uid}")
    trash_files = os.path.join(trash_root, "files")
    trash_info = os.path.join(trash_root, "info")
    if dry_run:
        log_message(f"[TRASH][DRY-RUN] Would mkdir -p '{trash_files}' '{trash_info}'")
        log_message(f"[TRASH][DRY-RUN] Would chown -R {username}:{username} '{trash_root}'")
        log_message(f"[TRASH][DRY-RUN] Would chmod 700 '{trash_root}'")
        return True
    try:
        os.makedirs(trash_files, exist_ok=True)
        os.makedirs(trash_info, exist_ok=True)
        # Own the trash directory tree
        for root, dirs, files in os.walk(trash_root):
            os.chown(root, uid, gid)
            for d in dirs:
                os.chown(os.path.join(root, d), uid, gid)
            for f in files:
                os.chown(os.path.join(root, f), uid, gid)
        os.chmod(trash_root, 0o700)
        log_message(f"[TRASH] Ready: {trash_root} (owner {username}, mode 700)")
        return True
    except Exception as e:
        log_message(f"[TRASH] Failed for {username} on {mount_point}: {e}")
        return False


# =====================
# MAIN
# =====================
def main():
    # Decide config + dry-run based on script filename or arguments
    args = parse_args(ARG_DESCRIPTION)
    script_basename = os.path.basename(sys.argv[0])
    if args.dry_run:
        cfg_dir = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(cfg_dir, CONFIG_TEST)
        dry_run = True
        log_message(f"Argument '--dry-run' detected — running in DRY-RUN with test config '{config_path}'.")
    elif script_basename == SCRIPT_NAME:
        config_path = CONFIG_FILE
        dry_run = False
    else:
        cfg_dir = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(cfg_dir, CONFIG_TEST)
        dry_run = True
        log_message(
            f"Script name '{script_basename}' != '{SCRIPT_NAME}' — "
            f"running in DRY-RUN with test config '{config_path}'."
        )
    log_message(f"Using config: {config_path}")
    log_message(f"Dry run: {dry_run}")
    # Load configuration
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        log_message(f"ERROR: config file '{config_path}' not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        log_message(f"ERROR: bad JSON in '{config_path}': {e}")
        sys.exit(1)
    mount_opts = cfg.get("MOUNT_OPTS", "defaults")
    drives = cfg.get("DRIVES", {})
    protected_roots = cfg.get("PROTECTED_ROOT_FOLDERS", [])
    trash_users = cfg.get("TRASH_USERS", [])
    log_message("=== Mount process started ===")
    # Mount drives
    for label, mount_point in drives.items():
        status = check_mount(label, mount_point)
        if status == 0:
            continue      # already mounted
        elif status == 1:
            continue      # device not found
        elif status == 2:
            mount_drive(label, mount_point, mount_opts, dry_run=dry_run)
    # Protect root folders
    for entry in protected_roots:
        protect_root_folder(
            path=entry["path"],
            owner=entry.get("owner", "root"),
            group=entry.get("group", "root"),
            mode=entry.get("permissions", "755"),
            dry_run=dry_run
        )
    # Create per-user Trash folders on each mount point
    if trash_users:
        for mount_point in drives.values():
            if not os.path.isdir(mount_point):
                continue
            for user in trash_users:
                ensure_trash_for_user(mount_point, user, dry_run=dry_run)
    log_message("=== Mount process completed ===")

if __name__ == "__main__":
    main()
