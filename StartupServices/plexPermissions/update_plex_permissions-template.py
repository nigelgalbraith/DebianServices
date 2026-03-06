#!/usr/bin/env python3
"""
update_plex_permissions.py

Configure and maintain filesystem permissions required by Plex Media Server.
The script ensures that Plex and specified users have the correct ownership,
group membership, and permissions for media folders. It also manages the Plex
service by enabling it at boot and restarting it after changes are applied.

The script is configuration-driven and reads settings from a JSON file. It can
operate in either production mode or dry-run mode for safe testing.

Features
--------
• Add users to the Plex media group
• Enforce ownership and permissions on media folders
• Protect root-level media folders from accidental deletion
• Enable the Plex service on system boot
• Restart the Plex service after permission updates
• Provide a detailed summary of all actions performed
• Support dry-run testing mode
• JSON-based configuration

Execution Safety
----------------
The script includes two safety mechanisms to prevent accidental system changes:

1. Script Name Check
   Only the correctly named production script (update_plex_permissions.py)
   performs real operations. If the script name differs, the script will run
   in dry-run mode using the template configuration.

2. --dry-run Argument
   Passing the --dry-run flag forces dry-run mode regardless of script name.

Dry-run mode logs all actions without modifying the system.

Usage
-----

Production execution:
    python3 update_plex_permissions.py

Explicit dry-run test:
    python3 update_plex_permissions.py --dry-run

Template/test script:
    python3 update_plex_permissions-template.py

Configuration
-------------

Production configuration file:
    /etc/update_plex_permissions_config.json

Dry-run/test configuration file:
    update_plex_permissions_config-template.json

Example configuration structure:

{
    "PLEX_SERVICE": "plexmediaserver",
    "GLOBAL_GROUP": "media",
    "GLOBAL_USERS": [
        "plex",
        "user1",
        "user2"
    ],
    "MEDIA_FOLDERS": [
        {
            "path": "/mnt/media/movies",
            "owner": "plex",
            "group": "media",
            "permissions": "775"
        }
    ],
    "PROTECTED_ROOT_FOLDERS": [
        {
            "path": "/mnt/media",
            "owner": "root",
            "group": "media",
            "permissions": "2775"
        }
    ]
}

Operation Overview
------------------

1. Load configuration from JSON
2. Ensure required users belong to the Plex media group
3. Apply ownership and permissions to media folders
4. Protect specified root-level media directories
5. Enable Plex service on boot
6. Restart Plex service
7. Print a summary of all operations performed

Logging
-------

All actions are logged with timestamps to stdout. When run via systemd or
automation scripts, these messages will appear in the system journal.
"""

import os
import sys
import json
import subprocess
import argparse
from datetime import datetime

CONFIG_FILE = "/etc/update_plex_permissions_config.json"
CONFIG_TEST = "update_plex_permissions_config-template.json"
SCRIPT_NAME = "update_plex_permissions.py"
ARG_DESCRIPTION = "Update Plex permissions and manage the Plex service."

# =====================
# LOGGING
# =====================
def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} : {message}")


# =====================
# COMMAND WRAPPER (RESPECTS DRY-RUN)
# =====================
def exec_cmd(args, dry_run=False):
    if dry_run:
        log_message("DRY-RUN: " + " ".join(args))
        return 0  # pretend success
    result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode


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
# GROUP MEMBERSHIP
# =====================
def add_user_to_group(group, user, dry_run=False):
    exists = exec_cmd(["id", "-u", user], dry_run=False) == 0
    if not exists:
        log_message(f"User '{user}' does not exist; skipping add_user_to_group.")
        return False
    rc = exec_cmd(["usermod", "-aG", group, user], dry_run=dry_run)
    if rc == 0:
        log_message(f"Added {user} to the {group} group.")
        return True
    else:
        log_message(f"Failed to add {user} to the {group} group.")
        return False


# =====================
# OWNERSHIP & PERMS
# =====================
def protect_root_folder(path, owner, group, mode, dry_run=False):
    """
    Protect a top-level media folder from deletion while allowing group writes.
    """
    if not os.path.isdir(path):
        log_message(f"Protected root folder not found, skipping: {path}")
        return False
    rc1 = exec_cmd(["chown", f"{owner}:{group}", path], dry_run=dry_run)
    rc2 = exec_cmd(["chmod", mode, path], dry_run=dry_run)
    if rc1 == 0 and rc2 == 0:
        log_message(
            f"Protected root folder set: {path} "
            f"({owner}:{group}, {mode})"
        )
        return True
    log_message(f"Failed to protect root folder: {path}")
    return False


def set_ownership(folder, user, group, dry_run=False):
    """
    Returns (attempted_items, failed_items)
    """
    attempted = failed = 0
    try:
        for root, dirs, files in os.walk(folder):
            for name in dirs + files:
                path = os.path.join(root, name)
                attempted += 1
                rc = exec_cmd(["chown", f"{user}:{group}", path], dry_run=dry_run)
                if rc != 0 and not dry_run:
                    failed += 1
                    log_message(f"Skip (ownership failed): {path}")
        log_message(f"Finished setting ownership of {folder} (skipped errors).")
    except Exception as e:
        log_message(f"Error setting ownership for {folder}: {e}")
    return attempted, failed


def set_permissions(folder, mode, dry_run=False):
    """
    Returns (attempted_items, failed_items)
    """
    attempted = failed = 0
    try:
        for root, dirs, files in os.walk(folder):
            for name in dirs + files:
                path = os.path.join(root, name)
                attempted += 1
                rc = exec_cmd(["chmod", mode, path], dry_run=dry_run)
                if rc != 0 and not dry_run:
                    failed += 1
                    log_message(f"Skip (chmod failed): {path}")
        log_message(f"Finished setting permissions of {folder} to {mode} (skipped errors).")
    except Exception as e:
        log_message(f"Error setting permissions for {folder}: {e}")
    return attempted, failed


# =====================
# SERVICE HELPERS
# =====================
def enable_service(service, dry_run=False):
    log_message(f"Enabling {service} on boot...")
    rc = exec_cmd(["systemctl", "enable", service], dry_run=dry_run)
    if rc == 0:
        log_message(f"{service} enabled to start on boot.")
        return True
    else:
        log_message(f"Failed to enable {service} to start on boot.")
        return False


def restart_service(service, dry_run=False):
    log_message(f"Restarting {service}...")
    rc = exec_cmd(["systemctl", "restart", service], dry_run=dry_run)
    if rc == 0:
        log_message(f"{service} restarted successfully.")
        return True
    else:
        log_message(f"Failed to restart {service}.")
        return False


# =====================
# PRETTY SUMMARY
# =====================
def print_summary(summary):
    print("\n===== Summary =====")    
    # Users
    u = summary["users"]
    print(f"Users added to group '{summary['group']}': {u['added']}/{u['attempted']} "
          f"(failed: {u['failed']})")

    # Protected roots
    p = summary["protected"]
    print(
        f"Protected root folders: {p['ok']}/{p['attempted']} "
        f"(failed: {p['failed']})"
    )
    # Folders
    totals = summary["totals"]
    print(f"Folders processed: {len(summary['folders'])} "
          f"(missing/skipped: {len(summary['missing'])})")
    print(f"Ownership: attempted {totals['own_attempted']}, failed {totals['own_failed']}")
    print(f"Permissions: attempted {totals['perm_attempted']}, failed {totals['perm_failed']}")
    # Per-folder line
    if summary["folders"]:
        print("\nPer-folder results:")
        for f in summary["folders"]:
            print(f" - {f['path']}: own {f['own_attempted']}/{f['own_failed']} failed, "
                  f"perm {f['perm_attempted']}/{f['perm_failed']} failed")
    # Missing
    if summary["missing"]:
        print("\nMissing folders:")
        for m in summary["missing"]:
            print(f" - {m}")
    # Service
    s = summary["service"]
    print("\nService actions:")
    print(f" - enable:  {'OK' if s['enabled_ok'] else 'FAILED'}")
    print(f" - restart: {'OK' if s['restarted_ok'] else 'FAILED'}")
    print("===================\n")


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
    plex_service = cfg["PLEX_SERVICE"]
    global_users = cfg["GLOBAL_USERS"]
    global_group = cfg["GLOBAL_GROUP"]
    media_folders = cfg["MEDIA_FOLDERS"]
    protected_roots = cfg.get("PROTECTED_ROOT_FOLDERS", [])
    log_message("Starting Plex permissions setup...")
    # Summary accumulator
    summary = {
        "group": global_group,
        "users": {"attempted": 0, "added": 0, "failed": 0},
        "folders": [],
        "missing": [],
        "protected": {"attempted": 0, "ok": 0, "failed": 0},
        "totals": {"own_attempted": 0, "own_failed": 0, "perm_attempted": 0, "perm_failed": 0},
        "service": {"enabled_ok": False, "restarted_ok": False},
    }
    # Add users to group
    for user in global_users:
        summary["users"]["attempted"] += 1
        ok = add_user_to_group(global_group, user, dry_run=dry_run)
        if ok:
            summary["users"]["added"] += 1
        else:
            summary["users"]["failed"] += 1
    # Protect root folders
    for entry in protected_roots:
        summary["protected"]["attempted"] += 1

        ok = protect_root_folder(
            path=entry["path"],
            owner=entry.get("owner", "root"),
            group=entry.get("group", global_group),
            mode=entry.get("permissions", "2775"),
            dry_run=dry_run
        )
        if ok:
            summary["protected"]["ok"] += 1
        else:
            summary["protected"]["failed"] += 1
    # Apply per-folder permissions
    for entry in media_folders:
        path = entry["path"]
        owner = entry.get("owner", "plex")
        group = entry.get("group", global_group)
        mode = entry.get("permissions", "775")
        if os.path.isdir(path):
            log_message(f"Processing {path} ...")
            own_attempted, own_failed = set_ownership(path, owner, group, dry_run=dry_run)
            perm_attempted, perm_failed = set_permissions(path, mode, dry_run=dry_run)
            summary["folders"].append({
                "path": path,
                "own_attempted": own_attempted,
                "own_failed": own_failed,
                "perm_attempted": perm_attempted,
                "perm_failed": perm_failed,
            })
            summary["totals"]["own_attempted"] += own_attempted
            summary["totals"]["own_failed"] += own_failed
            summary["totals"]["perm_attempted"] += perm_attempted
            summary["totals"]["perm_failed"] += perm_failed
        else:
            log_message(f"Warning: {path} not found, skipping.")
            summary["missing"].append(path)
    # Service actions
    summary["service"]["enabled_ok"] = enable_service(plex_service, dry_run=dry_run)
    summary["service"]["restarted_ok"] = restart_service(plex_service, dry_run=dry_run)
    log_message("Plex permissions setup completed.")
    print_summary(summary)


if __name__ == "__main__":
    main()
