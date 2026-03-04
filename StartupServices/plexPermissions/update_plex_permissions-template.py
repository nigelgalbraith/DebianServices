#!/usr/bin/env python3
# Debian update PLEX permissions, manage Plex service, and refresh Movies/TV libraries

import os
import sys
import json
import subprocess
from datetime import datetime

CONFIG_FILE = "/etc/update_plex_permissions_config.json"
CONFIG_TEST = "update_plex_permissions_config-template.json"
SCRIPT_NAME = "update_plex_permissions.py"

# ======================
# Logging
# ======================
def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} : {message}")

# ======================
# Command wrapper (respects dry-run)
# ======================
def exec_cmd(args, dry_run=False):
    if dry_run:
        log_message("DRY-RUN: " + " ".join(args))
        return 0  # pretend success
    result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode

# ======================
# Group membership
# ======================
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

# ======================
# Ownership & perms
# ======================
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

# ======================
# Service helpers
# ======================
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

# ======================
# Pretty summary
# ======================
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

# ======================
# Main
# ======================
def main():
    # Decide config + dry-run based on script filename
    script_basename = os.path.basename(sys.argv[0])
    if script_basename == SCRIPT_NAME:
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
