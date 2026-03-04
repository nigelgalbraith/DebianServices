#!/usr/bin/env python3
# Refresh Plex libraries (entire sections or specific folder categories/paths)
# - Live mode when script file is named exactly 'plex_refresh.py' (uses /etc config)
# - Dry-run with template config next to the script for any other filename

import os
import sys
import json
import time
import subprocess
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime

CONFIG_FILE = "/etc/plex_refresh_config.json"
CONFIG_TEST = "plex_refresh_config-template.json"
SCRIPT_NAME = "plex_refresh.py"

PLEX_TOKEN_LOC = "/var/lib/plexmediaserver/Library/Application Support/Plex Media Server/Preferences.xml"

# ======================
# Logging
# ======================
def log_message(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} : {message}")

# ======================
# Config + mode
# ======================
def get_config_and_mode() -> tuple[str, bool]:
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
    return config_path, dry_run

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ======================
# HTTP helpers (no extra deps)
# ======================
def http_get(url: str, timeout: float = 10.0) -> tuple[int, bytes]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.getcode(), resp.read()

def build_url(base: str, path: str, token: str, params: dict | None = None) -> str:
    q = dict(params or {})
    if token:
        q["X-Plex-Token"] = token
    query = urllib.parse.urlencode(q, safe="/:")
    return f"{base.rstrip('/')}/{path.lstrip('/')}?{query}"

def get_plex_token(token_loc: str):
    """Return Plex token from Preferences.xml using grep."""
    cmd = f"grep -oP 'PlexOnlineToken=\"\\K[^\"]+' \"{token_loc}\""
    try:
        result = subprocess.check_output(cmd, shell=True, text=True).strip()
        return result
    except subprocess.CalledProcessError:
        return ""

# ======================
# Plex API bits
# ======================
def fetch_sections(base_url: str, token: str, dry_run: bool) -> dict[str, dict]:
    """
    Returns a dict keyed by section key (string), with {'key','title','type'}.
    """
    url = build_url(base_url, "/library/sections", token, {})
    if dry_run:
        log_message(f"DRY-RUN: GET {url}")
        return {}
    code, body = http_get(url)
    if code != 200:
        log_message(f"ERROR: Failed to fetch sections (HTTP {code}).")
        return {}
    # Plex returns XML
    sections = {}
    try:
        root = ET.fromstring(body)
        for directory in root.findall(".//Directory"):
            key = directory.attrib.get("key")
            title = directory.attrib.get("title", "")
            stype = directory.attrib.get("type", "")
            if key:
                sections[key] = {"key": key, "title": title, "type": stype}
    except Exception as e:
        log_message(f"ERROR: parsing sections XML: {e}")
    return sections

def resolve_section_key(sections_by_key: dict[str, dict], title: str | None) -> str | None:
    if not title:
        return None
    for k, info in sections_by_key.items():
        if info.get("title") == title:
            return k
    return None

def refresh_section(base_url: str, token: str, section_key: str, force: bool, dry_run: bool) -> bool:
    params = {"force": 1 if force else 0}
    url = build_url(base_url, f"/library/sections/{section_key}/refresh", token, params)
    if dry_run:
        log_message(f"DRY-RUN: GET {url}")
        return True
    try:
        code, _ = http_get(url)
        if code == 200:
            log_message(f"Triggered refresh for section {section_key}.")
            return True
        log_message(f"ERROR: Section refresh {section_key} returned HTTP {code}.")
    except Exception as e:
        log_message(f"ERROR: Section refresh {section_key} failed: {e}")
    return False

def refresh_path(base_url: str, token: str, section_key: str, path: str, force: bool, dry_run: bool) -> bool:
    # The path-targeted refresh is a supported query variant:
    # /library/sections/<key>/refresh?path=<absolute_path>&force=1
    params = {"force": 1 if force else 0, "path": path}
    url = build_url(base_url, f"/library/sections/{section_key}/refresh", token, params)
    if dry_run:
        log_message(f"DRY-RUN: GET {url}")
        return True
    try:
        code, _ = http_get(url)
        if code == 200:
            log_message(f"Triggered refresh for section {section_key}, path: {path}")
            return True
        log_message(f"ERROR: Path refresh {path} (section {section_key}) HTTP {code}.")
    except Exception as e:
        log_message(f"ERROR: Path refresh {path} (section {section_key}) failed: {e}")
    return False

# ======================
# Summary
# ======================
def print_summary(s: dict) -> None:
    print("\n===== Plex Refresh Summary =====")
    print(f"Server: {s['server']['base_url']}")
    print(f"Mode:   {'DRY-RUN' if s['dry_run'] else 'LIVE'}")
    print(f"Items processed: {s['counts']['total']} (sections: {s['counts']['sections']}, paths: {s['counts']['paths']})")
    print(f"Success: {s['counts']['ok']}  |  Failed: {s['counts']['failed']}")
    if s["errors"]:
        print("\nErrors:")
        for e in s["errors"]:
            print(f" - {e}")
    print("================================\n")

# ======================
# Main
# ======================
def main() -> int:
    config_path, dry_run = get_config_and_mode()

    # Load config
    try:
        cfg = load_config(config_path)
    except FileNotFoundError:
        log_message(f"ERROR: config file '{config_path}' not found.")
        return 1
    except json.JSONDecodeError as e:
        log_message(f"ERROR: bad JSON in '{config_path}': {e}")
        return 1

    # Config fields
    server = cfg.get("PLEX_SERVER", {})
    base_url = str(server.get("base_url", "http://127.0.0.1:32400")).strip()
    token = get_plex_token(PLEX_TOKEN_LOC)
    if not token:
        log_message(f"ERROR: Could not read Plex token from '{PLEX_TOKEN_LOC}'.")
        return 1
    force = bool(cfg.get("force", True))
    sleep_secs = float(cfg.get("sleep_between_calls", 0.4))
    sections_cfg = cfg.get("sections", [])

    log_message("=== Plex refresh started ===")
    log_message(f"Server: {base_url}  |  force={force}  |  calls delay={sleep_secs}s")

    # Summary accumulators
    summary = {
        "server": {"base_url": base_url},
        "dry_run": dry_run,
        "counts": {"total": 0, "sections": 0, "paths": 0, "ok": 0, "failed": 0},
        "errors": []
    }

    # Prefetch sections to resolve keys by title (live mode only)
    known_sections = fetch_sections(base_url, token, dry_run)
    # Map for quick reverse lookup (title->key) when available
    # (In dry-run we won't have this; keys must be supplied or we'll skip.)
    title_to_key = {v.get("title"): k for k, v in known_sections.items()}

    # Process config
    for item in sections_cfg:
        # item supports: {"key": "1"} OR {"title": "Movies"}
        skey = str(item.get("key")) if item.get("key") is not None else None
        title = item.get("title")

        # Resolve section key if only title provided
        if not skey and title:
            skey = title_to_key.get(title)
            if not skey and dry_run:
                skey = f"(resolve-by-title:{title})"
            elif not skey:
                summary["errors"].append(f"Could not resolve section key for title '{title}'.")
                log_message(f"WARNING: Unknown section title '{title}', skipping.")
                continue

        # Full-section refresh only
        summary["counts"]["total"] += 1
        summary["counts"]["sections"] += 1
        ok = refresh_section(base_url, token, skey, force, dry_run)
        if ok:
            summary["counts"]["ok"] += 1
        else:
            summary["counts"]["failed"] += 1
        if sleep_secs > 0:
            time.sleep(sleep_secs)


    log_message("=== Plex refresh completed ===")
    print_summary(summary)
    return 0 if summary["counts"]["failed"] == 0 else 2

if __name__ == "__main__":
    sys.exit(main())
