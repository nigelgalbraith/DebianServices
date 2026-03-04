# DebianServices

A collection of **systemd services and helper scripts** for automating common tasks on Debian-based systems.

These services are designed to run during **startup or shutdown** and perform tasks such as:

* Mounting storage drives
* Organizing media libraries
* Updating file permissions
* Refreshing Plex metadata
* Creating configuration backups
* Running rsync-based backups

The repository provides **generic templates**.
You must adjust paths and users to match your system before use.

---

# Repository Structure

```
DebianServices/
├── StartupServices/
│   ├── mountDrives/
│   ├── plexRename/
│   ├── plexPermissions/
│   └── plexRefresh/
│
└── ShutdownServices/
    ├── configBackup/
    └── rsyncBackup/
```

Each service folder typically contains:

```
service-name/
├── service-name-template.service
├── service-name-template.py
├── service-name_config-template.json
└── service-name.logrotate
```

---

# Installation (example)

Copy the service file:

```bash
sudo cp service-name-template.service /etc/systemd/system/service-name.service
```

Copy the script:

```bash
sudo cp service-name-template.py /usr/local/bin/service-name.py
```

Copy and edit the configuration:

```bash
sudo cp service-name_config-template.json /etc/service-name_config.json
```

Reload systemd:

```bash
sudo systemctl daemon-reload
```

Enable the service:

```bash
sudo systemctl enable service-name.service
```

---

# Notes

These templates assume:

* Python 3 is installed
* scripts are placed in `/usr/local/bin`
* configuration files are stored in `/etc`

You may change these paths if desired.

---

# Disclaimer

These services modify files, permissions, and mount points on your system.
Review and adjust all configuration files before enabling them.

Use at your own risk.
