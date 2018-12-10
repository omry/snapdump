#!/usr/bin/env python3

SERVER = "ZFS SERVER"
# Recommended to use root user and restrict, see restricted_shell/README.md
SSH_USER = "root"
# Directory to put djmp snapshots in
BACKUP_ROOT = "/mnt/something-big/"
# SSH identity file, if we are using a restricted shell it's safe to leave it here
IDENTITY_FILE = "/tmp_backup/.key/backup_id_rsa"

# Names of datasets to dump
DATASETS = [
  "storage/home"
  "storage/vms"
]

# Number of days before starting a new backup
FULL_BACKUP_EVERY_DAYS = 30

# Number of days before starting another incremental backup
INCREMENTAL_BACKUP_EVERY_DAYS = 1

# Number of days to retain snapshots.
# This effects both zfs snapshots and zfs dumps in BACKUP_ROOT
BACKUP_RETENTION_DAYS = 90

TIME_FORMAT = "%Y_%m_%d__%H_%M_%S"
# Dump split size, some distributed file systems (like gluster) can't support arbitrarily large files.
SPLIT_SIZE = "200GB"

VERBOSE = False
SNAPSHOT_SUFFIX = "snapshot-part-"
TEMPDIR_SUFFIX = "dump-in-progress"

# Number of seconds without write activity to consider a
# dump which is in progress to be dead.
DUMP_DEAD_SECONDS = 60
