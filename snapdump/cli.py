#!/usr/bin/env python3

import argparse
import os
import shutil
from subprocess import Popen, PIPE, check_output
import time
from datetime import datetime
import glob
from omegaconf import OmegaConf

VERBOSE = False
TIME_FORMAT = "%Y_%m_%d__%H_%M_%S"
SNAPSHOT_SUFFIX = "snapshot-part-"
TEMPDIR_SUFFIX = "dump-in-progress"


def get_ssh_cmd_arr(conf):
    cmd = ["ssh", f"{conf.server.ssh_user}@{conf.server.hostname}"]
    if conf.server.identity_file is not None:
        cmd += ["-i", f"{conf.server.identity_file}"]
    return cmd


def ssh_cmd(conf, command):
    cmd = get_ssh_cmd_arr(conf) + command
    if VERBOSE:
        print('EXECUTING "{0}"'.format(" ".join(cmd)))
    return check_output(cmd)


def normalize_dataset_name(dataset):
    return dataset.replace("/", "_")


def parse_timestamp(dirname):
    try:
        d = datetime.strptime(dirname + " +0000", TIME_FORMAT + " %z")
        return d.timestamp()
    except ValueError as err:
        if VERBOSE:
            print("Error parsing timestamp in {0} : {0}".format(dirname, err))
        return 0


def get_backup_directory(conf, dataset, now):
    nowtime = datetime.utcfromtimestamp(now).strftime(TIME_FORMAT)
    dataset_dir = f"{conf.backup.directory}/{normalize_dataset_name(dataset)}"
    newest_dir = get_newest_file(dataset_dir)
    full_dir = "%s/%s" % (dataset_dir, nowtime)
    if newest_dir is None:
        os.makedirs(full_dir)
        if VERBOSE:
            print("Directory does not exist, creating a new directory : %s" % full_dir)
    else:
        ctime = parse_timestamp(newest_dir)
        delta_days = (now - ctime) / (60.0 * 60 * 24)
        if delta_days >= conf.backup.interval_days.full:
            os.makedirs(full_dir)
            if VERBOSE:
                print(
                    "Old directory older than %.2f days, creating a new one directory : %s"
                    % (conf.backup.interval_days.full, full_dir)
                )
        else:
            full_dir = "%s/%s" % (dataset_dir, newest_dir)
            if VERBOSE:
                print(
                    "Reusing existing directory %s (%.1f/%.1f days)"
                    % (full_dir, delta_days, conf.backup.interval_days.full)
                )
    return full_dir


def get_newest_file(path):
    if not os.path.exists(path):
        return None
    files = sorted(os.listdir(path), key=parse_timestamp)
    if len(files) > 0:
        return files[-1]
    return None


def chain(p1, p2_command):
    return Popen(p2_command, stdin=p1.stdout, stdout=PIPE)


def zfs_snapshot(conf, dataset, snapshot_name):
    ssh_cmd(conf, ["zfs", "snapshot", f"{dataset}@{snapshot_name}"])

def delete_temporary_dump_dirs(backup_dir):
    for tempdir in glob.glob(f"{backup_dir}/*.{TEMPDIR_SUFFIX}"):
        print(f"Deleting dead temporary dump dir : {tempdir}")
        shutil.rmtree(tempdir)

def is_dump_in_progress(conf, backup_dir):
    for tempdir in glob.glob(f"{backup_dir}/*.{TEMPDIR_SUFFIX}"):
        timestamps = [os.path.getmtime(f"{tempdir}/{x}") for x in os.listdir(tempdir)]
        if len(timestamps) == 0:
            return False
        delta = time.time() - max(timestamps)
        if delta < conf.backup.dump_dead_seconds:
            return True
    return False

def zfs_dump_snapshot(conf, backup_dir, dataset, snapshot_name, base_snapshot_name=None):
    zfs_cmd = ["zfs", "send", f"{dataset}@{snapshot_name}"]
    backup_type = "full"
    if base_snapshot_name is None:
        print("Creating full snapshot dump for {0}@{1}".format(dataset, snapshot_name))
    else:
        print(
            "Creating incremental snapshot dump for {0}@{1} based on {2}".format(
                dataset, snapshot_name, base_snapshot_name
            )
        )
        zfs_cmd += ["-i", base_snapshot_name]
        backup_type = "incr"

    if is_dump_in_progress(conf, backup_dir):
        print(f"Dump is already in progress in {backup_dir}, bailing up")
        return

    # delete dead dump directories
    delete_temporary_dump_dirs(backup_dir)

    parts_dir = f"{backup_dir}/{backup_type}##{snapshot_name}"
    temporary_dir = f"{parts_dir}.{TEMPDIR_SUFFIX}"
    os.makedirs(temporary_dir)

    ssh_process = Popen(get_ssh_cmd_arr(conf) + zfs_cmd, stdout=PIPE)
    gzip_process = chain(ssh_process, "gzip")
    split_process = chain(
        gzip_process,
        ["split", "-b", conf.backup.split_size, "-a3", "-", f"{temporary_dir}/{SNAPSHOT_SUFFIX}"],
    )

    ssh_process.stdout.close()
    gzip_process.stdout.close()
    split_process.communicate()
    ssh_process.wait()
    gzip_process.wait()
    cleanup_dataset_snapshots(conf, dataset)

    os.rename(temporary_dir, parts_dir)


def get_lines(s):
    return list(filter(len, s.decode().split("\n")))


def zfs_get_dataset_snapshots(conf, dataset):
    ret = ssh_cmd(conf,
        [
            "zfs",
            "list",
            "-H",
            "-t",
            "snapshot",
            "-o",
            "name",
            "-s",
            "creation",
            "-r",
            dataset,
        ]
    )
    return get_lines(ret)


def zfs_get_latest_snapshot(dataset):
    snapshots = zfs_get_dataset_snapshots(dataset)
    if len(snapshots) == 0:
        return None
    return snapshots[-1]


# returns sorted snapshot names in the directory
def get_snapshot_names(backup_dir):
    files = os.listdir(backup_dir)
    files = list(filter(lambda x: len(x.split("##")) == 2 and not x.endswith(TEMPDIR_SUFFIX) , files))
    snap_names = [v.split("##")[1] for v in files]
    return sorted(snap_names, key=parse_timestamp)


def get_and_verify_latest_snapshot(conf, backup_dir, dataset):
    snaps = get_snapshot_names(backup_dir)
    if len(snaps) == 0:
        return None
    latest = snaps[-1]
    zfs_snapshots = [x.split("@")[1] for x in zfs_get_dataset_snapshots(conf, dataset)]
    if latest not in zfs_snapshots:
        # If this happens something is wrong, not sure how to proceed.
        # Did someone  manually delete the snapshots from the zfs server?
        # An easy way out would be to delete the whole directory and let the backup create a fresh full snapshot and take it from there.
        raise Exception(
            "{0}@{1} snapshot is not on the zfs server, cannot take incremental snapshot. aborting.".format(
                dataset, latest
            )
        )
    return latest


def snapshot(conf, backup_dir, dataset, now):
    nowtime = datetime.utcfromtimestamp(now).strftime(TIME_FORMAT)
    newest_snapshot = get_and_verify_latest_snapshot(conf, backup_dir, dataset)
    zfs_snapshot(conf, dataset, nowtime)
    if newest_snapshot is None:
        zfs_dump_snapshot(conf, backup_dir, dataset, nowtime)
    else:
        ctime = parse_timestamp(newest_snapshot)
        delta_days = (now - ctime) / (60.0 * 60 * 24)
        if delta_days >= conf.backup.interval_days.incremental:
            zfs_dump_snapshot(conf, backup_dir, dataset, nowtime, newest_snapshot)
        else:
            print(f"Latest dir new enough, skipping {dataset} snapshot")


# returns a sorted list of tupples (group_dir, snapshot_type, snapshot_name, dir)
# list is sorted from older to newer snapshots
def get_stored_snapshots(dataset_dir):
    snapshots = []
    for group_dir in os.listdir(dataset_dir):
        for snapshot_dir in os.listdir(f"{dataset_dir}/{group_dir}"):
            snap_type = snapshot_dir.split("##")[0]
            snap_name  = snapshot_dir.split("##")[1]
            snapshots.append((group_dir, snap_type, snap_name, f"{group_dir}/{snapshot_dir}"))

    return sorted(snapshots, key=lambda x: parse_timestamp(x[2]))


def backup(conf, args):
    now = int(time.time())  # UTC unixtime
    if args.dataset:
        backup_dir = get_backup_directory(conf, args.dataset, now)
        snapshot(conf, backup_dir, args.dataset, now)
    else:
        for dataset in conf.backup.datasets:
            backup_dir = get_backup_directory(conf, dataset, now)
            snapshot(conf, backup_dir, dataset, now)

def restore(conf, args):
    dataset, snapshot = args.snapshot.split("@")
    dest_dataset = args.dest_dataset
    if dest_dataset is None:
        dest_dataset = f"{dataset}_restore"
    print(f"Restoring snapshot {dataset}@{snapshot} to {dest_dataset}")
    dataset_dir = f"{conf.backup.directory}/{normalize_dataset_name(dataset)}"
    if not os.path.exists(dataset_dir):
        raise Exception(f"Directory does not exist {dataset_dir}")
    # finds group:
    snapshots = get_stored_snapshots(dataset_dir)
    group_dir = [group_dir for group_dir, snap_type, snap_name, dir in snapshots if snap_name == snapshot][0]
    snapshot_names = [x[2] for x in snapshots]

    def index_of(lst, predicate):
        for idx, e in enumerate(lst):
            if predicate(e): return idx
        return -1
    # group_dir, snap_type, snap_name, dir
    first_index = index_of(snapshots, lambda x: x[0] == group_dir)
    last_index = index_of(snapshots, lambda x: x[2] == snapshot)
    assert first_index != -1 and last_index != -1
    for group_dir, snap_type, snap_name, dir in snapshots[first_index:last_index+1]:
        files = [f"{dataset_dir}/{dir}/{file}" for file in sorted(os.listdir(f"{dataset_dir}/{dir}"))]
        cat = Popen(["cat"] + files, stdout=PIPE)
        gunzip = chain(cat, ["gunzip", "-c"])
        ssh = chain(gunzip, get_ssh_cmd_arr(conf) + ["zfs", "recv", "-F", dest_dataset])
        cat.stdout.close()
        gunzip.stdout.close()
        ssh.communicate()
        cat.wait()
        gunzip.wait()


def list_dataset_snapshots(conf, dataset):
    print(f"{dataset}:")
    dataset_dir = "%s/%s" % (conf.backup.directory, normalize_dataset_name(dataset))
    if os.path.exists(dataset_dir):
        for (group_dir, snap_type, snap_name, dir) in get_stored_snapshots(dataset_dir):
            marker = "=" if snap_type == 'full' else '+' # = full, + = incremental
            print(f"\t{marker} {dataset}@{snap_name}")


def cleanup_dataset_snapshots(conf, dataset):
    now = int(time.time())  # UTC unixtime
    dataset_dir = "%s/%s" % (conf.backup.directory, normalize_dataset_name(dataset))
    if not os.path.exists(dataset_dir):
        raise Exception(f"Directory does not exist {dataset_dir}")
    # Cleaning up old snapshot dump dirs
    for dir in os.listdir(dataset_dir):
        timestamp = int(parse_timestamp(dir))
        delta_seconds = now - timestamp
        if conf.backup.retention_days <= delta_seconds / (60.0 * 60 * 24):
            print(f"Deleting old snapshot dir {dataset_dir}/{dir}")
            shutil.rmtree(f"{dataset_dir}/{dir}")

    # Cleaning up old zfs snapshots
    for snapshot in [x.split("@")[1] for x in zfs_get_dataset_snapshots(conf, dataset)]:
        timestamp = int(parse_timestamp(snapshot))
        # manual snapshots will not have parseable timestamp, we should leave those alone
        if timestamp != 0:
            delta_seconds = now - timestamp
            if conf.backup.retention_days <= delta_seconds / (60.0 * 60 * 24):
                print(f"Deleting old ZFS snapshot {dataset}@{snapshot} from server")
                ssh_cmd(conf, ["zfs", "destroy", f"{dataset}@{snapshot}"])


def list_snapshots(conf, args):
    if args.dataset is not None:
        list_dataset_snapshots(conf, args.dataset)
    else:
        # list all dataset snapshots
        for dataset in conf.backup.datasets:
            list_dataset_snapshots(conf, dataset)


def cleanup_snapshots(conf, args):
    if args.dataset is not None:
        cleanup_dataset_snapshots(conf, args.dataset)
    else:
        # list all dataset snapshots
        for dataset in conf.backup.datasets:
            cleanup_dataset_snapshots(conf, dataset)

def main():
    parser = argparse.ArgumentParser(
        description="snapdump : backup and restore zfs snapshots to/from a foreign file system"
    )
    parser.add_argument(
        "--conf",
        "-c",
        help="Config file name",
        type=str,
        default='config.yml',
    )
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")
    backup_parser = subparsers.add_parser("backup", help="Backup")
    backup_parser.add_argument(
        "--dataset", "-d", help="Optional dataset to operate on", type=str
    )
    restore_parser = subparsers.add_parser("restore", help="Restore")
    restore_parser.add_argument(
        "--snapshot",
        "-s",
        help="Snapshot to restore (for example storage/datasets01@2018_12_06__21_47_58)",
        type=str,
        required=True,
    )
    restore_parser.add_argument(
        "--dest-dataset",
        "-d",
        help="Name of destination dataset, by default uses DATASET_restore as the name (ex storage/home_restore)",
        type=str,
        required=False,
    )
    list_parser = subparsers.add_parser("list", help="Restore")
    list_parser.add_argument(
        "--dataset", "-d", help="Dataset to list snapshots for, default all", type=str
    )
    cleanup_parser = subparsers.add_parser(
        "cleanup", help="Cleanup old snapshots and dump directories"
    )
    cleanup_parser.add_argument(
        "--dataset", "-d", help="Dataset to cleanup, default all", type=str
    )
    args = parser.parse_args()
    conf = OmegaConf.from_filename(args.conf)

    if args.command == "backup":
        backup(conf, args)
    elif args.command == "restore":
        restore(conf, args)
    elif args.command== "list":
        list_snapshots(conf, args)
    elif args.command == "cleanup":
        cleanup_snapshots(conf, args)
    else:
        parser.print_help()

if __name__ == "__main__":
   main()

