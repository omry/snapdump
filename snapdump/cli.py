#!/usr/bin/env python3

import argparse
import os
import shutil
from subprocess import Popen, PIPE, check_output
import time
from datetime import datetime
import glob
from omegaconf import OmegaConf
import pkg_resources
import re

CRON = False
VERBOSE = False
TIME_FORMAT = "%Y_%m_%d__%H_%M_%S"
SNAPSHOT_SUFFIX = "snapshot-part-"
TEMPDIR_SUFFIX = "dump-in-progress"

def log(msg):
    if not CRON:
        print(msg)
    
def get_ssh_cmd_arr(conf):
    cmd = ["ssh", f"{conf.server.ssh_user}@{conf.server.hostname}"]
    if conf.server.identity_file is not None:
        cmd += ["-i", f"{conf.server.identity_file}"]
    if conf.server.ssh_options is not None:
        cmd += conf.server.ssh_options.split(' ')
    return cmd


def ssh_cmd(conf, command):
    cmd = get_ssh_cmd_arr(conf) + command
    if VERBOSE:
        log('EXECUTING "{0}"'.format(" ".join(cmd)))
    return check_output(cmd)


def normalize_dataset_name(dataset):
    return dataset.replace("/", "_")


def parse_timestamp(dirname):
    try:
        d = datetime.strptime(dirname + " +0000", TIME_FORMAT + " %z")
        return d.timestamp()
    except ValueError as err:
        if VERBOSE:
            log(f"Error parsing timestamp in {dirname} : {err}")
        return 0


def get_backup_directory(conf, dataset, now):
    nowtime = datetime.utcfromtimestamp(now).strftime(TIME_FORMAT)
    dataset_dir = f"{conf.backup.directory}/{normalize_dataset_name(dataset)}"
    newest_dir = get_newest_file(dataset_dir)
    full_dir = "%s/%s" % (dataset_dir, nowtime)
    if newest_dir is None:
        os.makedirs(full_dir)
        if VERBOSE:
            log(f"Directory does not exist, creating a new directory : {full_dir}")
    else:
        ctime = parse_timestamp(newest_dir)
        delta_days = (now - ctime) / (60.0 * 60 * 24)
        if delta_days >= conf.backup.interval_days.full:
            os.makedirs(full_dir)
            if VERBOSE:
                log(
                    "Old directory older than %.2f days, creating a new one directory : %s"
                    % (conf.backup.interval_days.full, full_dir)
                )
        else:
            full_dir = f"{dataset_dir}/{newest_dir}"
            if VERBOSE:
                log(
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
        log(f"Deleting dead temporary dump dir : {tempdir}")
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


def ensure_clean_exit(process):
    if process.returncode != 0:
        raise Exception(f"{' '.join(process.args)} exited with non zero exit code {process.returncode}")


def zfs_dump_snapshot(conf, backup_dir, dataset, snapshot_name, base_snapshot_name=None):
    zfs_cmd = ["zfs", "send", f"{dataset}@{snapshot_name}"]
    backup_type = "full"
    if base_snapshot_name is None:
        log("Creating full snapshot dump for {0}@{1}".format(dataset, snapshot_name))
    else:
        log(
            "Creating incremental snapshot dump for {0}@{1} based on {2}".format(
                dataset, snapshot_name, base_snapshot_name
            )
        )
        zfs_cmd += ["-i", base_snapshot_name]
        backup_type = "incr"

    if is_dump_in_progress(conf, backup_dir):
        log(f"Dump is already in progress in {backup_dir}, bailing up")
        return False

    # delete dead dump directories
    delete_temporary_dump_dirs(backup_dir)

    parts_dir = f"{backup_dir}/{backup_type}##{snapshot_name}"
    temporary_dir = f"{parts_dir}.{TEMPDIR_SUFFIX}"
    os.makedirs(temporary_dir)

    ssh = Popen(get_ssh_cmd_arr(conf) + zfs_cmd, stdout=PIPE)
    gzip = chain(ssh, "gzip")
    split = chain(
        gzip,
        ["split", "-b", conf.backup.split_size, "-a3", "-", f"{temporary_dir}/{SNAPSHOT_SUFFIX}"],
    )

    ssh.stdout.close()
    gzip.stdout.close()
    split.communicate()
    ssh.wait()
    gzip.wait()
    ensure_clean_exit(ssh)
    ensure_clean_exit(gzip)
    ensure_clean_exit(split)

    cleanup_dataset_snapshots(conf, dataset)

    os.rename(temporary_dir, parts_dir)
    return True

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


# returns sorted snapshot names in the directory
def get_snapshot_names(backup_dir):
    files = os.listdir(backup_dir)
    files = list(filter(lambda x: len(x.split("##")) == 2 and not x.endswith(TEMPDIR_SUFFIX), files))
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
        # An easy way out would be to delete the whole directory and let the
        # backup create a fresh full snapshot and take it from there.
        raise Exception(
            "{0}@{1} snapshot is not on the zfs server, cannot take incremental snapshot. aborting.".format(
                dataset, latest
            )
        )
    return latest


def snapshot(conf, backup_dir, dataset, now, verify):
    nowtime = datetime.utcfromtimestamp(now).strftime(TIME_FORMAT)
    newest_snapshot = get_and_verify_latest_snapshot(conf, backup_dir, dataset)
    zfs_snapshot(conf, dataset, nowtime)
    created = False
    if newest_snapshot is None:
        created = zfs_dump_snapshot(conf, backup_dir, dataset, nowtime)
    else:
        ctime = parse_timestamp(newest_snapshot)
        delta_days = (now - ctime) / (60.0 * 60 * 24)
        if delta_days >= conf.backup.interval_days.incremental:
            created = zfs_dump_snapshot(conf, backup_dir, dataset, nowtime, newest_snapshot)
        else:
            log(f"Latest dir new enough, skipping {dataset} snapshot")
            return

    if verify and created:
        verify_impl(conf, dataset, nowtime)


# returns a sorted list of tupples (group_dir, snapshot_type, snapshot_name, dir)
# list is sorted from older to newer snapshots
def get_stored_snapshots(dataset_dir):
    snapshots = []
    for group_dir in os.listdir(dataset_dir):
        for snapshot_dir in os.listdir(f"{dataset_dir}/{group_dir}"):
            snap_type = snapshot_dir.split("##")[0]
            snap_name = snapshot_dir.split("##")[1]
            snapshots.append((group_dir, snap_type, snap_name, f"{group_dir}/{snapshot_dir}"))

    return sorted(snapshots, key=lambda x: parse_timestamp(x[2]))


def backup(conf, args):
    now = int(time.time())  # UTC unixtime
    verify = not args.no_verify
    if args.dataset:
        backup_dir = get_backup_directory(conf, args.dataset, now)
        snapshot(conf, backup_dir, args.dataset, now, verify)
    else:
        for dataset in conf.backup.datasets:
            backup_dir = get_backup_directory(conf, dataset, now)
            snapshot(conf, backup_dir, dataset, now, verify)

def get_snapshots_chain(dataset_dir, snapshot_name):
    if not os.path.exists(dataset_dir):
        raise Exception(f"Directory does not exist {dataset_dir}")
    # finds group:
    snapshots = get_stored_snapshots(dataset_dir)
    groups = [group_dir for group_dir, snap_type, snap_name, directory in snapshots if snap_name == snapshot_name]
    if len(groups) == 0:
        raise Exception(f"snapshot '{snapshot_name}' does not exist")
    group = groups[0]

    def index_of(lst, predicate):
        for idx, e in enumerate(lst):
            if predicate(e):
                return idx
        return -1


    def index_of(lst, predicate):
        for idx, e in enumerate(lst):
            if predicate(e):
                return idx
        return -1

    # finds group:
    snapshots = get_stored_snapshots(dataset_dir)
    group_dir = [group_dir for group_dir, snap_type, snap_name, directory in snapshots if snap_name == snapshot_name][0]

    # group_dir, snap_type, snap_name, dir
    first_index = index_of(snapshots, lambda x: x[0] == group_dir)
    last_index = index_of(snapshots, lambda x: x[2] == snapshot_name)
    assert first_index != -1 and last_index != -1
    return snapshots[first_index:last_index + 1]

def restore(conf, args):
    dataset, snapshot_name = args.snapshot.split("@")
    dest_dataset = args.dest_dataset
    if dest_dataset is None:
        dest_dataset = f"{dataset}_restore"
    log(f"Restoring snapshot {dataset}@{snapshot_name} to {dest_dataset}")
    dataset_dir = f"{conf.backup.directory}/{normalize_dataset_name(dataset)}"
    for group_dir, snap_type, snap_name, directory in get_snapshots_chain(dataset_dir, snapshot_name):
        files = [f"{dataset_dir}/{directory}/{file}" for file in sorted(os.listdir(f"{dataset_dir}/{directory}"))]
        cat = Popen(["cat"] + files, stdout=PIPE)
        gunzip = chain(cat, ["gunzip", "-c"])
        ssh = chain(gunzip, get_ssh_cmd_arr(conf) + ["zfs", "recv", "-F", dest_dataset])
        cat.stdout.close()
        gunzip.stdout.close()
        ssh.communicate()
        cat.wait()
        gunzip.wait()
        ensure_clean_exit(ssh)
        ensure_clean_exit(gunzip)
        ensure_clean_exit(cat)

def verify_impl(conf, dataset, snapshot_name):
    log(f"Verifying snapshot {dataset}@{snapshot_name}")
    dataset_dir = f"{conf.backup.directory}/{normalize_dataset_name(dataset)}"

    files = []
    for group_dir, snap_type, snap_name, directory in get_snapshots_chain(dataset_dir, snapshot_name):
        files += [f"{dataset_dir}/{directory}/{file}" for file in sorted(os.listdir(f"{dataset_dir}/{directory}"))]
    cat = Popen(["cat"] + files, stdout=PIPE)
    gunzip = chain(cat, ["gunzip", "-c"])
    ssh = chain(gunzip, get_ssh_cmd_arr(conf) + ["zstreamdump"])
    cat.stdout.close()
    gunzip.stdout.close()
    out = ssh.communicate()
    cat.wait()
    gunzip.wait()
    ensure_clean_exit(ssh)
    ensure_clean_exit(gunzip)
    ensure_clean_exit(cat)

    from_guid = -1
    to_guid = -1
    prev_to_guid = -1
    #toguid = 6314ecefe1c7f1d8
    # fromguid = 0
    reg = re.compile(r"(toguid|fromguid) = ([\w]+)")
    for s in out[0].splitlines():
        s = s.decode("utf-8").strip()
        m = reg.match(s)
        if m:
            guid_type = m.group(1)
            guid = m.group(2)
            if guid_type == 'toguid':
                prev_to_guid = to_guid
                to_guid = guid
            elif guid_type == 'fromguid':
                from_guid = guid
                if from_guid != '0' and from_guid != prev_to_guid:
                    raise Exception(f"Mistmatch in guid chain : {from_guid} != {to_guid}")
    log("ZFS stream intact")

def verify(conf, args):
    dataset, snapshot_name = args.snapshot.split("@")
    verify_impl(conf, dataset, snapshot_name)



def list_dataset_snapshots(conf, dataset):
    print(f"{dataset}:")
    dataset_dir = "%s/%s" % (conf.backup.directory, normalize_dataset_name(dataset))
    if os.path.exists(dataset_dir):
        total = 0
        for (group_dir, snap_type, snap_name, directory) in get_stored_snapshots(dataset_dir):
            dump_dir = f"{dataset_dir}/{directory}"
            marker = "=" if snap_type == 'full' else '+'  # = full, + = incremental
            size_bytes = sum(os.path.getsize(f"{dump_dir}/{f}") for f in os.listdir(dump_dir) if os.path.isfile(f"{dump_dir}/{f}")) 
            total += size_bytes
            size_gb = size_bytes / (1024.0 * 1024 * 1024)
            total_gb = total / (1024.0 * 1024 * 1024)
            size_str = f"{total_gb:.2f}"
            if snap_type == 'incr':
                size_str += f" (+{size_gb:.2f})"
            print(f"\t{marker} {dataset}@{snap_name}, {size_str} GB")


def cleanup_dataset_snapshots(conf, dataset):
    now = int(time.time())  # UTC unixtime
    dataset_dir = "%s/%s" % (conf.backup.directory, normalize_dataset_name(dataset))
    if not os.path.exists(dataset_dir):
        raise Exception(f"Directory does not exist {dataset_dir}")
    # Cleaning up old snapshot dump dirs
    for directory in os.listdir(dataset_dir):
        timestamp = int(parse_timestamp(directory))
        delta_seconds = now - timestamp
        if conf.backup.retention_days <= delta_seconds / (60.0 * 60 * 24):
            log(f"Deleting old snapshot dir {dataset_dir}/{directory}")
            shutil.rmtree(f"{dataset_dir}/{directory}")

    # Cleaning up old zfs snapshots
    for snapshot_name in [x.split("@")[1] for x in zfs_get_dataset_snapshots(conf, dataset)]:
        timestamp = int(parse_timestamp(snapshot_name))
        # manual snapshots will not have parseable timestamp, we should leave those alone
        if timestamp != 0:
            delta_seconds = now - timestamp
            if conf.backup.retention_days <= delta_seconds / (60.0 * 60 * 24):
                log(f"Deleting old ZFS snapshot {dataset}@{snapshot_name} from server")
                ssh_cmd(conf, ["zfs", "destroy", f"{dataset}@{snapshot_name}"])


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
    version = pkg_resources.require("snapdump")[0].version
    parser = argparse.ArgumentParser(
        description="snapdump : backup and restore zfs snapshots to/from a foreign file system"
    )
    parser.add_argument('-v', '--version', action='version', version=f"snapdump {version}")
    parser.add_argument("--cron", '-q', help="Do not log anything except errors", action='store_true')
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
    backup_parser.add_argument(
        "--no-verify", "-n", help="Do not verify created stream", type=bool, nargs='?', const=True, default=False
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
    list_parser = subparsers.add_parser("list", help="List available snapshots to restore")
    list_parser.add_argument(
        "--dataset", "-d", help="Dataset to list snapshots for, default all", type=str
    )
    cleanup_parser = subparsers.add_parser(
        "cleanup", help="Cleanup old snapshots and dump directories"
    )
    cleanup_parser.add_argument(
        "--dataset", "-d", help="Dataset to cleanup, default all", type=str
    )

    verify_parser = subparsers.add_parser("verify", help="Verify the integrity of a snapshot chain")
    verify_parser.add_argument(
        "--snapshot",
        "-s",
        help="Snapshot to restore (for example storage/datasets01@2018_12_06__21_47_58)",
        type=str,
        required=True,
    )
    args = parser.parse_args()
    conf = OmegaConf.from_filename(args.conf)
    global CRON
    CRON = args.cron
    if args.command == "backup":
        backup(conf, args)
    elif args.command == "restore":
        restore(conf, args)
    elif args.command == "verify":
        verify(conf, args)
    elif args.command == "list":
        list_snapshots(conf, args)
    elif args.command == "cleanup":
        cleanup_snapshots(conf, args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
