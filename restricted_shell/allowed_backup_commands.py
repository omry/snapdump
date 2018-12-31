#!/usr/local/bin/python

import os
import re
import sys
import subprocess

datasets = [
    'storage/home',
    'storage/datasets01',
]

def denied():
    print("Access denied")
    sys.exit(1)


if 'SSH_ORIGINAL_COMMAND' not in os.environ:
    denied()

cmd = os.environ['SSH_ORIGINAL_COMMAND']

# read only, can take arbitrary flags
re_list = re.compile(r'^zfs list( -\w( [\w/_-]+)?)*$')

# work 	exactly on one snapshot and does not take any additional flags
re_snap_ops = re.compile(r'^zfs (snapshot|destroy) ([\w/]+)@([\w]+)$')
re_send = re.compile(r'^zfs send ([\w/]+)@([\w]+)( -i [\w]+)?$')
re_recv = re.compile(r'^zfs recv -F ([\w/]+)$')
zstreamdump = re.compile(r'^zstreamdump$')

def unsupported_dataset_error(a_dataset):
    print(f"{a_dataset} is not in the list of managed datasets, fix in {__file__} in the server")
    sys.exit(1)

def execute(a_cmd):
    subprocess.call(a_cmd, shell=True)

if zstreamdump.match(cmd) or re_list.match(cmd):
    execute(cmd)
elif re_snap_ops.match(cmd):
    m = re_snap_ops.match(cmd)
    op = m.group(1)
    dataset = m.group(2)
    snapshot = m.group(3)
    if dataset in datasets:
        execute(cmd)
    else:
        unsupported_dataset_error(dataset)
elif re_send.match(cmd):
    m = re_send.match(cmd)
    dataset = m.group(1)
    if dataset in datasets:
        execute(cmd)
    else:
        unsupported_dataset_error(dataset)
elif re_recv.match(cmd):
    # no need to verify dataset because we are creatign a new one
    # if dataset is already exist we will have an error, and since we
    # do not allow arbitrary flags the client can't force
    execute(cmd)
else:
    denied()
