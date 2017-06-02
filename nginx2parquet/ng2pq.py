#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Nginx to parquet.

Usage:
  ng2pq.py [--input=<s3_object>] [--output=<base_location>]
  ng2pq.py -h | --help

Options:
  -h --help       Show this screen.
  --version       Show version.
  --input=<s3_object>  Source [default: None].
  --output=<base_location>  Target [default: None].

"""
from __future__ import print_function
from docopt import docopt
from pyarrow import parquet as pq
from tempfile import NamedTemporaryFile
from collections import OrderedDict
import re
import os
import sys
import boto3
import botocore
import pandas as pd
import pyarrow as pa
import logging.config


logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)


def parse(log):
    pat = (
        r''
        '(?P<remote_addr>\d+.\d+.\d+.\d+)\s-\s'
        '(?P<remote_user>.+)\s'
        '\[(?P<time_local>.+)\]\s'
        '"(?P<request_method>\S+)\s'
        '(?P<request_url>\S+)\s'
        '(?P<request_protocol>\S+)"\s'
        '(?P<response_status>\d+)\s'
        '(?P<bytes_sent>\d+)\s'
        '"(?P<http_referrer>\S+)"\s'
        '"(?P<http_user_agent>.+)"'
    )
    match = re.match(pat, log)
    if match:
        return match.groupdict()
    return False


def convert(logs):
    df = pd.DataFrame(logs)
    # %z not parsed!
    # ptn = '%d/%b/%Y:%H:%M:%S %z'
    ptn = '%d/%b/%Y:%H:%M:%S +0900'
    df['time_local'] = pd.to_datetime(df['time_local'], format=ptn)
    df['datetime'] = df['time_local'].dt.strftime('%Y-%m-%dT%H:%M:%S+09:00')
    df['yyyymmdd'] = df['time_local'].dt.strftime('%Y%m%d')
    df['time_local'] = df['time_local'].apply(str)
    df['yyyymmdd'] = df['yyyymmdd'].apply(str)
    df['response_status'] = df['response_status'].apply(int)
    df['bytes_sent'] = df['bytes_sent'].apply(int)
    tables = {}
    for yyyymmdd in df['yyyymmdd'].unique():
        grouped_df = df.loc[df['yyyymmdd'] == yyyymmdd]
        grouped_df.drop('yyyymmdd', axis=1)
        table = pa.Table.from_pandas(grouped_df)
        tables[yyyymmdd] = table
    return tables


def save(parquet_table, fname):
    pq.write_table(parquet_table, fname)
    return os.path.join(os.getcwd(), fname)


def split_s3_bucket_key(s3_path):
    if not s3_path.startswith('s3://'):
        return ()
    s3_path = s3_path[5:]
    s3_components = s3_path.split('/')
    bucket = s3_components[0]
    s3_key = ""
    if len(s3_components) > 1:
        s3_key = '/'.join(s3_components[1:])
    return bucket, s3_key


def download_from_s3(bucket, key):
    temp_fname = NamedTemporaryFile().name
    s3 = boto3.resource('s3')
    try:
        s3.Bucket(bucket).download_file(key, temp_fname)
    except botocore.exceptions.ClientError as e:
        temp_fname = None
        logger.warning(e)
    return temp_fname


def upload_to_s3(fname, bucket, key):
    s_u_c_c_e_s_s_f_u_l = True
    if not os.path.exists(fname):
        logger.warning('Upload file not exist. {0}'.format(fname))
        return not s_u_c_c_e_s_s_f_u_l
    try:
        s3 = boto3.client('s3')
        s3.upload_file(fname, bucket, key)
    except botocore.exceptions.ClientError as e:
        s_u_c_c_e_s_s_f_u_l = False
        logger.warning(e)
    return s_u_c_c_e_s_s_f_u_l


def add_partition_key(s3_base_key, s3_obj_name, partition):
    partitioned_key = '/'.join(
        ['='.join([str(k), str(v)]) for k, v in partition.iteritems()]
    )
    if s3_base_key[-1] == '/':
        s3_base_key = s3_base_key[:-1]
    return '{0}/{1}/{2}'.format(s3_base_key, partitioned_key, s3_obj_name)


def main():
    arguments = docopt(__doc__, version='NG2PQ v0.1')
    i = split_s3_bucket_key(arguments.get('--input', None))
    o = split_s3_bucket_key(arguments.get('--output', None))
    if not all([i, o]):
        print(__doc__)
        sys.exit(1)
    (in_bucket, in_key) = i
    (out_bucket, out_base_key) = o
    out_obj_name = in_key.split()[-1]
    source_fname = download_from_s3(in_bucket, in_key)
    logs = []
    with open(source_fname) as f:
        for line in f.readlines():
            parsed_log = parse(line)
            logs.append(parsed_log)
    tables = convert(logs)
    for yyyymmdd, table in tables.iteritems():
        saved_fname = save(table, '{0}.{1}'.format(source_fname, yyyymmdd))
        key = add_partition_key(
            out_base_key, out_obj_name,
            OrderedDict([('year', yyyymmdd[:4]),
                         ('month', yyyymmdd[4:6]),
                         ('day', yyyymmdd[6:])])
        )
        upload_to_s3(saved_fname, out_bucket, key)


if __name__ == '__main__':
    main()
