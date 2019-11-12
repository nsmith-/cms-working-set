#!/usr/bin/env spark-submit
from __future__ import print_function
from itertools import chain
import argparse
import json
import os

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as fn

# stolen from CMSSpark
import schemas

def add_domain_location(df, domain, mapping, colname):

    ''' Add location (us/eu) to dataframe. I.e. if domain is fnal.gov, location
    is us.
    df: dataframe
    domain: dataframe column which is used to derive location
    mapping: maps domain to location
    colname: column name of location '''

    df = df.withColumn(colname, mapping[fn.substring_index(df[domain], '.', -1)])

    return df

def get_mapping():

    ''' Generate (domain, location) mapping. '''

    # Define mapping first in a dictionary
    d_dl = {'eu': ['ad', 'al', 'ar', 'at', 'ba', 'be', 'bg', 'by',
                   'ch', 'cy', 'cz', 'de', 'dk', 'ee', 'es', 'eu',
                   'fi', 'fr', 'gi', 'gr', 'hr', 'hu', 'ie', 'im',
                   'it', 'li', 'lt', 'lu', 'lv', 'mc', 'md', 'me',
                   'mk', 'mt', 'nl', 'no', 'pl', 'pt', 'ro', 'rs',
                   'se', 'si', 'sk', 'ua', 'uk', 'va'],
            'us': ['org', 'net', 'edu', 'gov', 'mil', 'ca', 'us'],
            'other': ['com', 'int', 'arpa', 'ac', 'ae', 'af', 'ag',
                      'ai', 'am', 'ao', 'aq', 'as', 'au', 'aw',
                      'ax', 'az', 'bb', 'bd', 'bf', 'bh', 'bi',
                      'bj', 'bm', 'bn', 'bo', 'br', 'bs', 'bt',
                      'bw', 'bz', 'cc', 'cd', 'cf', 'cg', 'ci',
                      'ck', 'cl', 'cm', 'cn', 'co', 'cr', 'cu',
                      'cv', 'cw', 'cx', 'dj', 'dm', 'do', 'dz',
                      'ec', 'eg', 'er', 'et', 'fj', 'fk', 'fm',
                      'fo', 'ga', 'gd', 'ge', 'gf', 'gh', 'gl',
                      'gm', 'gn', 'gp', 'gq', 'gs', 'gt', 'gu',
                      'gw', 'gy', 'hk', 'hm', 'hn', 'ht', 'id',
                      'il', 'in', 'io', 'iq', 'ir', 'is', 'je',
                      'jm', 'jo', 'jp', 'ke', 'kg', 'kh', 'ki',
                      'km', 'kn', 'kp', 'kr', 'kw', 'ky', 'kz',
                      'la', 'lb', 'lc', 'lk', 'lr', 'ls', 'ly',
                      'ma', 'mg', 'mh', 'ml', 'mn', 'mo', 'mp',
                      'mq', 'mr', 'ms', 'mu', 'mv', 'mw', 'mx',
                      'my', 'mz', 'na', 'nc', 'ne', 'nf', 'ng',
                      'ni', 'np', 'nr', 'nu', 'nz', 'om', 'pa',
                      'pe', 'pf', 'pg', 'ph', 'pk', 'pm', 'pn',
                      'pr', 'ps', 'pw', 'py', 'qa', 're', 'ru',
                      'rw', 'sa', 'sb', 'sc', 'sd', 'sg', 'sh',
                      'sl', 'sm', 'sn', 'so', 'sr', 'st', 'su',
                      'sv', 'sx', 'sy', 'sz', 'tc', 'td', 'tf',
                      'tg', 'th', 'tj', 'tk', 'tl', 'tm', 'tn',
                      'to', 'tr', 'tt', 'tv', 'tw', 'tz', 'ug',
                      'uy', 'uz', 'vc', 've', 'vg', 'vi', 'vn',
                      'vu', 'wf', 'ws', 'ye', 'yt', 'za', 'zm',
                      'zw']
           }

    # Create list with pairs
    map_dl = [ (k,v) for v,ks in d_dl.items() for k in ks ]
    # e.g. [('fi', 'eu'), ('uk', 'eu'), ('net', 'us')]

    # Create pyspark mapping
    mapping = fn.create_map([fn.lit(i) for i in chain.from_iterable(map_dl)])
    # e.g. Column<'map(fi, eu, uk, eu, net, us)'>

    return mapping

def run(args):
    conf = SparkConf().setMaster("yarn").setAppName("CMS Working Set")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    print("Initiated spark session on yarn, web URL: http://ithdp1101.cern.ch:8088/proxy/%s" % sc.applicationId)

    avroreader = spark.read.format("com.databricks.spark.avro")
    csvreader = spark.read.format("com.databricks.spark.csv").option("nullValue","null").option("mode", "FAILFAST")

    dbs_files = csvreader.schema(schemas.schema_files()).load("/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/FILES/part-m-00000")
    dbs_blocks = csvreader.schema(schemas.schema_blocks()).load("/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/BLOCKS/part-m-00000")
    dbs_datasets = csvreader.schema(schemas.schema_datasets()).load("/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/DATASETS/part-m-00000")

    for year in [2019]:
        for month in range(1, 13):

            # Zero padding month
            month = str(month).zfill(2)

            print('Access xrootd data for {}-{}.'.format(year, month))

            # Load xrootd JSONs and PhEDEx CSVs
            try:
                jobreports_xrootd = spark.read.json('/project/monitoring/archive/xrootd/raw/gled/{}/{}/*/*.json.gz'.format(year, month))
                jobreports_phedex = csvreader.schema(schemas.schema_phedex()).load('/project/awg/cms/phedex/block-replicas-snapshots/csv/time={}-{}-01_??h??m??s/part-m-00000'.format(year, month))
            except:
                print('Skip {}-{}, since no data seem to exist.'.format(year, month))
                continue

            # Get mapping, e.g.
            # ch --> eu
            # us --> us (d'oh!)
            mapping = get_mapping()

            # Convert to dataframes with needed columns
            df_phedex = (jobreports_phedex
                    .withColumn('phedex_datetime', fn.from_unixtime(col('now_sec')))
                    .withColumn('phedex_date', fn.date_format(fn.from_unixtime(col('now_sec')), 'yyyy-MM-dd'))
                    .withColumn('location_with_dataset', mapping[fn.lower(fn.substring_index(fn.substring_index(col('node_name'), '_', 2), '_', -1))])
                    )
            df_xrootd = (jobreports_xrootd
                    .join(dbs_files, col('data.file_lfn')==col('f_logical_file_name'))
                    .join(dbs_datasets, col('f_dataset_id')==col('d_dataset_id'))
                    .join(dbs_blocks, col('f_block_id')==col('b_block_id'))
                    .withColumn('xrootd_datetime', fn.from_unixtime(col('data.start_time')/fn.lit(1000)))
                    .withColumn('xrootd_date', fn.date_format(fn.from_unixtime(col('data.start_time')/fn.lit(1000)), 'yyyy-MM-dd'))
                    )

            # Columns to be stored for final dataframe
            cols = ['xrootd_datetime', 'xrootd_date', 'data.client_domain', 'data.client_host',
                    'data.server_domain', 'b_block_id', 'b_block_name', 'data.operation', 'data.read_bytes', 'data.write_bytes']

            df_xrootd = (df_xrootd
                    .join(df_phedex, [col('xrootd_date') == col('phedex_date'), col('b_block_name') == col('block_name')])
                    .filter(col('data.operation') == 'read')      # Filters out write accesses
                    .groupBy(cols)
                    .agg(fn.collect_set('location_with_dataset').alias('locations_with_dataset'))
                    )

            ## Show physical plan
            #print('Here is the physical plan, that Spark will execute.')
            #df_xrootd.explain()

            # Extract locations for server and client domains
            df_xrootd = add_domain_location(df_xrootd, 'server_domain', mapping, 'server_location')
            df_xrootd = add_domain_location(df_xrootd, 'client_domain', mapping, 'client_location')

            # Filter out IP addresses
            df_xrootd = df_xrootd.filter(col('client_location').isNotNull())

            # Check if dataset is available at client location
            df_xrootd = df_xrootd.withColumn('dataset_at_client_location', fn.expr('array_contains(locations_with_dataset, client_location)'))

            # Sum read bytes by server location, client location, and if dataset would also have been available at client location
            df_xrootd = (df_xrootd
                         .groupBy('server_location', 'client_location', 'dataset_at_client_location')
                         .sum('read_bytes')
                         .withColumnRenamed('sum(read_bytes)', 'total_read_bytes')
                        )

            # Write dataframe to parquet files
            df_xrootd.write.parquet('{}_{}-{}'.format(args.out, year, month))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
            "Computes working set (unique blocks accessed) per day, partitioned by various fields."
            "Please run prefixed with `spark-submit --packages com.databricks:spark-avro_2.11:4.0.0`"
            )
    defpath = "hdfs://analytix/user/bschneid/test02/generate_set"
    parser.add_argument("--out", metavar="OUTPUT", help="Output path in HDFS for result (default: %s)" % defpath, default=defpath)

    args = parser.parse_args()
    run(args)

