#!/usr/bin/env spark-submit
from __future__ import print_function
import argparse
import json
import os

from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import col
import pyspark.sql.functions as fn
import pyspark.sql.types as types

# stolen from CMSSpark
import schemas

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

    if args.source == 'classads':
        jobreports = avroreader.load("/project/awg/cms/jm-data-popularity/avro-snappy/year=201[6789]/month=*/day=*/*.avro")
        working_set_day = (jobreports
                .filter((col('JobExecExitTimeStamp')>0) & (col('JobExecExitCode')==0))
                .replace('//', '/', 'FileName')
                .join(dbs_files, col('FileName')==col('f_logical_file_name'))
                .join(dbs_datasets, col('f_dataset_id')==col('d_dataset_id'))
                .withColumn('day', (col('JobExecExitTimeStamp')-col('JobExecExitTimeStamp')%fn.lit(86400000))/fn.lit(1000))
                .withColumn('input_campaign', fn.regexp_extract(col('d_dataset'), "^/[^/]*/((?:HI|PA|PN|XeXe|)Run201\d\w-[^-]+|CMSSW_\d+|[^-]+)[^/]*/", 1))
                .groupBy('day', 'SubmissionTool', 'input_campaign', 'd_data_tier_id', 'SiteName')
                .agg(
                    fn.collect_set('f_block_id').alias('working_set_blocks'),
                )
            )
        working_set_day.write.parquet(args.out)
    elif args.source == 'cmssw':
        jobreports = avroreader.load("/project/awg/cms/cmssw-popularity/avro-snappy/year=201[6789]/month=*/day=*/*.avro")
        working_set_day = (jobreports
                .join(dbs_files, col('FILE_LFN')==col('f_logical_file_name'))
                .join(dbs_datasets, col('f_dataset_id')==col('d_dataset_id'))
                .withColumn('day', (col('END_TIME')-col('END_TIME')%fn.lit(86400)))
                .withColumn('input_campaign', fn.regexp_extract(col('d_dataset'), "^/[^/]*/((?:HI|PA|PN|XeXe|)Run201\d\w-[^-]+|CMSSW_\d+|[^-]+)[^/]*/", 1))
                .withColumn('isCrab', col('APP_INFO').contains(':crab:'))
                .groupBy('day', 'isCrab', 'input_campaign', 'd_data_tier_id', 'SITE_NAME')
                .agg(
                    fn.collect_set('f_block_id').alias('working_set_blocks'),
                )
            )
        working_set_day.write.parquet(args.out)
    elif args.source == 'xrootd':
        jobreports = spark.read.json("/project/monitoring/archive/xrootd/raw/gled/201[6789]/*/*/*.json.gz")
        working_set_day = (jobreports
                .join(dbs_files, col('data.file_lfn')==col('f_logical_file_name'))
                .join(dbs_datasets, col('f_dataset_id')==col('d_dataset_id'))
                .withColumn('day', (col('data.end_time')-col('data.end_time')%fn.lit(86400000))/fn.lit(1000))
                .withColumn('input_campaign', fn.regexp_extract(col('d_dataset'), "^/[^/]*/((?:HI|PA|PN|XeXe|)Run201\d\w-[^-]+|CMSSW_\d+|[^-]+)[^/]*/", 1))
                .groupBy('day', 'input_campaign', 'd_data_tier_id', 'data.client_domain')
                .agg(
                    fn.collect_set('f_block_id').alias('working_set_blocks'),
                )
            )
        working_set_day.write.parquet(args.out)
    elif args.source == 'fwjr':
        jobreports_prod = avroreader.load("/cms/wmarchive/avro/fwjr/201[789]/*/*/*.avro")
        jobreports_crab = avroreader.schema(jobreports_prod.schema).load("/cms/wmarchive/avro/crab/201[789]/*/*/*.avro")
        jobreports = (jobreports_prod.union(jobreports_crab)
                            .select(fn.explode(col("steps")).alias("cmsRun"), "*")
                            .drop("steps")
                            .filter(col("cmsRun.name").isin(["cmsRun"]+["cmsRun%d" % i for i in range(5)]))
                            )
        # jobreports.printSchema()
        working_set_day = (jobreports
                .filter(col("meta_data.jobstate")=="success")
                .select(fn.explode(col('LFNArray')).alias('lfn'), col('meta_data.ts').alias('time'), col('cmsRun.site').alias('site_name'))
                .join(dbs_files, col('lfn')==col('f_logical_file_name'))
                .join(dbs_datasets, col('f_dataset_id')==col('d_dataset_id'))
                .withColumn('day', (col('time')-col('time')%fn.lit(86400)))
                .withColumn('input_campaign', fn.regexp_extract(col('d_dataset'), "^/[^/]*/((?:HI|PA|PN|XeXe|)Run201\d\w-[^-]+|CMSSW_\d+|[^-]+)[^/]*/", 1))
                .groupBy('day', 'input_campaign', 'd_data_tier_id', 'site_name')
                .agg(
                    fn.collect_set('f_block_id').alias('working_set_blocks'),
                )
            )
        working_set_day.write.parquet(args.out)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
            "Computes working set (unique blocks accessed) per day, partitioned by various fields."
            "Please run prefixed with `spark-submit --packages com.databricks:spark-avro_2.11:4.0.0`"
            )
    defpath = "hdfs://analytix/user/ncsmith/working_set_day"
    parser.add_argument("--out", metavar="OUTPUT", help="Output path in HDFS for result (default: %s)" % defpath, default=defpath)
    parser.add_argument("--source", help="Source", default='classads', choices=['classads', 'cmssw', 'xrootd', 'fwjr'])

    args = parser.parse_args()
    run(args)

