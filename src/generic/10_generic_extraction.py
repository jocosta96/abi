# Databricks notebook source
import csv
import chardet
import re

from delta import DeltaTable

import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text('name', '')
dbutils.widgets.text('source_type', '')
dbutils.widgets.text('extraction_method', '')
dbutils.widgets.text('date', '')

str_name = dbutils.widgets.get('name')
str_source_type = dbutils.widgets.get('source_type')
str_extraction_method = dbutils.widgets.get('extraction_method')

# COMMAND ----------

str_base_path = f'/mnt/05_landing/{str_source_type}/{str_extraction_method}/{str_name}'
str_dest_path = f'/mnt/10_bronze/{str_source_type}/{str_extraction_method}/{str_name}'

list_path = dbutils.fs.ls(
    str_base_path
)
str_max_year = max([x.name for x in list_path])

list_path_2 = dbutils.fs.ls(
    f'{str_base_path}/{str_max_year}'
)
str_max_month = max([x.name for x in list_path_2])

list_path_3 = dbutils.fs.ls(
    f'{str_base_path}/{str_max_year}{str_max_month}'
)
str_max_day = max([x.name for x in list_path_3])

if dbutils.widgets.get('date'):
    str_year, str_month, str_day = \
    dbutils.widgets.get('date').replace('-', '/-').split('-')
    
else:
    str_year, str_month, str_day = \
    str_max_year, str_max_month, str_max_day
    
str_path_final = f'{str_base_path}/{str_year}{str_month}{str_day}'

list_files = dbutils.fs.ls(
    str_path_final
)

# COMMAND ----------

sdf_mt = spark.read.table('metadata.landing').filter(f'name = "{str_name}"')

if sdf_mt.rdd.isEmpty():
    
    try:
        sdf_10 = spark.read.parquet(
            str_path_final
        )
        
        str_type = 'parquet'
        str_delimiter = ''
        str_charenc = ''
        
    except:
        
        str_file_name = [x.name for x in list_files][0]
        
        with open(f'/dbfs/{str_path_final}{str_file_name}', "rb") as rawdata:
            
            obj_result = chardet.detect(rawdata.read())
            str_charenc = obj_result['encoding']

        with open(f'/dbfs/{str_path_final}{str_file_name}','rt', encoding=str_charenc) as csvfile:

            obj_dialect = csv.Sniffer().sniff(csvfile.read(2048), delimiters=";,\t|")
            csvfile.seek(0)
            obj_reader = csv.reader(csvfile, obj_dialect)
            str_delimiter = obj_reader.dialect.delimiter
        
        sdf_10 = (
            spark.read.option('sep', str_delimiter)
            .option('header', 'true')
            .option('encoding', str_charenc)
            .csv(str_path_final)
        )
        
        str_type = 'csv'
        
    spark.sql(f"""
        INSERT INTO metadata.landing
            VALUES('{str_name}', '{str_base_path}', '{str_type}', '{str_delimiter}', '{str_charenc}')
    """)
    
else:
    
    nmtp_params = sdf_mt.collect()[0]
    
    sdf_10 = (
        spark.read.format(nmtp_params.type)
            .option('sep', nmtp_params.sep)
            .option('path', str_path_final)
            .option('header', 'true')
            .load()
    )

# COMMAND ----------

sdf_final = sdf_10.select(
    *[F.col(x).alias(re.sub('\W+','', x)) for x in sdf_10.columns]
)

# COMMAND ----------

resp = (
    sdf_final.write.format("delta")
      .mode('overwrite')
      .option('delta.enableChangeDataFeed', 'true')
      .option('delta.logRetentionDuration', f'interval 7 days')
      .option('delta.autoOptimize.autoCompact', 'true')        
      .option('path', str_dest_path)
      .saveAsTable(f'10_bronze.{str_name}')
)

# COMMAND ----------

#DeltaTable.forName(spark, f'10_bronze.{str_name}').vacuum()
