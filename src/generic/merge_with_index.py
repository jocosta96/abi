# Databricks notebook source
from delta import DeltaTable

import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %run ./modeling

# COMMAND ----------

def index(idx_name, sdf, start=0):
    
    sdf = sdf.select('*', f.spark_partition_id(), f.monotonically_increasing_id())

    sdf = sdf.withColumn("_tmp_index", f.row_number().over(Window.partitionBy('SPARK_PARTITION_ID()') \
           .orderBy('monotonically_increasing_id()')))

    sdf_max = sdf.groupBy('SPARK_PARTITION_ID()').agg(f.max('_tmp_index')).orderBy('SPARK_PARTITION_ID()')

    windowval = (Window.orderBy('SPARK_PARTITION_ID()').rangeBetween(Window.unboundedPreceding, 0))

    df_w_cumsum = sdf_max.withColumn('cum_sum', f.sum('max(_tmp_index)').over(windowval)) \
                       .withColumn('SPARK_PARTITION_ID()', f.col('SPARK_PARTITION_ID()') + f.lit(1))

    sdf0 = spark.createDataFrame(
    data=[['0', 0, 0]],
    schema=['SPARK_PARTITION_ID()', 'max(_tmp_index)', 'cum_sum']
    )

    sdf_final = df_w_cumsum.union(sdf0)

    sdf = sdf.join(other=sdf_final, on='SPARK_PARTITION_ID()', how='inner') \
           .withColumn(idx_name, (f.col('cum_sum') + f.col('_tmp_index') + start).cast(IntegerType()))

    sdf = sdf.drop('SPARK_PARTITION_ID()', 'monotonically_increasing_id()', '_tmp_index', 'max(_tmp_index)', 'cum_sum')
    
    return sdf

# COMMAND ----------

def merge(sdf_src, tb_name, tb_keys, idx_name=''):

    dtb_tgt = DeltaTable.forName(spark, tb_name)
    
    if idx_name:
    
        sdf_tgt = dtb_tgt.toDF()

        sdf_change =  sdf_src.select(tb_keys).subtract(sdf_tgt.select(tb_keys))

        sdf_tgt_idx = sdf_tgt.select(*tb_keys, idx_name)

        if not sdf_change.rdd.isEmpty():

            max_id = sdf_tgt.agg(f.max(f.col('calendar_id')).alias('m')).collect()[0].m

            sdf_src_idx = index(idx_name, sdf_change, max_id).select(*tb_keys, idx_name)

            sdf_tgt_idx = sdf_tgt_idx.union(sdf_src_idx)

        sdf_final = sdf_src.join(sdf_tgt_idx, tb_keys)

        (dtb_tgt.alias('tgt')
          .merge(sdf_final.alias('src'), f'tgt.{idx_name} = src.{idx_name}')
          .whenMatchedUpdateAll()
          .whenNotMatchedInsertAll()
          .execute()
        )
        
    else:
        
        str_key = " AND ".join(f'tgt.{i} = src.{i}' for i in tb_keys)
        
        (dtb_tgt.alias('tgt')
          .merge(sdf_src.alias('src'), str_key)
          .whenMatchedUpdateAll()
          .whenNotMatchedInsertAll()
          .execute()
        )

# COMMAND ----------

def merge_with_model(sdf, layer, tb_name, tb_keys, idx_name):
    
    if spark.catalog.tableExists(tb_name):

        merge(
              idx_name= idx_name
            , tb_name= tb_name
            , tb_keys= tb_keys
            , sdf_src= sdf
        )

    else:

        sdf_final = index(
             idx_name= idx_name
            ,sdf = sdf
        )

        sdf_final_m = add_not_matched(sdf_final, idx_name)

        (
        sdf_final_m.write.format("delta")
          .mode('overwrite')
          .option('delta.enableChangeDataFeed', 'true')
          .option('delta.logRetentionDuration', f'interval 7 days')
          .option('delta.autoOptimize.autoCompact', 'true')
          .option('path', f'/mnt/{layer}/{tb_name}')
          .option("overwriteSchema", "true")
          .saveAsTable(f'{layer}.{tb_name}')
        )
