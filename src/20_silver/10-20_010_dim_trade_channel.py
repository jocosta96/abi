# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../generic/merge_with_index

# COMMAND ----------

sdf_10 = spark.read.table('10_bronze.beverage_channel_group')

# COMMAND ----------

sdf_10 = sdf_10.select(
    
      F.trim('TRADE_CHNL_DESC').alias('trade_channel_desc')
    , F.trim('TRADE_GROUP_DESC').alias('trade_group_desc')
    , F.trim('TRADE_TYPE_DESC').alias('trade_type_desc')
    
)

# COMMAND ----------

merge_with_model(
    
      sdf=sdf_10
    , layer='20_silver'
    , tb_name='dim_trade_channel'
    , tb_keys=['trade_channel_desc']
    , idx_name='trade_channel_id'
    
)
