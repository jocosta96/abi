# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../generic/merge_with_index

# COMMAND ----------

sdf_10 = spark.read.table('10_bronze.beverage_sales')

# COMMAND ----------

sdf_bo = sdf_10.select(
    
    F.col(
        'Btlr_Org_LVL_C_Desc'
    ).alias(
        'bottler_origin_name', metadata={'comment': 'Btlr_Org_LVL_C_Desc'}
    )
    
).distinct()

# COMMAND ----------

merge_with_model(
      sdf=sdf_bo
    , layer='20_silver'
    , tb_name='dim_bottler_origin'
    , tb_keys=['bottler_origin_name']
    , idx_name='bottler_origin_id'
)
