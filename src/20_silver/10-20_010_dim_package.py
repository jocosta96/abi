# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../generic/merge_with_index

# COMMAND ----------

sdf_10 = spark.read.table('10_bronze.beverage_sales')

# COMMAND ----------

sdf_package = sdf_10.select(
    
     F.trim(F.col('PKG_CAT')).alias('package_category_code')
    ,F.trim(F.col('Pkg_Cat_Desc')).alias('package_desc')
    ,F.trim(F.col('TSR_PCKG_NM')).alias('package_code')
    
).distinct()

# COMMAND ----------

merge_with_model(
      sdf=sdf_package
    , layer='20_silver'
    , tb_name='dim_package'
    , tb_keys=['package_code']
    , idx_name='package_id'
)
