# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../generic/merge_with_index

# COMMAND ----------

sdf_10 = spark.read.table('10_bronze.beverage_sales')

# COMMAND ----------

sdf_brand = sdf_10.select(
    
      F.col('BRAND_NM').alias('brand_name')
    , F.col('CE_BRAND_FLVR').alias('brand_code')
    
).distinct()

# COMMAND ----------

merge_with_model(
      sdf=sdf_brand
    , layer='20_silver'
    , tb_name='dim_brand'
    , tb_keys=['brand_code']
    , idx_name='brand_id'
)

# COMMAND ----------

spark.read.table('10_bronze.beverage_sales').show()

# COMMAND ----------


