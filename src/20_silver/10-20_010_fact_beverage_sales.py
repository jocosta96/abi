# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../generic/merge_with_index

# COMMAND ----------

sdf_10 = spark.read.table('10_bronze.beverage_sales').alias('sa')

sdf_calendar = spark.read.table('20_silver.dim_calendar').alias('ca')
sdf_bo = spark.read.table('20_silver.dim_bottler_origin').alias('bo')
sdf_brand = spark.read.table('20_silver.dim_brand').alias('bd')
sdf_package = spark.read.table('20_silver.dim_package').alias('pk')
sdf_tc = spark.read.table('20_silver.dim_trade_channel').alias('tc')
sdf_cg = spark.read.table('20_silver.dim_channel_group').alias('cg')

# COMMAND ----------

list_id_cols = [
      'calendar_id'
    , 'montly_calendar_id'
    , 'bottler_origin_id'
    , 'brand_id'
    , 'package_id'
    , 'trade_channel_id'
    , 'channel_group_id'
]

# COMMAND ----------

sdf_join = sdf_10.join(

     other= F.broadcast(sdf_calendar)
    ,on= F.date_format(F.to_date(F.col('sa.DATE'), 'M/d/yyyy'), 'yyyyMMdd') == F.col('ca.calendar_id')
    ,how= 'left'

).join(
    
     other= F.broadcast(sdf_bo)
    ,on= F.trim(F.col('sa.Btlr_Org_LVL_C_Desc')) == F.col('bo.bottler_origin_name')
    ,how= 'left'
    
).join(
    
     other= F.broadcast(sdf_brand)
    ,on= F.trim(F.col('CE_BRAND_FLVR')) == F.col('bd.brand_code')
    ,how= 'left'
    
).join(

    other= F.broadcast(sdf_package)
    ,on = F.trim(F.col('sa.TSR_PCKG_NM')) == F.col('pk.package_code')
    ,how= 'left'

).join(

    other= F.broadcast(sdf_tc)
    ,on = F.trim(F.col('sa.TRADE_CHNL_DESC')) == F.col('tc.trade_channel_desc')
    ,how= 'left'

).join(

    other= F.broadcast(sdf_cg)
    ,on = F.trim(F.col('sa.CHNL_GROUP')) == F.col('cg.channel_group_desc')
    ,how= 'left'

)

# COMMAND ----------

sdf_sales = sdf_join.select(
    
    *list_id_cols
    , F.col('Volume').cast('float').alias('volume')

)

# COMMAND ----------

if spark.catalog.tableExists('20_silver.fact_beverage_sales'):

    merge(sdf_sales, '20_silver.fact_beverage_sales', list_id_cols)
    
else:
    
    (
        sdf_sales.write.format("delta")
          .mode('overwrite')
          .option('delta.logRetentionDuration', f'interval 7 days')
          .option('delta.autoOptimize.autoCompact', 'true')
          .option('path', '/mnt/20_silver/fact_beverage_sales')
          .option('comment', f'fact_beverage_sales')
          .saveAsTable(f'20_silver.fact_beverage_sales')
    )
