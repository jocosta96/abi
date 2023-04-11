# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

def add_not_matched(sdf, id_col=str):
    
    sdf0 = spark.createDataFrame(
    data=[[-1] + ['' for col in range(len(sdf.columns) -1)]],
    schema=[id_col] + [col for col in sdf.columns if col != id_col]
    ).select(
        *[F.col(x).cast(y) for x, y in dict(sdf.dtypes).items()]
    )
    
    return sdf.unionByName(sdf0)
