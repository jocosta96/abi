# Databricks notebook source
# MAGIC %md
# MAGIC [doc](https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts)

# COMMAND ----------

dbutils.fs.mount(
  source = f"wasbs://dls@{dbutils.secrets.get(scope = 'kv-abi-test', key = 'sa-name')}.blob.core.windows.net",
  mount_point = "/mnt",
  extra_configs = {
      f"fs.azure.account.key.{dbutils.secrets.get(scope = 'kv-abi-test', key = 'sa-name')}.blob.core.windows.net":
      dbutils.secrets.get(scope = 'kv-abi-test', key = 'sa-key')
  }
)

# COMMAND ----------

# MAGIC %fs ls /mnt

# COMMAND ----------


