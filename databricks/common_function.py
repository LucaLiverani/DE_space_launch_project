# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get_last_updated_file_from_storge_account

# COMMAND ----------

def get_last_updated_file(file_type):
    
    try:
        
        storage_account = "lake00001"
        container_name = "container00001"
        scope = "adlsGen2ClientSecret"
        storage_account_key = "StorageAccountKey"

        spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        dbutils.secrets.get(scope=scope, key=storage_account_key))

        files = dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/")
        files.reverse()

        for f in files:
            file_name = f.name
            if file_type in file_name:
                path = f.path 
                return path
        
    except Exception as e:
        raise Exception (e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge function

# COMMAND ----------

def merge(target_table, upd_table, merge_key):
    
    try:   
        deltatabletrg=DeltaTable.forName(spark,target_table)
        df_upt=spark.sql("select * from {0}".format(upd_table))
        mrg_condition="trg.{0}=upt.{0}".format(merge_key)
        
        deltatabletrg.alias("trg")\
                .merge(df_upt.alias("upt"),
                mrg_condition)\
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll()\
                .execute()
            
        
    except Exception as e:
        raise Exception (e)


# COMMAND ----------

#df.write.saveAsTable("<table-name>")
