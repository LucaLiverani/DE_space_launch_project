# Databricks notebook source
# MAGIC %run /Users/l.liverani@reply.it/common_function

# COMMAND ----------

import json

file_path = get_last_updated_file("latest_launch")
print(file_path)

launch_df = (spark.read
  .format("parquet")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(file_path)
)

launch_df = launch_df.dropDuplicates()
display(launch_df)

spark.sql("drop table if exists default.staging_launch")
launch_df.write.saveAsTable("default.staging_launch")

# COMMAND ----------

table_name = "staging_launch"

launch_df.createOrReplaceTempView(f"{table_name}_update")

target_table = f"default.{table_name}"
upd_table = f"{table_name}_update"
merge_key = "id"

merge(target_table, upd_table, merge_key)
