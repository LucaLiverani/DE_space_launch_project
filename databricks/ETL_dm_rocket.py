# Databricks notebook source
# MAGIC %run /Users/l.liverani@reply.it/common_function

# COMMAND ----------

import pandas as pd
import json

staging_launch_df = spark.sql("select rocket.configuration from default.staging_launch")
display(staging_launch_df)

# COMMAND ----------

columns = ['configuration.id', 'configuration.full_name', 'configuration.family', 'configuration.description','configuration.diameter','configuration.length', 'configuration.to_thrust','configuration.leo_capacity','configuration.gto_capacity','configuration.launch_mass','configuration.reusable','configuration.launch_cost','configuration.apogee', 'configuration.attempted_landings', 'configuration.consecutive_successful_landings','configuration.consecutive_successful_launches', 'configuration.failed_landings', 'configuration.failed_launches', 'configuration.successful_landings', 'configuration.successful_launches','configuration.total_launch_count','configuration.pending_launches']



rocket_df = staging_launch_df.select('configuration.id', 'configuration.full_name', 'configuration.family', 'configuration.description','configuration.diameter','configuration.length', 'configuration.to_thrust','configuration.leo_capacity','configuration.gto_capacity','configuration.launch_mass','configuration.reusable','configuration.launch_cost','configuration.apogee', 'configuration.attempted_landings', 'configuration.consecutive_successful_landings','configuration.consecutive_successful_launches', 'configuration.failed_landings', 'configuration.failed_launches', 'configuration.successful_landings', 'configuration.successful_launches','configuration.total_launch_count','configuration.pending_launches',  staging_launch_df.configuration.manufacturer.id.alias("manufacturer_id")).dropDuplicates()



display(rocket_df)

#spark.sql("drop table if exists default.dm_rocket")
#rocket_df.write.saveAsTable("default.dm_rocket")

# COMMAND ----------

table_name = "dm_rocket"

rocket_df.createOrReplaceTempView(f"{table_name}_update")

target_table = f"default.{table_name}"
upd_table = f"{table_name}_update"
merge_key = "id"

merge(target_table, upd_table, merge_key)
