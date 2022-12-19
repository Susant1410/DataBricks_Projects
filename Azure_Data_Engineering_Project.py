# Databricks notebook source
dbutils.fs.mount(
       source = "wasbs://rajas-de@rajaadls.blob.core.windows.net",
       mount_point = "/mnt/adls_test2",
       extra_configs ={"fs.azure.account.key.rajaadls.blob.core.windows.net":"l+5U57ZhZVETYGEQNPTYdUqK6hN0T6npD9bDfIxgm1XlQt535gnbUpNErsrq4rm0NJ4fXC+WDPZh+ASt8BP55Q=="}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/adls_test2")

# COMMAND ----------

std_df = spark.read.format("csv").options(inferSchema="True",header = "True",sep=",").load("/mnt/adls_test2/Students_Data.csv")
display(std_df)

# COMMAND ----------

std_df.columns

# COMMAND ----------

df_drop_col = std_df.drop("Registration Code","Area Manager","Cluster Manager","Correspondence Land Line Number","Student Code","Mother Name","Guardian/ Father Name","Correspondence Email Id","What Brought me to NIIT Foundation")
display(df_drop_col)

# COMMAND ----------

df_drop_col.createOrReplaceTempView('table')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table

# COMMAND ----------


