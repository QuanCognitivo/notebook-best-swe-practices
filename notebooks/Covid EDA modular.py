# Databricks notebook source
data_path = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv'
print(f'Data path: {data_path}')

# COMMAND ----------

from covid_analysis.transforms import *
import pandas as pd

df = pd.read_csv(data_path)
df = filter_country(df, country='DZA')
df = pivot_and_clean(df, fillna=0)
df = clean_spark_cols(df)
df = index_to_col(df, colname='date')

# Convert from Pandas to a pyspark sql DataFrame
df = spark.createDataFrame(df)

display(df)

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('covid_stats')

# COMMAND ----------

display(spark.table('covid_stats'))

# COMMAND ----------

df.toPandas().plot(figsize=(13,6), grid=True).legend(loc='upper left')
