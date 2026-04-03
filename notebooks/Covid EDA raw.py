# Databricks notebook source
!wget -q https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv -O /tmp/covid-hospitalizations.csv

# COMMAND ----------

import pandas as pd

# read from /tmp, subset for USA, pivot and fill missing values
df = pd.read_csv("/tmp/covid-hospitalizations.csv")
df = df[df.iso_code == 'USA']\
    .pivot_table(values='value', columns='indicator', index='date')\
    .fillna(0)

display(df)

# COMMAND ----------

df.plot(figsize=(13,6), grid=True).legend(loc='upper left')


# COMMAND ----------

clean_cols = [c.replace(' ', '_') for c in df.columns]

# Reset index so 'date' becomes a regular column
df_reset = df.reset_index()
df_reset.columns = ['date'] + clean_cols

# Write to Delta table, overwrite with latest data each time
sdf = spark.createDataFrame(df_reset)
sdf.write.mode('overwrite').saveAsTable('dev_covid_analysis')
