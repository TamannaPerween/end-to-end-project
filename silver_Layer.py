# Databricks notebook source


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##reading data

# COMMAND ----------

df_cal=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/Calender/")
display(df_cal)

# COMMAND ----------

df_cus=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/Customers/")
display(df_cus)

# COMMAND ----------

df_Pro_Cat=spark.read.format("csv").option("header",True).option("InferSchema",True).load("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/Product_Categories/")
display(df_Pro_Cat)

# COMMAND ----------

df_Pro_Sub_Cat=spark.read.format("csv").option("header",True).option("InferSchema",True).load("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/Product_Subcategories/")
display(df_Pro_Sub_Cat)

# COMMAND ----------

df_Pro=spark.read.format("csv").option("header",True).option("InferSchema",True).load("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/Products/")
display(df_Pro)

# COMMAND ----------

df_Ret=spark.read.format("csv").option("header",True).option("InferSchema",True).load("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/Returns/")
display(df_Ret)

# COMMAND ----------

df_Sales_2016=spark.read.format("csv").option("header",True).option("InferSchema",True).load("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/Sales_2016/")
display(df_Sales_2016)

# COMMAND ----------

df_Sales_2015=spark.read.format("csv").option("header",True).option("InferSchema",True).load("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/Sales_2015/")
display(df_Sales_2015)

# COMMAND ----------

df_Sales_2017=spark.read.format("csv").option("header",True).option("InferSchema",True).load("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/Sales_2017/")
display(df_Sales_2017)

# COMMAND ----------

df_Terri=spark.read.format("csv").option("header",True).option("InferSchema",True).load("abfss://bronze@tg00ginats24tamperdadls.dfs.core.windows.net/Territories/")
display(df_Terri)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation

# COMMAND ----------

df_cal=df_cal.withColumn("Month",month(col("Date")))\
    .withColumn('Year',year(col('Date')))
    
display(df_cal)


# COMMAND ----------

df_cal.write.format('delta').mode("append").save("abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/Calender/")

# COMMAND ----------

# MAGIC %md
# MAGIC transformation on customer data
# MAGIC

# COMMAND ----------

display(df_cus)

# COMMAND ----------

df_cus.show()

# COMMAND ----------

df_cus = df_cus.withColumn('fullname',concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))
display(df_cus)

# COMMAND ----------

df_cus.write.format('delta').mode('append').save('abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/Customrs/')

# COMMAND ----------

df_Pro_Sub_Cat.write.format('delta').mode('append').save('abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/Product_Subcategories/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## # Transformation on Product DAta

# COMMAND ----------

display(df_Pro)

# COMMAND ----------

df_Pro = df_Pro.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
                .withColumn('ProductName',split(col('ProductName'),'-')[0])
display(df_Pro)

# COMMAND ----------

df_Pro.write.format("delta").mode('append').save('abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/Products/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Returns

# COMMAND ----------

df_Ret.write.format("delta").mode('append').save('abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/Returns/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Territories

# COMMAND ----------

df_Terri.write.format("delta").mode('append').save('abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/Territories/')

# COMMAND ----------

# MAGIC %md
# MAGIC sales
# MAGIC ## 

# COMMAND ----------

display(df_Sales_2015)

# COMMAND ----------

df_Sales_2015= df_Sales_2015.withColumn('StockDate',to_timestamp('StockDate'))\
                            .withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))\
                            .withColumn('Multiply',col('OrderLineItem')*col('OrderQuantity'))
display(df_Sales_2015)

# COMMAND ----------

# MAGIC %md
# MAGIC ## sales Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC how many order receive everyday

# COMMAND ----------

df1=df_Sales_2015.groupBy('OrderDate').agg(count('OrderNumber').alias('Total_Order'))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC analysis

# COMMAND ----------

display(df_Pro_Cat)

# COMMAND ----------

# MAGIC %md
# MAGIC analysis

# COMMAND ----------

display(df_Terri)

# COMMAND ----------

df_Sales_2015.write.format("delta").mode('append').save('abfss://silver@tg00ginats24tamperdadls.dfs.core.windows.net/Sales_2015/')