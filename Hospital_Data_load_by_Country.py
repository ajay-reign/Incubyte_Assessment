# Databricks notebook source
#import Statements
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col,datediff,current_date,floor,when
import os
from datetime import datetime
from pathlib import Path

# COMMAND ----------

#schema and Variable declaration
schema = """
Empty String,
H String,
Customer_Name STRING,
Customer_ID STRING,
Customer_Open_Date STRING,
Last_Consulted_Date STRING,
Vaccination_Type STRING,
Doctor_Consulted STRING,
State STRING,
Country STRING,
Date_of_Birth STRING,
Active_Customer STRING
"""
formatted_date = datetime.now().strftime('%Y%m%d')
file_name=f"Hospital_data_{formatted_date}.csv"
file_path=f"dbfs:/FileStore/{file_name}"

# COMMAND ----------

def load_into_stageTable (file_path:any):
    raw_df = spark.read.option("header", True).schema(schema).csv(file_path,sep="|")
    #Fetching Only the Required Columns
    Stage_df=raw_df.select("Customer_ID","Customer_Name","Customer_Open_Date","Last_Consulted_Date","Vaccination_Type","Doctor_Consulted","State","Country","Date_of_Birth","Active_Customer")
    Stage_df.write.mode("overwrite").saveAsTable("hospital_data_stage")
    return True


# COMMAND ----------

def transformation():
    Transformation_df =spark.sql("SELECT  Customer_ID,Customer_Name,date_format(to_date(customer_Open_Date, 'yyyyMMdd'), 'yyyy-MM-dd') as customer_Open_Date,date_format(to_date(Last_Consulted_Date, 'yyyyMMdd'), 'yyyy-MM-dd') as Last_Consulted_Date,Vaccination_Type,Doctor_Consulted,State,Country,date_format(to_date(Date_of_Birth, 'ddMMyyyy'), 'yyyy-MM-dd') as Date_of_Birth,Active_Customer FROM hospital_data_stage ")

    #derived Columns
    Transformation_df=Transformation_df.withColumn("Age",floor(datediff(current_date(),col("Date_of_Birth"))/365)).withColumn("Consulted_lastMonth",when(datediff(current_date(),col("Last_Consulted_Date"))<30,"Y").otherwise("N")).withColumn("Rundate",current_date())

    #Delete Duplicate records if any and Fill null values with 'Unknown'
    Transformation_df=Transformation_df.dropDuplicates().fillna('Unknown')

    return Transformation_df

# COMMAND ----------

def loadintoDestination(Transformation_df:any):
    countries=Transformation_df.select("Country").distinct().rdd.flatMap(lambda x:x).collect()
    for Country in countries:
        country_df=Transformation_df.filter(Transformation_df.Country== Country).filter(Transformation_df.Active_Customer=="A")

        table_name=f"hospital_data_{Country}"

        country_df.write.mode("append").saveAsTable(table_name)

        print (f"Data for {Country} loaded into table: {table_name}")


# COMMAND ----------

#main function
if dbutils.fs.ls('dbfs:/FileStore'):
    file_exists=any(file.name==file_name for file in dbutils.fs.ls('dbfs:/FileStore'))
    if file_exists:
        result=load_into_stageTable(file_path)
        if result==True:
            trans_result=transformation()
            loadintoDestination(trans_result)
        else:
            print("Issue with the inbound File")
    else:
        print("Latest File Doesn't Exist")
