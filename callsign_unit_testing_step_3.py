import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd


# ---- CONFIG ----
col_path = '/home/ubuntu/collections_output.jsonl'
pred_path = '/home/ubuntu/predictions_output.jsonl'

# ---- INIT SPARK ----
spark = SparkSession.builder \
    .appName("Unit Test Cases") \
    .getOrCreate()

# ---- READ JSONL FILES ----
predictions_df = spark.read.json(pred_path)
collections_df = spark.read.json(col_path)

predictions_df.createOrReplaceTempView("predictions")
collections_df.createOrReplaceTempView("collections")

test1 = spark.sql("""
                  SELECT 'Collections_Profile_ID', count(1) as Missing_ID_Validation  from collections where profile_id is null
                      union all
                      SELECT 'Predictions_Profile_ID', count(1) as Null_Profile_IDs  from predictions where profile_id is null
                          union all
                    SELECT 'Collections_Session_ID', count(1) as Null_Session_IDs  from collections where session_id is null
                    union all
                    SELECT 'Predictions_Session_ID', count(1) as Null_Session_IDs  from predictions where session_id is null
                    union all
                    SELECT 'Collections_Collection_ID', count(1) as Null_Collection_IDs  from collections where collection_id is null
                    union all
                    SELECT 'Predictions_Collection_ID', count(1) as Null_Collection_IDs  from predictions where collection_id is null    
""")

print("Test Case:1 - Missing ID's or Blank ID's Validation")

print(test1.show())


test2 = spark.sql("""
      SELECT 'Collections_Session_ID', session_id, count(profile_id) as profiles_count from 
					(select distinct session_id, profile_id from collections) group by session_id having 
					profiles_count>1
                    union all
                    SELECT 'Predictions_Session_ID', session_id, count(profile_id) as profiles_count from 
					(select distinct session_id, profile_id from predictions) group by session_id having 
					profiles_count>1
					
""")

print("Test Case:2 - Duplicate ID's Validation")
print(test2.show())



test3 = spark.sql("""
					select 'Missing_in_Predictions', count(session_id) as Missing_Session_IDs from collections where session_id not in (select session_id from predictions)
					union all
					select 'Missing_in_Collections', count(session_id) from predictions where session_id not in (select session_id from collections)
""")
print("Test Case:3 - Missing Session IDs Validation")
print(test3.show())

test4 = spark.sql("""
                  	select 'Collections_Missing_Behaviour_Type', count(1) from collections where behaviour_type is null
					union all
					select 'Predictions_Missing_Model_Names', count(1) from predictions where model_name is null
""")
print("Test Case:4 - Missing Behaviour Types and Model Names Validation")
print(test4.show())



