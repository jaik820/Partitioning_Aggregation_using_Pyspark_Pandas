from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import boto3
import json
import os

col_path = '/home/ubuntu/collections_output.jsonl'
pred_path = '/home/ubuntu/predictions_output.jsonl'
col_df = pd.read_json(col_path, lines=True)
pred_df = pd.read_json(pred_path, lines=True)

spark = SparkSession.builder \
    .appName("Pandas to Spark SQL") \
    .getOrCreate()


# Convert pandas DataFrame to Spark DataFrame
df_col_spark1 = spark.createDataFrame(col_df)
df_pred_spark = spark.createDataFrame(pred_df)


df_col_spark = df_col_spark1.withColumnRenamed("collection_id", "col_collection_id") \
       .withColumnRenamed("session_id", "col_session_id") \
       .withColumnRenamed("profile_id", "col_profile_id")\
       .withColumnRenamed("behaviour_type", "col_behaviour_type")\
       .withColumnRenamed("timestamp", "col_behaviour_timestamp")\
           .withColumnRenamed("x", "col_x")\
       .withColumnRenamed("y", "col_y")\
       .withColumnRenamed("key_hash", "col_key_hash")

#print(df_col_spark.show(4))

# Assuming collections and predictions are already loaded DataFrames
# Join the collections and predictions dataset on 'profile_id' and 'session_id'

joined_df = df_col_spark.join(
    df_pred_spark,
    (df_col_spark["col_profile_id"] == df_pred_spark["profile_id"]) &
    (df_col_spark["col_session_id"] == df_pred_spark["session_id"]),
    how="left"
)

#print(joined_df.show(5))

# 1. Number of collections per session

number_collections_df = joined_df.groupBy("col_session_id", "col_profile_id").agg(
    F.countDistinct("col_collection_id").alias("number_collection_ids")
)

#print(number_collections_df.show(5))


# 2. Weighted mean model score per session


weighted_mean_score_df = joined_df.groupBy("col_session_id", "col_profile_id").agg(
    (F.sum(F.col("score") * F.col("confidence")) / F.sum("confidence")).alias("weighted_mean_model_score")
)

#print(weighted_mean_score_df.show(5))

# 3. Mean swipe score

mean_swipe_score_df = joined_df.filter(F.col("model_name") == "swipe_model") \
    .groupBy("col_session_id", "col_profile_id") \
    .agg(F.avg("score").alias("mean_swipe_score"))



#print(mean_swipe_score_df.show(5))

# 4. Failed keystroke predictions (where model is keystroke_model and score is NULL)

failed_keystroke_df = joined_df.filter(
    (F.col("model_name") == "keystrokes_model") & (F.col("score").isNull())
).groupBy("col_session_id", "col_profile_id").agg(
    F.count("col_collection_id").alias("number_failed_keystroke_predictions")
)


#print(failed_keystroke_df.show(5))

# 5. Join all metrics together

final_df = number_collections_df \
    .join(weighted_mean_score_df, ["col_session_id", "col_profile_id"], how="left") \
    .join(mean_swipe_score_df, ["col_session_id", "col_profile_id"], how="left") \
    .join(failed_keystroke_df, ["col_session_id", "col_profile_id"], how="left")

# Show the final aggregated DataFrame
final_df.show()

#final_df.write.json("/home/ubuntu/final_output.json")

final_df.write.mode("overwrite").json("/home/ubuntu/aggregated")

#Writing spark dataframe to s3 folder:wq

json_data = "\n".join([json.dumps(row.asDict()) for row in final_df.collect()])


s3_client = boto3.client("s3")
bucket_name = "callsign-output"
s3_folder = "aggregated/aggregated-json/aggregated_output.json"

# Upload JSON data to S3

s3_client.put_object(Bucket=bucket_name, Key=s3_folder, Body=json_data)

print("Data successfully uploaded to S3!")


# Upload Parquet file to S3

# Save as Parquet to a local folder
output_dir = "/tmp/parquet_output"
final_df.write.mode("overwrite").parquet(output_dir)

bucket_name = "callsign-output"
s3_folder = "aggregated/aggregated-parquet"  # no leading slash
local_folder = "/tmp/parquet_output"

s3 = boto3.client("s3")

# Recursively upload all files
for root, _, files in os.walk(local_folder):
    for file in files:
        local_path = os.path.join(root, file)
        relative_path = os.path.relpath(local_path, local_folder)
        s3_key = f"{s3_folder}/{relative_path}".replace("\\", "/")  # Windows fix

        print(f"Uploading {local_path} to s3://{bucket_name}/{s3_key}")
        s3.upload_file(local_path, bucket_name, s3_key)


print("Parquet file successfully uploaded to S3!")

