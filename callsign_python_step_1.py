import json
import pandas as pd
import logging
from dotenv import load_dotenv
import os
import boto3

load_dotenv()

temp_input_col = os.getenv("temp_input_col")
output_file_col = os.getenv("output_file_col")
temp_input_pred = os.getenv("temp_input_pred")
output_file_pred = os.getenv("output_file_pred")
bucket_name = os.getenv("bucket_name")
output_bucket_name = os.getenv("output_bucket_name")
folder_path_col= os.getenv("folder_path_col")
local_filename_col = os.getenv("local_filename_col")
folder_path_pred= os.getenv("folder_path_pred")
local_filename_pred = os.getenv("local_filename_pred")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log")     # Logs to file
    ]
)

# Initialize S3 client
s3 = boto3.client("s3")

#download collections file from s3 
s3.download_file(bucket_name, folder_path_col, local_filename_col)

#download predictions file from s3 
s3.download_file(bucket_name, folder_path_pred, local_filename_pred)

####### Collections #############

def collections(input_path):
    rows = []

    if not input_path:
        logging.error(f"Input file not found: {input_path}")
    
    logging.info(f"Reading file: {input_path}")


    with open(input_path, 'r') as f:
        for line in f:
            if not line.strip():
                logging.debug(f"Skipping empty lines")
                continue  # Skip empty lines
            try:    
                data = json.loads(line)
                metadata = data.get('metadata', {})
                metadata['col_timestamp'] = metadata.pop('timestamp')
                behaviours = data.get('behaviour', {}).get('mobile', {})

####Looping through the types inside behaviour

                for key, value in behaviours.items():
                    for entry in value:
                        row = {
                            **metadata,
                            'behaviour_type': key,
                            **entry
                        }
                        rows.append(row)
            except json.JSONDecodeError as e:
                logging.warning(f"Invalid JSON: {e}")

    df = pd.DataFrame(rows)
    df.to_json(output_file_col, orient="records", lines=True)
    logging.info(f"Output written to {output_file_col}")


####### Predictions #############

def predictions(input_path_pred):
    rows_pred = []

    if not input_path_pred:
        logging.error(f"Input file not found: {input_path_pred}")
    
    logging.info(f"Reading file: {input_path_pred}")


    with open(input_path_pred, 'r') as f:
        for line in f:
            if not line.strip():
                logging.debug(f"Skipping empty lines")
                continue  # Skip empty lines
            try:    
                data = json.loads(line)
                metadata = data.get('metadata', {})
                predictions = data.get('prediction', {})
                merged_dict = {
                    **metadata,
                    **predictions
                }
                row = merged_dict
                rows_pred.append(row)

            except json.JSONDecodeError as e:
                logging.warning(f"Invalid JSON: {e}")

    df = pd.DataFrame(rows_pred)
    df.to_json(output_file_pred, orient="records", lines=True)
    logging.info(f"Output written to {output_file_pred}")

collections(temp_input_col)
predictions(temp_input_pred)

##########################

collections_df = pd.read_json(output_file_col, lines=True)
predictions_df = pd.read_json(output_file_pred, lines=True)

# Partition data by profile_id
profile_ids = set(predictions_df["profile_id"]).union(set(collections_df["profile_id"]))

for profile_id in profile_ids:
    pred_partition = predictions_df[predictions_df["profile_id"] == profile_id].to_json(orient="records", lines=True)
    coll_partition = collections_df[collections_df["profile_id"] == profile_id].to_json(orient="records", lines=True)

    # Store in S3

    s3.put_object(Bucket=output_bucket_name, Key=f"profile-id={profile_id}/predictions.jsonl", Body=pred_partition)
    s3.put_object(Bucket=output_bucket_name, Key=f"profile-id={profile_id}/collections.jsonl", Body=coll_partition)


