# ETL Pipeline with PySpark for Collections and Predictions Data

This project implements an ETL (Extract, Transform, Load) pipeline using Python and Apache Spark to process user interaction data (`collections.jsonl`) and machine learning prediction data (`predictions.jsonl`).

---

## Objective

To process and transform raw JSONL files stored in an S3 bucket, and output both partitioned data and aggregated metrics into a designated S3 output location.

---

## Input Data

Two JSON Lines (`.jsonl`) files stored in the **input S3 bucket**:

- `collections.jsonl`: User interaction logs (e.g., clicks, keystrokes)
- `predictions.jsonl`: Predictions from machine learning models for each collection

---

## Output

The pipeline generates and stores the following in the **output S3 bucket**:

1. **Partitioned Data** by `profile_id`:
   - `/output/labels/profile-id=<profile_id>/collections.jsonl`
   - `/output/predictions/profile-id=<profile_id>/predictions.jsonl`

2. **Aggregated Results**:
   - `/output/aggregated-result/data.parquet`

---

## Requirements

- Python 3.8+
- Apache Spark 3.x
- Hadoop AWS JARs (for S3 access)
- AWS credentials (via IAM role, environment vars, or `~/.aws/credentials`)
- boto3 (optional, for manual upload if not using Spark S3 connector)

---

## 🛠 Setup


Step -1: Launch an EC2 Instance via AWS Console, CLI or Terraform (Infrastructure as a service)   
###############################################################################################

Please follow the below Steps:

### 1- Login to AWS and launch an instance and select ubuntu server along with Instance Type

### 2- Make sure the security group type has all traffic allowed to inbound and outbound that you need

### 3- Create a Private Key file (.pem)

### 4- Change the permission of Private Key
```console
chmod 400 <pem file name>.pem
```
### 5 Connect using SSH

ssh -i <pem file name> ubuntu@public-dns-name

### 6- Update the packages
```console
sudo apt-get update
sudo apt-get upgrade
```
### 7- Installing Pip & JAVA 
#### Download pip
```console
sudo apt install python3-pip
```
#### Check pip version
```console
pip3 --version
```
#### Download Java
```console
pip3 install py4j
sudo apt-get install openjdk-8-jdk
``` 

### 8- Installing Jupyter Notebook
```console
sudo apt install jupyter-notebook
```

### 9- Configuring Jupyter Notebook settings
```console
jupyter notebook --generate-config
```
#### Jupyter Config File
```console
cd .jupyter
nano jupyter_notebook_config.py
```

#### Increase the "#c.NotebookApp.iopub_data_rate_limit" value to "100000000" and remove the # from the front.
```console
c.NotebookApp.iopub_data_rate_limit = 100000000
```
### 10- Installing Spark
#### Make sure you have Java 8 or higher installed on your computer and then, visit the Spark downloads page (https://spark.apache.org/downloads.html).
 
```console
pip3 install findspark
wget https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz 
```

#### Unzip it and move it to your /opt folder and create a symbolic link
```console
tar xvf spark-*
sudo mv spark-3.0.1-bin-hadoop2.7 /opt/spark-3.0.1
sudo ln -s /opt/spark-3.0.1 /opt/spark
```

#### Configure Spark & Java & PySpark driver to use Jupyter Notebook
```console
nano ~/.bashrc
```
##### -Write into:
```console
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
export JAVA_HOME='/usr/lib/jvm/java-8-openjdk-amd64'
export PATH=$JAVA_HOME/bin:$PATH
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip 0.0.0.0 --port=8888'
```
#### After saving path, you need to run
```console
source ~/.bashrc
```
 
### 11- Connecting the Jupyter Notebook from your Web-Browser 
```console
pyspark
```

### 12- Jupyter Notebook Web UI
```console
https://<your public dns>:8888/
```
##### Paste the copied token and create a new password if you want

#### To stop jupyter notebook services
```console
jupyter notebook stop 8888
```

Step - 2: Install Required Dependencies
##########################################

pip install pyspark boto3 pandas

#Download required Hadoop JARs:

mkdir -p ~/spark_jars
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar -P ~/spark_jars
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar -P ~/spark_jars

Step - 3: Create a Python script with Pandas, Boto3 and Json Libraries in the EC2 Instance which does the following

Script name = callsign_python_step_1.py

### 1 - Load the Environment file with the required variables and paths

### 2 - Enable Logging for tracking any issues 

### 3 - Download the collections.jsonl and predictions.jsonl files from s3-callsign s3 input and convert it into proper json format by handling inconsistent metadata.

### 4  **Partitioned Data** by `profile_id`: and save it in the callsign_output s3 bucket
   - `/output/labels/profile-id=<profile_id>/collections.jsonl`
   - `/output/predictions/profile-id=<profile_id>/predictions.jsonl`

Step - 4: Create a Pyspark script in the EC2 Instance which is dependent on the above Python Script

Script name = callsign_pyspark_step_2.py

### 1 - Create two spark dataframes - collections and predictions

### 2 - Implement the necessary aggregations mentioned on the Instructions:

# 1. Number of collections per session
# 2. Weighted mean model score per session
# 3. Mean swipe score
# 4. Failed keystroke predictions (where model is keystroke_model and score is NULL)

Step - 5: Write the above spark dataframes in 2 different formats in the output S3 bucket - callsign_output (Json for validation in human readable format) and (parquet as per the instructions)

aggregated-json
aggregated-parquet

Step - 6: Execute the following unit testing script

Script name = callsign_unit_testing_step_3.py

Test Case:1 - Missing ID's or Blank ID's Validation
+----------------------+---------------------+
|Collections_Profile_ID|Missing_ID_Validation|
+----------------------+---------------------+
|  Collections_Profi...|                    0|
|  Predictions_Profi...|                    0|
|  Collections_Sessi...|                    0|
|  Predictions_Sessi...|                    0|
|  Collections_Colle...|                    0|
|  Predictions_Colle...|                    0|
+----------------------+---------------------+

None
Test Case:2 - Duplicate ID's Validation
+----------------------+----------+--------------+
|Collections_Session_ID|session_id|profiles_count|
+----------------------+----------+--------------+
+----------------------+----------+--------------+

None
Test Case:3 - Missing Session IDs Validation
+----------------------+-------------------+
|Missing_in_Predictions|Missing_Session_IDs|
+----------------------+-------------------+
|  Missing_in_Predic...|                  4|
|  Missing_in_Collec...|                  1|
+----------------------+-------------------+

None
Test Case:4 - Missing Behaviour Types and Model Names Validation
+----------------------------------+--------+
|Collections_Missing_Behaviour_Type|count(1)|
+----------------------------------+--------+
|              Collections_Missi...|       0|
|              Predictions_Missi...|       0|
+----------------------------------+--------+


 
