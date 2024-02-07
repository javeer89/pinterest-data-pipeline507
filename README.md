# Pinterest Data Pipeline
*Pinterest Data Pipeline project using Databricks, Spark, Airflow, Kinesis, Kafka, API Gateway*

Pinterest crunches billions of data points every day to decide how to provide more value to their users.\
In this project, we'll create a similar system using the AWS Cloud.


## Project Outline
Build the system that Pinterest uses to analyse both historical, and real-time data generated by posts from their users.


**The pipeline will have multiple stages and connectors.**\

- Set up an EC2 instance
- Configure a Apache kafka repository and Confluent Kafka connect
- Connect API to S3 database
- Activate Apache Kafka REST proxy
- Ingest data via user_posting_emulation.py
- Mount S3 database to DataBricks Notebook
- Data Transformations - see [databricks](databricks)
- Setup Apache Airflow and DAG file for Kinesis operations
- Connect API to AWS Kinesis
- Ingest data via user_posting_emulation_streaming.py
- Retreive data on DataBricks
- Transform data (using previously constructed methods)
- Write data into Delta Tables


## Installation Instructions
Large parts of this project are undertaken on the AWS infrastructure and DataBricks. That said, there are some notable extensions that are required to operate our scripts.

- python 3.11.5
- pandas 2.1.2
- pyspark 3.5.0
- boto 2.49.0
- requests2 2.16.0
- SQLAlchemy 2.0.25
- apache-airflow 2.8.1
- kafka-python 2.0.2
- Confluent Kafka Connect



## Usage instructions
Setup pre-requisite systems and run emulation scripts to ingest data.



## File structure of the project
user_post_emulation.py scripts for both S3 ingest and Kinesis Streaming can be found in the [scripts](scripts/) folder.
The DAG file for Apache Airflow can also be found in the [scripts](scripts/) folder.

You can use the env.yaml file for a quick install of all environment installs used.



## License information
MIT LICENCE

