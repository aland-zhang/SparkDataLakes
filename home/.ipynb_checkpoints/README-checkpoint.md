# Sparkify Data Lake with AWS S3 and Spark

## Summary:
This project extract CSV files present in AWS S3, transforms the data using Apache Spark and Python, and finally outputs the data back into S3 in an encrypted Parquet file format.

## How to run the program:

In order to run the program, first open the dl.cfg file and enter appropriate Amazon credentials.

Once that is completed execute the following command in the command line:

python etl.py