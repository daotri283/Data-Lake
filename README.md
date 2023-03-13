# Introduction
The goal of this project was to create an ETL pipeline for I94 immigration, global land temperatures and US demographics datasets to form an analytics database on immigration events. A use case for this analytics database is to find immigration patterns to the US. For example, we could try to find answears to questions such as, is temperature a factor when it comes to immigratng for people born outside of the US?

# Project Description
In this project, we will build an ETL pipeline for a data lake hosted on S3. We will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. We will then deploy this Spark process on a cluster using AWS.

# How to run the project 
Step 1: Set environments variables in dl.cfg file
Step 2: Run python etl.py 