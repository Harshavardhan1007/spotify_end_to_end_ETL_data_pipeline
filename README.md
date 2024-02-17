# Spotify End-To-End ETL Data Pipeline Project

### Introduction
In this project, we will build as ETL (Extract, Transform, Load) pipeline using the spotify API on AWS. The pipeline will recieve data from the spotify API, transform it to a desired format, and load it into an AWS data store.

### Architecture
![Architecture Diagram](https://github.com/Harshavardhan1007/spotify_end_to_end_ETL_data_pipeline/blob/main/spotify_data_pipeline_Architecture.jpg)

### Project Execution Flow
Etract Data from API -> Lambda Trigger (every 1 day) -> Run Extract Code -> Store Raw Data -> Trigger Transform Function -> Transform Data and Load it -> Query Using Athena 

### About Dataset/API
This API contains information about music Artist, Album and Songs. [[Spotify API Docs](https://pages.github.com/](https://developer.spotify.com/documentation/web-api)).
 
### Services Used
1. **s3 (Simple Storage Service):** Amazon s3(Simple Storage Service) is a highly scalable object storage service that can store and retrieve any amount of data from anywhere on the web. It is commonly used to store and distribute large media files, data backups, and static website files.
2.  **AWS Lambda:** AWS Lambda is a serverless computing. Service that, lets you run your code without managing servers. You can use Lambda to run code in response to events like changes in S3, dynamo DB or other AWS services.
3. **Cloud Watch:** Amazon CloudWatch is a monitoring service for AWS resources and the application you run on them. You can use cloud, watch to collect and track metrics, collect and monitor log files and set alarms.
4. **Glue Crawler:** AWS Glue Crawler is a fully managed service that automatically crawls your data sources, identifies data formats and infers schemas to create an AWS Glue Data Catalog.
5. **Data Catalog:** AWS Glue Data Catalog is fully managed meta data repository that makes it easy to Discover and manage data in AWS. You can use the Glue Data Catalog with other AWS services such as Athena.
6. **Amazon Athena:** Amazon Athena is a interactive query service, that makes it easy to analyze data in Amazon S3 using standard SQL. You can use Athena to analyze data in your Glue Data Catalog or in other S3 buckets.

### Install Packages
```
pip install pandas
pip install spotipy
```



