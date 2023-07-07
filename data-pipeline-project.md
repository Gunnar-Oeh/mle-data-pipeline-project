# Data Pipeline Project

In this project you are going to build a data pipeline that is processing the `Green Taxi Trips` of the NYC Taxi Trip Dataset. 

1. The first task is to write a script that is directly uploading the data of the first 3 months of the year 2021 to a GCS bucket.
  - Set up Google Cloud
  - Familiarize with Buckets
    - How to access the Buckets 
    - especially for this task (how stored in the Bucket)
  - Refactoring data_ingestion.py customized to many months and the GCS bucket

2. The second task is to write a ETL or ELT pipeline that is taking the data from the GCS bucket and that is processing the data and calculating the revenue per day.  This can be done in the Google Cloud or on your local machine.
   - ETL with spark before customized data are injected into the Warehouse. Runs on the cloud, how?
   - ELT directly load into a Warehouse and Transformed there with dbt.

Bonus task if you have the time:

1. Use prefect for the Workflow Orechestration.
   - Python commands only?
   - Or from within the prefect script Docker-Run/DBT run


## Questions:

1. What are the steps you took to complete the project?
   a. GoogleCloud:
     - Created new project (mle-neue-fische-gunnaroeh) 
     - enable Identity and Access Management (IAM) API.
     - Create one Service account with the two roles Storage Admin, Storage Object Admin
       - enter details under service accounts, select a role, add another role
     - Created Bucket 01_data_pipeline_project in europe-west-3 with public access prevention   

3. What are the challenges you faced?
4. What are the things you would do differently if you had more time?

## Submission:

Please submit your solution as a link to a github repository. The repository should contain the scripts and a README.md file that is answering the questions above.