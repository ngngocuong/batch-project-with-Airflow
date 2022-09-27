# batch-project-with-Airflow

## Objective
  <img src="/images/objective.png" alt="objective" title="objective">
  
  
 - In movie_review, score_film is randomly from 1-10. If score_film > 7 it will be good review and asgin as 1 <br />
 - amount_spent = quantity * unit_price <br />
 - review_score is the number of reviews greater than 7
 - review_count is the number of reviews of each customer
 - insert_date is date that invoice_purchase happen

## Desgin
  <img src="/images/design.jpg" alt="design" title="design">
  
  
 - File py will generate data for moive_review.csv then push to s3 and invoce_purchase table in database <br />
 - Airflow wil run to push data to S3, create external table and insert data in public.user_behavior in Redshift <br />

## Prerequisites
  1. AWS account to setup infrastructure
  2. Docker to run Airflow
  3. Dbever connect Redshift to check result 

## Setup
  1. Down load code
  2. Create S3 bucket to storage data
  3. Run file py to generate data (remember add permission to push file csv to S3 and change code in file py)
  4. Download and config to run Airflow https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
  5. Go to airflow webserver container and add connections to Postgres DB and AWS Redshift
  ```
  
  ```
  6. Create Redshift cluster and set role for that cluster to use S3
  7. Using Dbeaver to connect Redshift and check the result: https://www.kodyaz.com/aws/connect-to-amazon-redshift-using-dbeaver-database-management-tool.aspx

## Result
 - Data in spectrum.invoice_purchase <br />
<img src="/images/invoice_purchase.png" alt="invoice_purchase" title="invoice_purchase">
 - Data in spectrum.movie_review <br />
<img src="/images/movie_review.jpg" alt="movie_review" title="movie_review"> <br />
 - user_behavior_metric <br />
<img src="/images/result.png" alt="result" title="result">
 
