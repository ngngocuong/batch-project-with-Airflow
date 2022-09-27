CREATE EXTERNAL SCHEMA spectrum
FROM DATA CATALOG 
DATABASE 'spectrum' 
iam_role 'arn:aws:iam::"$AWS_ID":role/"$IAM_ROLE_NAME"'
CREATE EXTERNAL DATABASE IF NOT EXISTS;


DROP TABLE IF EXISTS spectrum.invoice_purchase;
CREATE EXTERNAL TABLE spectrum.invoice_purchase(
    invoice_id varchar(100) primary key,
    quantity integer, 
    unit_price integer, 
    invoice_date timestamp, 
    customer_id integer, 
    country varchar(100)
) PARTITIONED BY (insert_date DATE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS textfile LOCATION 's3://BUCKET_NAME/stage/invoice_purchase/' TABLE PROPERTIES ('skip.header.line.count' = '1');
-- Add your BUCKET_NAME in above line

DROP TABLE IF EXISTS spectrum.movie_review;
CREATE EXTERNAL TABLE spectrum.movie_review (
    cid VARCHAR(100),
    score_film INTEGER,
    insert_date VARCHAR(12)
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS textfile LOCATION 's3://BUCKET_NAME/stage/movie_review/';
-- Add your BUCKET_NAME in above line

DROP TABLE IF EXISTS public.user_behavior_metric;
CREATE TABLE public.user_behavior_metric (
    customerid INTEGER,
    amount_spent DECIMAL(18, 5),
    review_score INTEGER,
    review_count INTEGER,
    insert_date DATE
);