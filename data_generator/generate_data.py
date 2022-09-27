import random
from typing import Tuple, List
import uuid
from faker import Faker
from datetime import datetime 
import csv
from contextlib import contextmanager
import psycopg2
import  boto3

lst_price = [10, 20, 35, 50]
customer_id = [i + 1 for i in range(10)]
BUCKET_NAME = ""    # Add your S3 BUCKET that you create


class DatabaseConnection:
    def __init__(self):
        # DO NOT HARDCODE !!!
        self.conn_url = (
            "postgresql://airflow:airflow@localhost:5432/airflow"
        )

    @contextmanager
    def managed_cursor(self, cursor_factory=None):
        self.conn = psycopg2.connect(self.conn_url)
        self.conn.autocommit = True
        self.curr = self.conn.cursor(cursor_factory=cursor_factory)
        try:
            yield self.curr
        finally:
            self.curr.close()
            self.conn.close()


def generate_invoice() ->  List[Tuple[str,int,int,str,int,str]]:
    fake = Faker()
    return [(
            str(uuid.uuid4()), # invoice_id
            random.randrange(5) + 1,    # quantity
            random.choice(lst_price),   # unit_price
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            cust_id,
            fake.country()
        )
        for cust_id in customer_id
    ]


def movie_review():
    lst_movie_review=[]
    for i in range(100):
        lst_movie_review.append((random.choice(customer_id), random.randrange(10)+1, datetime.today().strftime("%Y-%m-%d")))
    return lst_movie_review

def movie_review_to_csv(movie_review_film):
    with open("movie_review.csv", "w", newline='') as new_file:
        field_name = ["customer_id", "score_film", "insert_date"]
        csv_writer = csv.writer(new_file, delimiter=",")
        csv_writer.writerow(field_name)
        for line in movie_review_film:
            csv_writer.writerow(line)

def generate_data():  
    invoice_purchase = generate_invoice()
    print(invoice_purchase)
    movie_review_film = movie_review()
    print(movie_review_film)
    movie_review_to_csv(movie_review_film)

    with DatabaseConnection().managed_cursor() as curr:
        inser_query = "INSERT INTO invoice_purchase (invoice_id, quantity, unit_price, invoice_date, customer_id, country)\
            VALUES (%s,%s,%s,%s,%s,%s)"
        for cd in invoice_purchase:
            curr.execute(inser_query, cd)


    # To connect with S3 using API you have to setup credentials in .aws
    s3 = boto3.client("s3")
    with open("./movie_review.csv", "rb") as f:
        s3.upload_fileobj(
            f, BUCKET_NAME, "stage/movie_review.csv", ExtraArgs={"ACL":"public-read"}
        )



if __name__ == "__main__": 
    generate_data()