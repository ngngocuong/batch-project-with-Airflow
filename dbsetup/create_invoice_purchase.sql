create table invoice_purchase(
    invoice_id varchar(100) primary key,
    quantity integer, 
    unit_price integer, 
    invoice_date timestamp, 
    customer_id integer, 
    country varchar(100)
)
