COPY (
       select invoice_id,
              quantity,
              unit_price,
              invoice_date,
              customer_id,
              country
       from public.invoice_purchase
       where invoce_date = date(invoicce_date)
) TO '{{ params.invoice_purchase }}' WITH (FORMAT CSV, HEADER);