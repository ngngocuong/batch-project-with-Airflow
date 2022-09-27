DELETE FROM public.user_behavior_metric
WHERE insert_date = '{{ ds }}';

INSERT INTO public.user_behavior_metric (
    customer_id,
    amount_spent,
    review_score,
    review_count,
    insert_date
)
SELECT ip.customer_id,
    CAST(
        SUM(ip.quantity * ip.unit_price) AS DECIMAL(18,5)
    ) AS amount_spent,
    SUM(mrcs.positive_review) AS review_score,
    COUNT(mrcs.cid) AS review_count, 
    '{{ ds }}'
FROM spectrum.invoice_purchase ip
    JOIN (
        SELECT cid,
            CASE 
                WHEN score_film > 7 THEN 1
                ELSE 0
            END AS positive_review
        FROM spectrum.movie_review 
        WHERE insert_date = '{{ ds }}'
    ) mrcs ON ip.customer_id = mrcs.cid
WHERE ip.insert_date = '{{ ds }}'
GROUP BY ip.customer_id;
