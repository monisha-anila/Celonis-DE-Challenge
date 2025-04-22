{{ config(materialized='incremental', unique_key=[ 'invoiceno','stockcode','quantity','InvoiceDate','customerid','unitprice','description']) }}

SELECT country,
  customerid,
  description,
  "InvoiceDate",
  invoiceno,
  quantity,
  stockcode,
  timeframe,
  unitprice,
  CASE WHEN quantity < 0 THEN 'yes' ELSE 'no' END AS returns
FROM public.online_retail_table
WHERE  1=1 {% if is_incremental() %} AND "InvoiceDate" >= (current_date - interval '1 day') {% endif %}

