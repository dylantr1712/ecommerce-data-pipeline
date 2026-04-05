truncate table staging.stg_orders_raw;
truncate table staging.stg_customers_raw;
truncate table staging.stg_order_items_raw;
truncate table staging.stg_order_payments_raw;
truncate table staging.stg_order_reviews_raw;
truncate table staging.stg_products_raw;
truncate table staging.stg_sellers_raw;
truncate table staging.stg_product_category_translation_raw;

copy staging.stg_orders_raw
from 's3://dylan-ecommerce-data-pipeline/silver/orders/'
iam_role 'arn:aws:iam::313828097071:role/RedshiftS3ReadRole'
format as parquet;

copy staging.stg_customers_raw
from 's3://dylan-ecommerce-data-pipeline/silver/customers/'
iam_role 'arn:aws:iam::313828097071:role/RedshiftS3ReadRole'
format as parquet;

copy staging.stg_order_items_raw
from 's3://dylan-ecommerce-data-pipeline/silver/order_items/'
iam_role 'arn:aws:iam::313828097071:role/RedshiftS3ReadRole'
format as parquet;

copy staging.stg_order_payments_raw
from 's3://dylan-ecommerce-data-pipeline/silver/order_payments/'
iam_role 'arn:aws:iam::313828097071:role/RedshiftS3ReadRole'
format as parquet;

copy staging.stg_order_reviews_raw
from 's3://dylan-ecommerce-data-pipeline/silver/order_reviews/'
iam_role 'arn:aws:iam::313828097071:role/RedshiftS3ReadRole'
format as parquet;

copy staging.stg_products_raw
from 's3://dylan-ecommerce-data-pipeline/silver/products/'
iam_role 'arn:aws:iam::313828097071:role/RedshiftS3ReadRole'
format as parquet;

copy staging.stg_sellers_raw
from 's3://dylan-ecommerce-data-pipeline/silver/sellers/'
iam_role 'arn:aws:iam::313828097071:role/RedshiftS3ReadRole'
format as parquet;

copy staging.stg_product_category_translation_raw
from 's3://dylan-ecommerce-data-pipeline/silver/product_category_name_translation/'
iam_role 'arn:aws:iam::313828097071:role/RedshiftS3ReadRole'
format as parquet;
