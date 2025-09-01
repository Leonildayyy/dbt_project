-- models/audit_debug.sql

{{ auditing(
    old_db = target.database,
    old_schema = "dbt_yy",
    old_identifier = "customer_orders",
    new_model = "fct_customer_orders",
    pk = "order_id"
) }}