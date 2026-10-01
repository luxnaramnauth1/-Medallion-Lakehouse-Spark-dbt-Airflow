select
    customer_id,
    first_name,
    last_name,
    concat_ws(' ', first_name, last_name) as full_name,
    email,
    is_email_valid,
    country,
    signup_date,
    updated_at
from {{ source('silver', 'customers') }}
