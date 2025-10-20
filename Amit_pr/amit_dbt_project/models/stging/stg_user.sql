{{ config(materialized='table') }}

SELECT
    user_id,
    LOWER(name) as user_name,
    LOWER(address) as user_city,
    LOWER(company) as user_company
FROM {{ source('raw', 'raw_users') }}