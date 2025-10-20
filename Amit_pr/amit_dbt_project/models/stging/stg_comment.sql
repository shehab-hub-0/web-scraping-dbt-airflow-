{{ config(materialized='table') }}

SELECT
    post_id,
    comment_id,
    LOWER(name) as commenter_name,
    LOWER(email) as commenter_email,
    LOWER(body) as comment_body
FROM {{ source('raw', 'raw_comments') }}