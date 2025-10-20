{{ config(materialized='table') }}

SELECT
    user_id,
    album_id,
    LOWER(title) as album_title
FROM {{ source('raw', 'raw_albums') }}