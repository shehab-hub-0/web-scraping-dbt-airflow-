

SELECT
    user_id,
    album_id,
    LOWER(title) as album_title
FROM "dev"."main"."raw_albums"