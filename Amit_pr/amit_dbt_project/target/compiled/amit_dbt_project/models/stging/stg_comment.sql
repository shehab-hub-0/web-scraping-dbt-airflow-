

SELECT
    post_id,
    comment_id,
    LOWER(name) as commenter_name,
    LOWER(email) as commenter_email,
    LOWER(body) as comment_body
FROM "dev"."main"."raw_comments"