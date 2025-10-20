
  
    
    

    create  table
      "dev"."main"."stg_album__dbt_tmp"
  
    as (
      

SELECT
    user_id,
    album_id,
    LOWER(title) as album_title
FROM "dev"."main"."raw_albums"
    );
  
  