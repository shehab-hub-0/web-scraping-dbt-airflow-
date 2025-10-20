import requests 
import pandas as pd
import duckdb
import os

def loader():
    print("Starting data loading...")

    users_url = 'http://jsonplaceholder.typicode.com/users'
    albums_url = 'http://jsonplaceholder.typicode.com/albums'
    comments_url = 'https://jsonplaceholder.typicode.com/comments'

    # Fetch data
    users_data = requests.get(users_url).json()
    albums_data = requests.get(albums_url).json()
    comments_data = requests.get(comments_url).json()

    # DataFrames
    users_df = pd.DataFrame([{
        'user_id': u['id'],
        'name': u['name'],
        'address': u['address']['city'],
        'company': u['company']['name']
    } for u in users_data])

    albums_df = pd.DataFrame([{
        'user_id': a['userId'],
        'album_id': a['id'],
        'title': a['title']
    } for a in albums_data])

    comments_df = pd.DataFrame([{
        'post_id': c['postId'],
        'comment_id': c['id'],
        'name': c['name'],
        'email': c['email'],
        'body': c['body']
    } for c in comments_data])

    # Save in correct DuckDB path
    db_path = "/opt/airflow/dags/Amit_pr/amit_dbt_project/dev.db"
    if os.path.exists(db_path):
        os.remove(db_path)   # إزالة أي نسخة بايظة

    print(f"Connecting to DuckDB at {db_path} ...")
    conn = duckdb.connect(db_path)

    # Register DataFrames
    conn.register("users_df", users_df)
    conn.register("albums_df", albums_df)
    conn.register("comments_df", comments_df)

    # Create or replace tables
    conn.execute("CREATE OR REPLACE TABLE raw_users AS SELECT * FROM users_df")
    conn.execute("CREATE OR REPLACE TABLE raw_albums AS SELECT * FROM albums_df")
    conn.execute("CREATE OR REPLACE TABLE raw_comments AS SELECT * FROM comments_df")

    print("Verification:")
    print(conn.execute("SHOW TABLES").fetchall())

    conn.close()
    print("Data loading completed successfully!")

if __name__ == "__main__":
    loader()
