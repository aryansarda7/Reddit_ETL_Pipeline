import pandas as pd
from datetime import datetime
import s3fs
from extract_reddit import extract_reddit_posts

def save_bronze(bucket_name = 'reddit-airflow-etl-bucket', limit = 100, subreddit_name = 'worldnews'):
    
    posts = extract_reddit_posts(limit=limit, subreddit_name=subreddit_name)

    df = pd.DataFrame(posts)

    now = datetime.utcnow()
    year = now.year
    month = now.month
    day = now.day

    s3_path = f's3://{bucket_name}/bronze/year={year}/month={month:02d}/day={day:02d}/reddit_raw.csv'

    # df.to_json(s3_path, orient='records', lines = True, storage_option = {'anon': False})

    fs = s3fs.S3FileSystem(anon=False)

    with fs.open(f's3://{s3_path}', 'w', encoding='utf-8') as f:
        df.to_csv(f, index=False)

    print(f"Bronze Data Saved to : {s3_path}")