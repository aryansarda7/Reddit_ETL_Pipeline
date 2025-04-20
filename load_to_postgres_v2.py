import pandas as pd
import s3fs
import psycopg2
from io import StringIO
import csv
from datetime import datetime, timedelta

# Define the schema for each table
SCHEMAS = {
    'top_positive_posts': """
        id TEXT,
        title TEXT,
        author TEXT,
        score INTEGER,
        url TEXT,
        num_comments INTEGER,
        created_utc FLOAT,
        text TEXT,
        subreddit TEXT,
        extracted_at TIMESTAMP,
        sentiment_score FLOAT,
        sentiment_label TEXT
    """,
    'subreddit_sentiment_avg': """
        subreddit TEXT,
        avg_sentiment_score FLOAT,
        year INTEGER,
        month INTEGER,
        day INTEGER
    """,
    'sentiment_distribution': """
        sentiment_label TEXT,
        count INTEGER,
        year INTEGER,
        month INTEGER,
        day INTEGER
    """
}

# Columns to enforce type conversion (optional per table)
COLUMN_CASTS = {
    'top_positive_posts': {
        'score': int,
        'num_comments': int,
        'created_utc': float,
        'sentiment_score': float
    },
    'subreddit_sentiment_avg': {
        'avg_sentiment_score': float
    },
    'sentiment_distribution': {
        'count': int
    }
}

def load_table(table_name, bucket_name, year, month, day):
    file_path = f's3://{bucket_name}/gold/year={year}/month={month:02d}/day={day:02d}/{table_name}.parquet'
    
    try:
        fs = s3fs.S3FileSystem(anon=False)
        with fs.open(file_path, 'rb') as f:
            df = pd.read_parquet(f)
        print(f"✅ Loaded {len(df)} rows from {file_path}")
    except Exception as e:
        print(f"❌ Failed to read {table_name} from S3: {str(e)}")
        return

    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user='',    #Airflow User
            password='',   #Airflow Password
            host='localhost',
            port= 5432          #Port
        )
        cursor = conn.cursor()

        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(f"CREATE TABLE {table_name} ({SCHEMAS[table_name]})")

        # Cast columns if needed
        if table_name in COLUMN_CASTS:
            for col, dtype in COLUMN_CASTS[table_name].items():
                if col in df.columns:
                    df[col] = df[col].astype(dtype)

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_ALL)
        buffer.seek(0)

        cursor.copy_expert(
            f"""COPY {table_name} FROM STDIN WITH CSV QUOTE '"' DELIMITER ',' NULL ''""",
            buffer
        )
        conn.commit()
        print(f"✅ Loaded {len(df)} rows into {table_name}")

    except Exception as e:
        print(f"❌ Database error for {table_name}: {str(e)}")
        conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

def load_all_tables(bucket_name='reddit-airflow-etl-bucket', days_offset=0):
    now = datetime.utcnow() - timedelta(days=days_offset)
    year, month, day = now.year, now.month, now.day

    tables = [
        'top_positive_posts',
        'subreddit_sentiment_avg',
        'sentiment_distribution'
    ]
    for table in tables:
        load_table(table, bucket_name, year, month, day)

if __name__ == "__main__":
    load_all_tables()