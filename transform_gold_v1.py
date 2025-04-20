import pandas as pd
from datetime import datetime
import s3fs


def load_silver_data(bucket_name, year, month, day):
    path = f's3://{bucket_name}/silver/year={year}/month={month:02d}/day={day:02d}/reddit_clean.parquet'
    df = pd.read_parquet(path, engine='pyarrow', storage_options={'anon':False})
    return df

def create_gold_tables(df):
    top_positive_posts = df.sort_values(by = 'sentiment_score', ascending = False).head(10)

    subreddit_sentiment_avg = df.groupby('subreddit')['sentiment_score'].mean().reset_index()
    subreddit_sentiment_avg.rename(columns={'sentiment_score': 'avg_sentiment'}, inplace=True)

    sentiment_distribution = df['sentiment_label'].value_counts().reset_index()
    sentiment_distribution.columns = ['sentiment_label', 'count']

    return top_positive_posts, subreddit_sentiment_avg, sentiment_distribution

def save_gold_table(df, bucket_name, year, month, day, table_name):
    fs = s3fs.S3FileSystem(anon=False)
    path = f's3://{bucket_name}/gold/year={year}/month={month:02d}/day={day:02d}/{table_name}.parquet'
    df.to_parquet(path, index=False, engine='pyarrow', storage_options={'anon': False})
    print(f"✅ Saved {table_name} to: {path}")

def transform_gold(bucket_name='reddit-airflow-etl-bucket'):
    now = datetime.utcnow()
    year, month, day = now.year, now.month, now.day

    print("📥 Loading Silver data...")
    df_silver = load_silver_data(bucket_name, year, month, day)

    print("📊 Creating Gold summary tables...")
    top_posts, avg_sentiment, sentiment_dist = create_gold_tables(df_silver)

    print("💾 Saving Gold outputs to S3...")
    save_gold_table(top_posts, bucket_name, year, month, day, 'top_positive_posts')
    save_gold_table(avg_sentiment, bucket_name, year, month, day, 'subreddit_sentiment_avg')
    save_gold_table(sentiment_dist, bucket_name, year, month, day, 'sentiment_distribution')

# Local test
if __name__ == "__main__":
    transform_gold()