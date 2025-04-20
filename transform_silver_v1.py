import pandas as pd
import numpy as np
from datetime import datetime
import s3fs
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# --- 1. Load Bronze Data from S3 ---

def load_bronze_data(bucket_name, year, month, day):
    fs = s3fs.S3FileSystem(anon=False)
    path = f's3://{bucket_name}/bronze/year={year}/month={month:02d}/day={day:02d}/reddit_raw.csv'
    df = pd.read_csv(fs.open(path, 'r', encoding = 'utf-8'))
    return df

# --- 2. Apply Cleaning and Sentiment ---

def clean_and_enrich(df):
    df = df[df['title'].notna()].copy()

    df['post_length'] = df['title'].apply(lambda x:len(str(x)))

    analyzer = SentimentIntensityAnalyzer()

    def get_sentiment(text):
        score = analyzer.polarity_scores(str(text))['compound']
        label = 'positive'if score > 0.05 else 'negative' if score <-0.05 else 'neutral'
        return pd.Series([score, label])
    
    df[['sentiment_score', 'sentiment_label']] = df['title'].apply(get_sentiment)

    return df


# --- 3. Data Quality Checks ---

def run_data_quality_checks(df):
    assert len(df) >= 100
    assert df['title'].notna().all()
    assert 'sentiment_score' in df.columns
    print("Data quality checks passed!")


# --- 4. Save Cleaned Data as Parquet to Silver Layer ---

def save_to_silver(df, bucket_name, year, month, day):
    fs = s3fs.S3FileSystem(anon=False)
    path = f's3://{bucket_name}/silver/year={year}/month={month:02d}/day={day:02d}/reddit_clean.parquet'
    df.to_parquet(path, index=False, engine = 'pyarrow', storage_options={'anon':False})
    print(f'Silver data saved to: {path}')

# --- 5. Orchestrator Function ---
def transform_silver(bucket_name='reddit-airflow-etl-bucket'):
    now = datetime.utcnow()
    year, month, day = now.year, now.month, now.day

    print("📥 Loading Bronze data...")
    df = load_bronze_data(bucket_name, year, month, day)

    print("🧹 Cleaning and enriching...")
    df_clean = clean_and_enrich(df)

    print("🔍 Running data quality checks...")
    run_data_quality_checks(df_clean)

    print("📤 Saving to Silver layer...")
    save_to_silver(df_clean, bucket_name, year, month, day)


if __name__ == "__main__":
    transform_silver()