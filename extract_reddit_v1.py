import praw
from datetime import datetime

def extract_reddit_posts(limit = 100, subreddit_name = 'worldnews'):
    reddit = praw.Reddit(
        client_id = "",       #Reddit Client ID
        client_secret="",     #Reddit Client Secret
        user_agent="linux:Airflow_ETL_Pipeline:v1.0"
    )

    posts = []

    subreddit = reddit.subreddit(subreddit_name)
    for post in subreddit.hot(limit=limit):
        posts.append(
            {
                'id' : post.id,
                'title' : post.title,
                'author' : str(post.author),
                'score' : post.score,
                'url' : post.url,
                'num_comments' : post.num_comments,
                'created_utc' : post.created_utc,
                'text': post.selftext,
                'subreddit': subreddit_name,
                'extracted_at': datetime.utcnow().isoformat()
            }
        )
    return posts

if __name__ == "__main__":
    posts = extract_reddit_posts(limit=5)
    print(posts[0])