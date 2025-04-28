# airflow_dags/step5_load_postgres_dag.py

# === Standard Libs ===
import logging, requests
from datetime import datetime, timezone, timedelta

# === Airflow ===
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# === NLP + Web ===
import praw, feedparser, spacy
from transformers import pipeline

# === Helpers ===
from utils.helpers import get_full_article_text, enrich_article
import boto3
import json
# === Secrets ===
NEWSAPI_KEY = Variable.get("newsapi_key")
REDDIT_CLIENT_ID = Variable.get("reddit_client_id")
REDDIT_CLIENT_SECRET = Variable.get("reddit_client_secret")
REDDIT_USER_AGENT = Variable.get("reddit_user_agent")
AWS_ACCESS_KEY = Variable.get("aws_access_key")
AWS_SECRET_KEY = Variable.get("aws_secret_key")
S3_BUCKET_NAME = Variable.get("s3_bucket_name")

# === DAG Config ===
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="Data_Ingestion_Dag",
    default_args=default_args,
    description="Fetch, enrich, and upload financial news articles per ticker to S3",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ingestion", "enrichment", "s3_upload"]
)
def step5_dag():

    @task
    def fetch_articles():
        # === Load ticker list from Airflow Variable ===
        tickers = Variable.get("ticker_list", deserialize_json=True)

        to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        from_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        all_articles = []

        def from_newsapi(ticker):
            url = (
                f"https://newsapi.org/v2/everything?"
                f"q={ticker}&from={from_date}&to={to_date}"
                f"&sortBy=publishedAt&pageSize=100&apiKey={Variable.get('newsapi_key')}"
            )
            logging.info(f"Fetching from NewsAPI for {ticker}")
            response = requests.get(url)
            try:
                res = response.json()
            except Exception as e:
                logging.error(f"Error parsing NewsAPI JSON: {e}")
                return []

            if res.get("status") != "ok":
                logging.error(f"NewsAPI error: {res.get('message')}")
                return []

            return [{
                "article_id": a.get("url"),
                "ticker_symbol": ticker,
                "article_title": a.get("title"),
                "article_text": get_full_article_text(a.get("url")),
                "published_date": to_date,
                "source": a["source"]["name"],
                "author": a.get("author", "N/A")
            } for a in res.get("articles", [])]

        def from_google_news(ticker):
            url = f"https://news.google.com/rss/search?q={ticker}+stock+market"
            feed = feedparser.parse(url)
            logging.info(f"Google News returned {len(feed.entries)} entries for {ticker}")
            articles = []

            for entry in feed.entries:
                try:
                    real_url = entry.get("source", {}).get("href", entry.link)
                    text = get_full_article_text(real_url)
                    articles.append({
                        "article_id": real_url,
                        "ticker_symbol": ticker,
                        "article_title": entry.title,
                        "article_text": text,
                        "published_date": to_date,
                        "source": entry.get("source", {}).get("title", "Google News"),
                        "author": "N/A"
                    })
                except Exception as e:
                    logging.error(f"Error processing entry: {e}")
            return articles

        for ticker in tickers:
            articles = from_newsapi(ticker) + from_google_news(ticker)
            for article in articles:
                article["ticker_symbol"] = ticker
            all_articles.extend(articles)

        unique_articles = {}
        for article in all_articles:
            title = article.get("article_title", "").strip().lower()
            if title and article["article_text"] != "Full content not available":
                unique_articles[title] = article

        logging.info(f"Final deduplicated articles: {len(unique_articles)}")
        return list(unique_articles.values())

    @task
    def enrich_articles(articles):
        nlp = spacy.load("en_core_web_sm")
        reddit = praw.Reddit(
            client_id=Variable.get("reddit_client_id"),
            client_secret=Variable.get("reddit_client_secret"),
            user_agent=Variable.get("reddit_user_agent")
        )
        vader = __import__("vaderSentiment.vaderSentiment").vaderSentiment.SentimentIntensityAnalyzer()
        finbert = pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone")

        enriched = [enrich_article(article, vader, finbert, reddit, nlp) for article in articles]
        return enriched

    @task
    def upload_to_s3(rows):
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )

        today_date = datetime.now().strftime('%Y-%m-%d')

        # Split by ticker
        ticker_to_articles = {}
        for article in rows:
            ticker = article.get("ticker_symbol")
            ticker_to_articles.setdefault(ticker, []).append(article)

        for ticker, articles in ticker_to_articles.items():
            s3_key = f"news_data/{today_date}/{ticker}/enriched_articles.json"
            tmp_dir = "/tmp"
            output_path = os.path.join(tmp_dir, f"{ticker}_enriched_articles.json")

            with open(output_path, "w") as f:
                json.dump(articles, f)

            try:
                s3_client.upload_file(output_path, S3_BUCKET_NAME, s3_key)
                s3_url = f"s3://{S3_BUCKET_NAME}/{s3_key}"
                logging.info(f"✅ Uploaded {ticker} articles to {s3_url}")
            except Exception as e:
                logging.error(f"❌ Error uploading {ticker} articles: {e}")
                raise e

    # === Chaining ===
    fetched = fetch_articles()
    enriched = enrich_articles(fetched)
    upload_to_s3(enriched)

# === Create the DAG object ===
dag = step5_dag()