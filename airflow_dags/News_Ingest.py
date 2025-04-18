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

# === Secrets ===
NEWSAPI_KEY = Variable.get("newsapi_key")
REDDIT_CLIENT_ID = Variable.get("reddit_client_id")
REDDIT_CLIENT_SECRET = Variable.get("reddit_client_secret")
REDDIT_USER_AGENT = Variable.get("reddit_user_agent")

# === DAG Config ===
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="step5_load_postgres_dag",
    default_args=default_args,
    description="Step 5: Enrich articles and load to PostgreSQL",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["step-by-step", "postgres"]
)
def step5_dag():

    @task
    def create_table():
        hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        with open("/opt/airflow/db/init_schema.sql", "r") as f:
            schema_sql = f.read()
        hook.run(schema_sql)

    @task
    def fetch_articles():
        tickers = ["AAPL", "TSLA", "GOOGL"]
        to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        from_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        all_articles = []

        def from_newsapi(ticker):
            url = (
                f"https://newsapi.org/v2/everything?"
                f"q={ticker}&from={from_date}&to={to_date}"
                f"&sortBy=publishedAt&pageSize=100&apiKey={NEWSAPI_KEY}"
            )
            logging.info(f"🔍 Fetching from NewsAPI for {ticker}")
            response = requests.get(url)
            try:
                res = response.json()
            except Exception as e:
                logging.error(f"❌ Error parsing NewsAPI JSON: {e}")
                return []

            if res.get("status") != "ok":
                logging.error(f"❌ NewsAPI error: {res.get('message')}")
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
            logging.info(f"🌐 Google News returned {len(feed.entries)} entries for {ticker}")
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
                    logging.error(f"❌ Error processing entry: {e}")
            return articles

        for ticker in tickers:
            all_articles.extend(from_newsapi(ticker))
            all_articles.extend(from_google_news(ticker))

        # Deduplicate by article_title
        unique_articles = {}
        for article in all_articles:
            title = article.get("article_title", "").strip().lower()
            if title and article["article_text"] != "Full content not available":
                unique_articles[title] = article

        logging.info(f"📦 Final deduplicated articles: {len(unique_articles)}")
        return list(unique_articles.values())

    @task
    def enrich_articles(articles):
        nlp = spacy.load("en_core_web_sm")
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        vader = __import__("vaderSentiment.vaderSentiment").vaderSentiment.SentimentIntensityAnalyzer()
        finbert = pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone")

        enriched = [enrich_article(article, vader, finbert, reddit, nlp) for article in articles]
        return enriched

    @task
    def load_to_postgres(rows):
        hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        insert_query = """
            INSERT INTO financial_news (
                article_id, ticker_symbol, article_title, article_text, published_date, source, author,
                reddit_mentions, word_count, sentence_count, readability_score,
                sentiment_vader, sentiment_finbert, named_entities
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        for row in rows:
            values = tuple(row.get(k) for k in [
                "article_id", "ticker_symbol", "article_title", "article_text", "published_date", "source", "author",
                "reddit_mentions", "word_count", "sentence_count", "readability_score",
                "sentiment_vader", "sentiment_finbert", "named_entities"
            ])
            hook.run(insert_query, parameters=values)

    # Task chaining
    create_table() >> load_to_postgres(enrich_articles(fetch_articles()))

dag = step5_dag()
