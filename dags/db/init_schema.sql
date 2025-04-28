-- db/init_schema.sql

CREATE TABLE IF NOT EXISTS financial_news (
    article_id TEXT PRIMARY KEY,
    ticker_symbol TEXT,
    article_title TEXT,
    article_text TEXT,
    published_date DATE,
    source TEXT,
    author TEXT,
    reddit_mentions INT,
    word_count INT,
    sentence_count INT,
    readability_score FLOAT,
    sentiment_vader FLOAT,
    sentiment_finbert TEXT,
    named_entities JSON
);
