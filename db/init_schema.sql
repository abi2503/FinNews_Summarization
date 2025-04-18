CREATE TABLE IF NOT EXISTS financial_news (
    id SERIAL PRIMARY KEY,
    article_id TEXT,
    ticker_symbol VARCHAR(10),
    article_title TEXT,
    article_text TEXT,
    published_date DATE,
    source VARCHAR(255),
    author VARCHAR(255),
    reddit_mentions INT,
    word_count INT,
    sentence_count INT,
    readability_score FLOAT,
    sentiment_vader FLOAT,
    sentiment_finbert VARCHAR(50),
    named_entities TEXT
);
