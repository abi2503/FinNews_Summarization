# utils/helpers.py

import requests
from bs4 import BeautifulSoup
import textstat
from transformers import AutoTokenizer

import torch

def get_full_article_text(url):
    """
    Fetch the full article text given a URL.
    """
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return "Full content not available"
        soup = BeautifulSoup(response.content, 'html.parser')
        paragraphs = soup.find_all('p')
        text = ' '.join([para.get_text() for para in paragraphs])
        return text if text else "Full content not available"
    except Exception as e:
        return "Full content not available"



# Load FinBERT tokenizer
finbert_tokenizer = AutoTokenizer.from_pretrained("yiyanghkust/finbert-tone")


def enrich_article(article, vader, finbert, reddit, nlp):
    # Decode unicode-escaped text
    article_text = article.get("article_text", "")
    try:
        decoded_text = bytes(article_text, "utf-8").decode("unicode_escape")
    except Exception as e:
        logging.error(f"Unicode decode error: {e}")
        decoded_text = article_text

    article["article_text"] = decoded_text

    # Sentiment
    sentiment_vader = vader.polarity_scores(decoded_text)["compound"]
    sentiment_finbert = finbert(decoded_text[:1000])[0]["label"] if decoded_text else "neutral"

    # ✅ Safe Reddit Search
    try:
        reddit_mentions = reddit.subreddit("wallstreetbets").search(article.get("ticker_symbol"), limit=10)
        reddit_count = sum(1 for _ in reddit_mentions)
    except Exception as e:
        logging.error(f"Reddit search error: {e}")
        reddit_count = 0

    # NER and features
    doc = nlp(decoded_text)
    named_entities = list(set([ent.text for ent in doc.ents]))

    word_count = len(decoded_text.split())
    sentence_count = decoded_text.count('.') + decoded_text.count('!') + decoded_text.count('?')

    # Readability
    try:
        import textstat
        readability_score = textstat.flesch_reading_ease(decoded_text)
    except:
        readability_score = 50.0

    article.update({
        "word_count": word_count,
        "sentence_count": sentence_count,
        "readability_score": readability_score,
        "sentiment_vader": sentiment_vader,
        "sentiment_finbert": sentiment_finbert,
        "named_entities": named_entities,
        "reddit_mentions": reddit_count
    })

    return article



