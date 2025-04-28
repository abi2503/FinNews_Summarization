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


def enrich_article(article, vader, finbert_pipeline, reddit, nlp):
    text = article["article_text"]

    # 1. Vader Sentiment
    vader_score = vader.polarity_scores(text)["compound"]

    # 2. FinBERT Sentiment (proper safe)
    encoded_inputs = finbert_tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    input_ids = encoded_inputs["input_ids"][0]

    # Split into safe 510 token chunks (account for [CLS] and [SEP])
    chunk_size = 510
    chunks = [input_ids[i:i+chunk_size] for i in range(0, len(input_ids), chunk_size)]

    finbert_sentiments = []

    for chunk in chunks:
        if len(chunk) == 0:
            continue

        # Prepare full input with [CLS] and [SEP]
        chunk = torch.cat([
            torch.tensor([finbert_tokenizer.cls_token_id]), 
            chunk, 
            torch.tensor([finbert_tokenizer.sep_token_id])
        ])

        # Send directly to FinBERT pipeline
        inputs = finbert_tokenizer.decode(chunk, skip_special_tokens=True)
        result = finbert_pipeline(inputs)[0]
        finbert_sentiments.append(result["label"])

    # Aggregate sentiments
    if finbert_sentiments:
        pos = finbert_sentiments.count("positive")
        neg = finbert_sentiments.count("negative")
        neu = finbert_sentiments.count("neutral")

        if pos >= neg and pos >= neu:
            final_finbert_sentiment = "positive"
        elif neg >= pos and neg >= neu:
            final_finbert_sentiment = "negative"
        else:
            final_finbert_sentiment = "neutral"
    else:
        final_finbert_sentiment = "neutral"

    # 3. Other NLP features
    doc = nlp(text)

    article["word_count"] = len(text.split())
    article["sentence_count"] = len(list(doc.sents))
    article["readability_score"] = textstat.flesch_reading_ease(text) if text else 0
    article["sentiment_vader"] = vader_score
    article["sentiment_finbert"] = final_finbert_sentiment
    article["named_entities"] = [ent.text for ent in doc.ents]

    try:
        subreddit = reddit.subreddit("all")
        mentions = sum(1 for submission in subreddit.search(article["ticker_symbol"], limit=10))
        article["reddit_mentions"] = mentions
    except Exception as e:
        article["reddit_mentions"] = 0

    return article


