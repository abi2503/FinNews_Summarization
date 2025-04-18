import requests, textstat, nltk
from bs4 import BeautifulSoup
from newspaper import Article
from unidecode import unidecode
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline
from langchain.document_loaders import WebBaseLoader

nltk.download("punkt")

def get_full_article_text(url):
    try:
        article = Article(url)
        article.download()
        article.parse()
        return unidecode(article.text.strip())
    except:
        pass
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "lxml")
        paragraphs = soup.find_all("p")
        return unidecode(" ".join([p.get_text() for p in paragraphs if len(p.get_text().split()) > 5]))
    except:
        pass
    try:
        loader = WebBaseLoader(url)
        docs = loader.load()
        if docs:
            return unidecode(docs[0].page_content.strip())
    except:
        pass
    return "Full content not available"

def get_reddit_mentions(ticker, reddit):
    try:
        return sum(1 for _ in reddit.subreddit("wallstreetbets").search(ticker, limit=50))
    except:
        return 0

def enrich_article(article, vader, finbert, reddit, nlp):
    from nltk.tokenize import sent_tokenize
    text = article["article_text"]
    return {
        **article,
        "reddit_mentions": get_reddit_mentions(article["ticker_symbol"], reddit),
        "word_count": len(text.split()),
        "sentence_count": len(sent_tokenize(text)),
        "readability_score": textstat.flesch_reading_ease(text),
        "sentiment_vader": vader.polarity_scores(text)["compound"],
        "sentiment_finbert": finbert(text[:512])[0]["label"],
        "named_entities": ", ".join([ent.text for ent in nlp(text).ents if ent.label_ in ["ORG", "PERSON"]])
    }
