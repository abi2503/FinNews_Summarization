# summarizer_core/summarizer_chain.py

from summarizer_core.model_loader import load_model
from nltk.tokenize import sent_tokenize
import nltk

nltk.download("punkt")

def split_into_chunks(text, max_words=500):
    sentences = sent_tokenize(text)
    chunks = []
    current_chunk = []

    for sentence in sentences:
        if len(" ".join(current_chunk + [sentence]).split()) <= max_words:
            current_chunk.append(sentence)
        else:
            chunks.append(" ".join(current_chunk))
            current_chunk = [sentence]

    if current_chunk:
        chunks.append(" ".join(current_chunk))

    return chunks

def summarize_articles(articles, model_name="bart", target_words=250):
    # Step 1: Combine all article texts
    full_text = " ".join(article["article_text"] for article in articles if article["article_text"])
    
    if not full_text.strip():
        return "No content available to summarize."

    # Step 2: Chunk into smaller pieces
    chunks = split_into_chunks(full_text, max_words=500)

    # Step 3: Load the model
    summarizer = load_model(model_name)

    # Step 4: Generate summary for each chunk
    intermediate_summaries = []
    for chunk in chunks:
        try:
            summary = summarizer(chunk, max_length=150, min_length=40, do_sample=False)[0]["summary_text"]
            intermediate_summaries.append(summary)
        except Exception as e:
            print(f"Error summarizing chunk: {e}")
            continue

    # Step 5: Combine summaries and compress again
    combined_summary = " ".join(intermediate_summaries)
    final_chunks = split_into_chunks(combined_summary, max_words=target_words)

    final_summary = final_chunks[0] if final_chunks else combined_summary
    return final_summary
