Folder Structure and functionality

Folder                           | Purpose

airflow_dags/                    | Automates ingestion & model retraining using DAGs

db/                              | PostgreSQL schema and DB utility functions

models/                          | Fine-tuning and training of BART/T5 models


summarizer_core/                 | LangChain summarizer logic + model loading

rag_knowledge/                   | Embeddings, vector DBs, document retrieval pipeline

sentiment_analysis/              | FinBERT or VADER-based financial sentiment scoring

evaluation/                      | ROUGE/metrics evaluation logic

app/                             | Streamlit frontend and dropdown components

api/                             | FastAPI/Flask routes for summarization & sentiment endpoints

retrainer/                       | Auto fine-tuning scripts triggered by DAGs or jobs

ensemble_logic/                  | Combine multiple model/sentiment outputs into a consensus prediction

notebooks/                       | EDA and testing for experiments

utils/                           | Configs, logging, helper functions

deployment/                      | Dockerfile, cloudbuild, deployment infrastructure
