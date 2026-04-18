# Agentic Fraud Investigation MVP

A deterministic, agentic fraud investigation system for **card-not-present (CNP) ecommerce payments**.

This project combines:

* rule-based fraud scoring
* dataset-driven investigation
* LLM-powered query interface (parsing only)
* SQL-backed analytics engine

---

# Features

## Fraud Investigation Engine

* Deterministic scoring (no LLM in decisioning)
* Multi-signal fraud detection:

  * velocity
  * anomalies
  * payment auth (AVS, CVV, 3DS)
* Structured investigation output

## Agentic Query Interface

* Natural language → structured query (OpenAI tools)
* Strict schema validation (Pydantic)
* Deterministic SQL execution

## Dataset Pipeline

* CSV → SQLite ingestion
* Batch enrichment (fraud scoring)
* Indexed query layer

## Analytics

* Transaction ranking
* User risk analysis
* Aggregations (country, merchant category, channel)

---

# Architecture

```
User Prompt
     ↓
LLM (OpenAI - tools)
     ↓
Structured Query (DatasetQuery)
     ↓
SQL Engine (SQLite)
     ↓
Result Formatter
```

### Design Principle

LLM is used **only for parsing**, never for:

* scoring
* decisions
* ranking

All fraud logic is:

* deterministic
* testable
* reproducible

---

# Project Structure

```
app/
  agent/                  # Fraud investigation logic
  query/                  # Query contract, parser, SQL engine
  storage/                # Dataset + DB layer
  data/
    dataset/              # CSV dataset
run.py                    # CLI entrypoint
```

---

# Setup

## 1. Clone

```
git clone <repo-url>
cd <repo>
```

## 2. Virtual environment

```
python3 -m venv venv
source venv/bin/activate
```

## 3. Install dependencies

```
pip install -r requirements.txt
```

## 4. Environment variables

Create `.env`:

```
OPENAI_API_KEY=your_api_key
OPENAI_MODEL=gpt-4.1-mini
DB_PATH=./app/data/db.sqlite
```

---

# Usage

## Step 1: Initialize database

```
python3 run.py init-dataset-db
```

## Step 2: Ingest dataset

```
python3 run.py ingest-dataset \
  --file-path ./app/data/dataset/transactions.csv
```

## Step 3: Build enriched dataset

```
python3 run.py build-enriched-dataset-cmd \
  --batch-size 1000 \
  --log-every 500
```

### For testing (small subset)

```
python3 run.py build-enriched-dataset-cmd \
  --limit 2000 \
  --batch-size 250
```

---

## Step 4: Ask questions

```
python3 run.py ask-dataset \
  --question "What is the 3rd most fraudulent transaction?"
```

---

# Example Queries

## Transaction ranking

```
python3 run.py ask-dataset --question "Show top 5 highest-risk transactions"
```

```
python3 run.py ask-dataset --question "What is the 3rd most fraudulent transaction?"
```

---

## User analysis

```
python3 run.py ask-dataset --question "Which user has the most suspicious transactions?"
```

```
python3 run.py ask-dataset --question "Show top 10 users by suspicious transaction count"
```

---

## Aggregations (Group By)

```
python3 run.py ask-dataset --question "Which merchant category has the most declined transactions?"
```

```
python3 run.py ask-dataset --question "Show top 5 countries by suspicious transaction count"
```

```
python3 run.py ask-dataset --question "Which channel has the fewest approved transactions?"
```

---

## Filtering

```
python3 run.py ask-dataset --question "How many declined transactions belong to user 32?"
```

```
python3 run.py ask-dataset --question "Show top 5 highest-risk transactions in electronics"
```

---

# Unsupported Queries

These are intentionally rejected:

```
python3 run.py ask-dataset --question "Why is this dataset broken?"
```

```
python3 run.py ask-dataset --question "What will be the next fraudulent transaction?"
```

Reason:

* not deterministic
* not SQL-executable
* outside contract

---

# Query Contract

Supported intents:

* nth_highest_risk_transaction
* top_k_highest_risk_transactions
* user_with_most_suspicious_transactions
* top_k_users_by_suspicious_transaction_count
* count_transactions
* top_category_by_transaction_count

Supported grouping:

* merchant_category
* country
* channel

---

# Database

## raw_transactions

Original dataset.

## enriched_transactions

Adds:

* risk_score
* risk_level
* recommended_action

---

# ⚡ Performance

* Batch enrichment (500–2000 rows recommended)
* Indexed queries
* Fast reads after enrichment
* LLM cost minimal (parsing only)

---

# Debug & Maintenance

## Dataset status

```
python3 run.py dataset-status
```

## Reset database

```
python3 run.py reset-dataset-db
```

---

# Design Philosophy

### Deterministic Core

No AI in decision-making.

### Agentic Interface

Natural language → structured execution.

### Separation of Concerns

| Layer   | Role        |
| ------- | ----------- |
| LLM     | Parsing     |
| SQL     | Truth       |
| Scoring | Fraud logic |

---

# Future Work

* Dataset quality analysis
* Similarity search (vector DB)
* Real-time ingestion
* API (FastAPI)
* Dashboard UI

---

# Author

Masoud Amouzgar
System thinker | Payment architect | Problem solver
