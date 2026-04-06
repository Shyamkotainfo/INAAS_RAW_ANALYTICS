# Raw Data Lab – LLM‑Driven File Analytics with Databricks

## Overview

`raw_data_lab` is a sandbox project to enable **LLM‑driven analytics and summarization on raw data files** stored in S3.

It allows users to:

* Ask analytical questions on raw files (CSV, JSON, Parquet, Avro, etc.)
* Get Spark‑executed results using PySpark (via Databricks)
* Ask for a full explanation / summary of a file
* Use RAG to ground LLM responses on real schema and metadata

This project is intentionally developed **outside the main INAAS backend** and can be merged later once stabilized.

---

## Core Principles

* No databases, no tables
* No Glue, no Hive, no catalog dependency
* All execution is PySpark on files
* LLM never executes code
* Strict separation between control plane and data plane

---

## Architecture at a Glance

### Control Plane (Backend)

* Handles user questions
* Uses Bedrock Knowledge Base (OpenSearch Serverless) for RAG
* Uses Titan Embeddings for semantic search
* Uses Amazon Nova Pro for reasoning
* Submits jobs to Databricks

### Data Plane (Databricks)

* Reads files from S3
* Extracts schema and samples
* Executes SparkSQL on DataFrames
* Writes results back to S3 / DBFS

---

## Technology Stack

| Component        | Service                                        |
| ---------------- | ---------------------------------------------- |
| Raw storage      | Amazon S3                                      |
| Execution engine | Databricks (PySpark)                           |
| LLM              | Amazon Nova Pro                                |
| Embeddings       | Amazon Titan Embeddings                        |
| RAG store        | Bedrock Knowledge Base (OpenSearch Serverless) |

---

## End‑to‑End Flow

### 1. File Registration / First Access

1. Backend triggers a Databricks job in **introspection mode**
2. Databricks:

   * Reads file from S3
   * Extracts schema
   * Extracts 1–2 sample rows
3. Backend:

   * Converts metadata to canonical format
   * Generates embeddings using Titan
   * Indexes metadata into Bedrock Knowledge Base

This step runs once per file (or on demand).

---

### 2. User Asks an Analytics Question

Example:

> "Average order value by country"

Flow:

1. Backend classifies intent as `analytics`
2. User question is embedded (Titan)
3. Bedrock KB retrieves relevant files + columns
4. Amazon Nova Pro generates **SparkSQL only**
5. Backend submits Databricks job with:

   * File path
   * SparkSQL
6. Databricks:

   * Reads file
   * Registers temp view
   * Executes SparkSQL
   * Writes result
7. Backend fetches result
8. Amazon Nova Pro summarizes the output

---

### 3. User Asks for File Summary

Example:

> "Explain what this file contains"

Flow:

1. Backend classifies intent as `file_summary`
2. Full schema + samples fetched from KB
3. Amazon Nova Pro generates a human‑readable explanation

No Spark execution is required in this path.

---

## Folder Structure

```
raw_data_lab/
├── config/            # AWS, Bedrock, Databricks configs
├── core/              # Settings, logging, constants
├── databricks/        # Databricks Jobs API integration
├── spark/             # Code executed inside Databricks
├── introspection/     # Schema & sample extraction
├── metadata/          # Canonical schema & KB indexing
├── bedrock/           # Nova Pro, Titan, KB clients
├── rag/               # File & column retrieval
├── query/             # Intent & SparkSQL generation
├── summarization/     # File & result summaries
├── orchestrator/      # End‑to‑end flow control
├── playground.py      # Local test harness
└── tests/             # Unit tests
```

---

## Key Execution Boundary

### Backend

* Never reads data
* Never executes Spark
* Never sees full datasets

### Databricks

* Never calls LLMs
* Never talks to Bedrock
* Executes only validated Spark logic

---

## How Databricks Is Used (Option 2)

* Backend submits jobs via Databricks Jobs API
* Jobs run `spark/spark_job_entry.py`
* Parameters control job behavior:

  * `mode = introspection | query`
  * `file_path`
  * `spark_sql` (optional)

All jobs are stateless and repeatable.

---

## Running Locally (Development)

### Prerequisites

* Python 3.10+
* PySpark
* AWS credentials (for S3 + Bedrock)
* Databricks PAT token

### Install dependencies

```bash
pip install -r requirements.txt
```

### Test file introspection

Edit `playground.py`:

```python
FILE_PATH = "s3://your-bucket/path/file.parquet"
```

Run:

```bash
python playground.py
```

---

## Security & Guardrails

* SparkSQL only (no DataFrame API from LLM)
* Only temp views allowed
* No DDL, no writes from LLM
* Column whitelist enforced
* Result row limits applied
* IAM isolation between services

---

## Why This Exists Separately from INAAS

* Faster iteration
* Clear boundaries
* Zero risk to existing flows
* Easy merge once stable

---

## What Comes Next

* Bedrock KB index mappings
* Column‑level embeddings
* Query validation rules
* Cost and execution metrics
* Merge into INAAS backend

---

## Ownership

This is an engineering sandbox, not a demo. Each module is production‑oriented and replaceable.
