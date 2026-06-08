# AWS Integration & Data Engineering Architecture

## Current Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     AWS Cloud                               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐    ┌─────────────┐    ┌──────────────┐   │
│  │ S3 Bucket    │───▶│  Lambda/    │───▶│  ECS/App     │   │
│  │ (Raw Data)   │    │  Glue ETL   │    │  Runner      │   │
│  │              │    │             │    │  (main.py)   │   │
│  └──────────────┘    └─────────────┘    └──────────────┘   │
│         △                    │                    │          │
│         │                    ▼                    ▼          │
│         │            ┌─────────────┐    ┌──────────────┐   │
│         │            │  RDS/DDB    │    │  CloudWatch  │   │
│         │            │  (Metadata) │    │  (Logs)      │   │
│         │            └─────────────┘    └──────────────┘   │
│         │                    │                               │
│         └────────────────────┘                               │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Vector DB (ChromaDB on S3/EBS)              │  │
│  │    - Embedded restaurant reviews                     │  │
│  │    - Semantic search index                           │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Data Engineering Pipeline

### 1. **Data Ingestion (S3 → Lambda)**
```
realistic_restaurant_reviews.csv
        ↓
    S3 Bucket
        ↓
AWS Lambda (Trigger)
        ↓
Extract data
```

### 2. **ETL Processing (AWS Glue or Lambda)**
```
Raw CSV Data
    ↓
Extract: Read from S3
    ↓
Transform: 
  - Clean data
  - Parse reviews
  - Generate embeddings
    ↓
Load: Store in Vector DB
```

### 3. **Vector Database (ChromaDB)**
- Stores embedded restaurant reviews
- Enables semantic search
- Can run on:
  - **EBS Volume** (persistent storage on EC2)
  - **S3** (if using serverless option)
  - **RDS** (PostgreSQL with pgvector extension)

### 4. **Main Application (main.py)**
```
User Question
    ↓
Container (Docker)
    ↓
LangChain + Ollama LLM
    ↓
Query Vector DB
    ↓
Generate Response
```

## How main.py Runs in AWS

### Option 1: ECS (Elastic Container Service)
```
1. Docker image pushed to ECR
2. ECS Task pulls image
3. Task container runs main.py
4. main.py connects to:
   - Vector DB (mounted volume or RDS)
   - Ollama (self-hosted or separate service)
```

### Option 2: Lambda (Serverless - for API)
```
API Gateway Request
    ↓
Lambda Function
    ↓
Invoke Python code (similar to main.py)
    ↓
Query Vector DB
    ↓
Return Response
```

### Option 3: App Runner
```
1. Push Docker image to ECR
2. App Runner auto-deploys
3. Container runs continuously
4. Accessible via URL
```

## ETL Pipeline Setup

### Step 1: Create Lambda Function for ETL
```python
# lambda_etl.py
import boto3
import pandas as pd
from langchain_community.embeddings import HuggingFaceEmbeddings
import chromadb

def lambda_handler(event, context):
    # Read from S3
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='pizza-bot-bucket', Key='realistic_restaurant_reviews.csv')
    df = pd.read_csv(obj['Body'])
    
    # Generate embeddings
    embeddings = HuggingFaceEmbeddings()
    
    # Store in ChromaDB
    chroma_client = chromadb.HttpClient(host='vector-db-endpoint')
    collection = chroma_client.get_or_create_collection(name="reviews")
    
    for idx, row in df.iterrows():
        collection.add(
            ids=[str(idx)],
            documents=[row['review']],
            embeddings=[embeddings.embed_query(row['review'])]
        )
    
    return {'statusCode': 200, 'body': 'ETL Complete'}
```

### Step 2: Schedule ETL with EventBridge
```
EventBridge Rule (Daily)
    ↓
Trigger Lambda ETL
    ↓
Process CSV
    ↓
Update Vector DB
```

### Step 3: Store Metadata in RDS
```sql
CREATE TABLE restaurant_reviews (
    id INT PRIMARY KEY,
    review TEXT,
    rating FLOAT,
    timestamp DATETIME,
    embedding_id VARCHAR(255)
);
```

## Data Flow Summary

```
CSV File (Local/S3)
    ↓
AWS Lambda/Glue ETL
    ↓
Transform & Embed
    ↓
ChromaDB (Vector Storage)
    ↓
ECS/App Runner Container (main.py)
    ↓
User asks question
    ↓
Semantic search in Vector DB
    ↓
LLM generates response
    ↓
CloudWatch Logs
```

## Key AWS Services for DE

| Service | Role | Purpose |
|---------|------|---------|
| **S3** | Data Lake | Store raw CSV files |
| **Lambda** | ETL Processing | Trigger data pipelines |
| **Glue** | ETL Orchestration | Complex data transformations |
| **RDS/DynamoDB** | Metadata Store | Store review metadata |
| **ECS** | Container Orchestration | Run main.py continuously |
| **ECR** | Container Registry | Store Docker images |
| **CloudWatch** | Monitoring | Logs and metrics |
| **EventBridge** | Scheduling | Trigger ETL on schedule |
| **VPC** | Networking | Secure internal communication |

## Next Steps

1. Set up S3 bucket for data
2. Create Lambda ETL function
3. Deploy Docker container to ECS
4. Configure vector DB (ChromaDB on RDS or self-hosted)
5. Set up CloudWatch for monitoring
6. Create EventBridge schedule for daily ETL
