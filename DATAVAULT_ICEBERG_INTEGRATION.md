# Data Vault & Apache Iceberg Integration

## Overview

Data Vault and Iceberg provide advanced data warehouse capabilities for your pizza bot pipeline.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Complete Data Architecture                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Raw Data Layer (S3)                                            │
│  ├── realistic_restaurant_reviews.csv                           │
│  └── Real-time streaming events                                 │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │         AWS Glue ETL / Lambda                           │  │
│  │  - Extract & Transform                                  │  │
│  │  - Data Quality Checks                                  │  │
│  └──────────────────────────────────────────────────────────┘  │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │     Apache Iceberg (S3 + Glue Catalog)                  │  │
│  │  ├── Staging Layer (Raw data in Iceberg format)         │  │
│  │  ├── Data Vault Layer (Hub/Link/Satellite tables)       │  │
│  │  └── Mart Layer (Aggregated for AI)                     │  │
│  └──────────────────────────────────────────────────────────┘  │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Vector Database (ChromaDB / RDS pgvector)              │  │
│  │  - Embeddings from cleaned reviews                       │  │
│  │  - Metadata from Data Vault                              │  │
│  └──────────────────────────────────────────────────────────┘  │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  ECS Container (main.py)                                │  │
│  │  - Query vector DB                                       │  │
│  │  - LLM inference                                         │  │
│  │  - User interaction                                      │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Monitoring: CloudWatch, DataBrew, Athena                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Part 1: Apache Iceberg Integration

### What is Apache Iceberg?

Iceberg is an open table format that provides:
- **ACID transactions** - Reliable data updates
- **Schema evolution** - Add/modify columns without reprocessing
- **Time travel** - Query historical versions
- **Hidden partitioning** - Automatic partition management
- **Compatibility** - Works with Spark, Trino, Flink, Presto

### Architecture with Iceberg

```
S3 Bucket Structure:
pizza-bot-warehouse/
├── staging/
│   └── reviews_iceberg/
│       ├── metadata/
│       ├── data/
│       └── manifests/
├── datavault/
│   ├── hubs/
│   │   ├── hub_restaurant/
│   │   ├── hub_review/
│   │   └── hub_reviewer/
│   ├── links/
│   │   └── link_restaurant_review/
│   └── satellites/
│       ├── sat_restaurant_details/
│       └── sat_review_content/
└── marts/
    └── reviews_mart/
```

### Iceberg with AWS Glue

```python
# ETL using Spark + Iceberg
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergETL") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://pizza-bot-warehouse") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", 
            "org.apache.iceberg.aws.glue.GlueCatalog") \
    .getOrCreate()

# Read from S3 CSV
df = spark.read.csv("s3://pizza-bot-data/reviews.csv", header=True)

# Create Iceberg Table
df.writeTo("glue_catalog.reviews_db.raw_reviews") \
    .mode("overwrite") \
    .option("write-format", "parquet") \
    .create_or_replace()

# Time travel example - query from 1 hour ago
df_historical = spark.read \
    .option("as-of-timestamp", "2024-01-15 12:00:00") \
    .table("glue_catalog.reviews_db.raw_reviews")

# Schema evolution - add new column
spark.sql("""
    ALTER TABLE glue_catalog.reviews_db.raw_reviews 
    ADD COLUMN sentiment_score DOUBLE
""")
```

---

## Part 2: Data Vault Architecture

### Data Vault Concepts

```
Hub Tables (Unique business keys)
├── hub_restaurant
│   └── restaurant_key, restaurant_id, load_date, source
├── hub_review
│   └── review_key, review_id, load_date, source
└── hub_reviewer
    └── reviewer_key, reviewer_id, load_date, source

Link Tables (Relationships)
├── link_restaurant_review
│   └── link_key, restaurant_key, review_key, load_date, source
└── link_restaurant_reviewer
    └── link_key, restaurant_key, reviewer_key, load_date, source

Satellite Tables (Attributes)
├── sat_restaurant_details
│   ├── restaurant_key, load_date, name, cuisine, address, rating
│   └── load_end_date (slowly changing dimension)
├── sat_review_content
│   ├── review_key, load_date, review_text, rating, helpful_count
│   └── load_end_date
└── sat_reviewer_profile
    ├── reviewer_key, load_date, reviewer_name, join_date
    └── load_end_date
```

### Data Vault SQL Implementation

```sql
-- Hub: Restaurant
CREATE TABLE IF NOT EXISTS hub_restaurant (
    restaurant_key BIGINT PRIMARY KEY,
    restaurant_id VARCHAR(255) UNIQUE,
    load_date TIMESTAMP,
    source_system VARCHAR(100)
);

-- Hub: Review
CREATE TABLE IF NOT EXISTS hub_review (
    review_key BIGINT PRIMARY KEY,
    review_id VARCHAR(255) UNIQUE,
    load_date TIMESTAMP,
    source_system VARCHAR(100)
);

-- Link: Restaurant has Review
CREATE TABLE IF NOT EXISTS link_restaurant_review (
    link_key BIGINT PRIMARY KEY,
    restaurant_key BIGINT REFERENCES hub_restaurant,
    review_key BIGINT REFERENCES hub_review,
    load_date TIMESTAMP,
    source_system VARCHAR(100)
);

-- Satellite: Restaurant Details (SCD Type 2)
CREATE TABLE IF NOT EXISTS sat_restaurant_details (
    restaurant_key BIGINT NOT NULL REFERENCES hub_restaurant,
    load_date TIMESTAMP NOT NULL,
    load_end_date TIMESTAMP,
    restaurant_name VARCHAR(500),
    cuisine_type VARCHAR(100),
    address VARCHAR(500),
    rating DECIMAL(3,2),
    is_current BOOLEAN,
    source_system VARCHAR(100),
    PRIMARY KEY (restaurant_key, load_date)
);

-- Satellite: Review Content
CREATE TABLE IF NOT EXISTS sat_review_content (
    review_key BIGINT NOT NULL REFERENCES hub_review,
    load_date TIMESTAMP NOT NULL,
    load_end_date TIMESTAMP,
    review_text TEXT,
    rating INT,
    helpful_count INT,
    is_current BOOLEAN,
    source_system VARCHAR(100),
    PRIMARY KEY (review_key, load_date)
);
```

### ETL Pipeline: CSV to Data Vault

```python
import pandas as pd
from datetime import datetime
import hashlib

class DataVaultETL:
    def __init__(self, s3_client, glue_context):
        self.s3 = s3_client
        self.glue = glue_context
        self.load_date = datetime.now()
        
    def extract_raw_data(self):
        """Extract data from S3"""
        df = pd.read_csv('s3://pizza-bot-data/reviews.csv')
        return df
    
    def create_hubs(self, df):
        """Create hub tables with business keys"""
        
        # Hub Restaurant
        hub_restaurant = df[['restaurant_id', 'restaurant_name']].drop_duplicates()
        hub_restaurant['restaurant_key'] = hub_restaurant['restaurant_id'].apply(
            lambda x: int(hashlib.md5(str(x).encode()).hexdigest(), 16) % 10**8
        )
        hub_restaurant['load_date'] = self.load_date
        hub_restaurant['source_system'] = 'CSV_IMPORT'
        
        # Hub Review
        hub_review = df[['review_id']].drop_duplicates()
        hub_review['review_key'] = hub_review['review_id'].apply(
            lambda x: int(hashlib.md5(str(x).encode()).hexdigest(), 16) % 10**8
        )
        hub_review['load_date'] = self.load_date
        hub_review['source_system'] = 'CSV_IMPORT'
        
        return hub_restaurant, hub_review
    
    def create_links(self, df, hub_restaurant, hub_review):
        """Create link tables for relationships"""
        
        link_df = df[['restaurant_id', 'review_id']].drop_duplicates()
        
        # Merge to get keys
        link_df = link_df.merge(hub_restaurant[['restaurant_id', 'restaurant_key']], 
                               on='restaurant_id')
        link_df = link_df.merge(hub_review[['review_id', 'review_key']], 
                               on='review_id')
        
        link_df['link_key'] = (link_df['restaurant_key'].astype(str) + 
                              link_df['review_key'].astype(str)).apply(
                                  lambda x: int(hashlib.md5(x.encode()).hexdigest(), 16) % 10**8
                              )
        link_df['load_date'] = self.load_date
        link_df['source_system'] = 'CSV_IMPORT'
        
        return link_df[['link_key', 'restaurant_key', 'review_key', 
                       'load_date', 'source_system']]
    
    def create_satellites(self, df, hub_restaurant, hub_review):
        """Create satellite tables for attributes"""
        
        # Satellite: Restaurant Details
        sat_restaurant = df[['restaurant_id', 'restaurant_name', 'cuisine', 
                            'address', 'rating']].drop_duplicates()
        sat_restaurant = sat_restaurant.merge(
            hub_restaurant[['restaurant_id', 'restaurant_key']], 
            on='restaurant_id'
        )
        sat_restaurant['load_date'] = self.load_date
        sat_restaurant['load_end_date'] = None
        sat_restaurant['is_current'] = True
        sat_restaurant['source_system'] = 'CSV_IMPORT'
        
        # Satellite: Review Content
        sat_review = df[['review_id', 'review_text', 'rating']].drop_duplicates()
        sat_review = sat_review.merge(
            hub_review[['review_id', 'review_key']], 
            on='review_id'
        )
        sat_review['load_date'] = self.load_date
        sat_review['load_end_date'] = None
        sat_review['is_current'] = True
        sat_review['source_system'] = 'CSV_IMPORT'
        
        return sat_restaurant, sat_review
    
    def load_to_warehouse(self, hubs, links, satellites):
        """Load all tables to Iceberg"""
        
        for table_name, df in hubs.items():
            self._write_iceberg(df, f"datavault.{table_name}")
        
        for table_name, df in links.items():
            self._write_iceberg(df, f"datavault.{table_name}")
        
        for table_name, df in satellites.items():
            self._write_iceberg(df, f"datavault.{table_name}")
    
    def _write_iceberg(self, df, table_path):
        """Write DataFrame to Iceberg table"""
        # Convert Pandas to Spark DataFrame
        spark_df = self.glue.createDataFrame(df)
        spark_df.writeTo(table_path).mode("append").create_or_replace()
```

---

## Part 3: Complete Pipeline Integration

### Data Flow with Data Vault + Iceberg

```
1. Raw CSV in S3
   ↓
2. AWS Glue ETL (Python/Spark)
   ├── Extract raw data
   ├── Generate business keys (MD5 hash)
   ├── Create Hub/Link/Satellite tables
   └── Write to Iceberg format
   ↓
3. Iceberg Tables on S3 (with Glue Catalog)
   ├── Hub tables (deduplicated business entities)
   ├── Link tables (relationships)
   └── Satellite tables (attributes with history)
   ↓
4. Data Mart (Analytics layer)
   ├── Join Hubs + Links + Satellites
   ├── Flatten into denormalized view
   └── Create embedding-ready dataset
   ↓
5. Vector Database Population
   ├── Query mart tables
   ├── Generate embeddings
   └── Store in ChromaDB/pgvector
   ↓
6. AI Application (main.py)
   ├── User query
   ├── Semantic search in vector DB
   ├── Retrieve context from data vault
   └── LLM inference
```

### Benefits

| Feature | Benefit |
|---------|---------|
| **Data Vault Hubs** | Single source of truth for business entities |
| **Satellites** | Track historical changes (SCD Type 2) |
| **Iceberg** | ACID transactions, schema evolution, time travel |
| **S3 + Glue Catalog** | Scalable, serverless data warehouse |
| **Vector DB** | Fast semantic search for AI queries |
| **CloudWatch** | Monitor data quality and pipeline health |

### Query Example: AI-Ready View

```sql
-- Create denormalized view for AI embeddings
CREATE TABLE glue_catalog.analytics.reviews_for_embeddings AS
SELECT 
    hr.restaurant_key,
    sr.review_key,
    srd.restaurant_name,
    srd.cuisine_type,
    srd.rating as restaurant_rating,
    src.review_text,
    src.rating as review_rating,
    sr.load_date,
    -- AI-ready features
    CONCAT(srd.restaurant_name, ': ', src.review_text) as embedding_text
FROM hub_restaurant hr
JOIN sat_restaurant_details srd 
    ON hr.restaurant_key = srd.restaurant_key 
    AND srd.is_current = TRUE
JOIN link_restaurant_review lrr 
    ON hr.restaurant_key = lrr.restaurant_key
JOIN hub_review sr 
    ON lrr.review_key = sr.review_key
JOIN sat_review_content src 
    ON sr.review_key = src.review_key 
    AND src.is_current = TRUE
WHERE srd.load_end_date IS NULL;

-- Now embeddings are generated from this view
```

### AWS Glue Job Configuration

```python
# glue_datavault_job.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Iceberg
spark.sql("CREATE NAMESPACE IF NOT EXISTS glue_catalog.datavault")

# Read raw data
raw_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://pizza-bot-data/reviews.csv"]},
    transformation_ctx="raw_data"
)

# Convert to Spark DataFrame
df = raw_data.toDF()

# Initialize Data Vault ETL
etl = DataVaultETL(glueContext.spark_session)
hubs = etl.create_hubs(df)
links = etl.create_links(df, hubs[0], hubs[1])
satellites = etl.create_satellites(df, hubs[0], hubs[1])

# Load to Iceberg
etl.load_to_warehouse(hubs, links, satellites)

job.commit()
```

---

## Summary

**Apache Iceberg** provides:
- ✅ ACID compliance for data consistency
- ✅ Time travel queries for auditing
- ✅ Schema evolution without reprocessing
- ✅ Efficient partitioning

**Data Vault** provides:
- ✅ Historical tracking (Slowly Changing Dimensions)
- ✅ Flexible schema for changes
- ✅ Separation of concerns (Hub/Link/Satellite)
- ✅ Business key-based architecture

**Together with your Pizza Bot**:
- Raw CSV → Glue ETL → Iceberg DV Tables → Vector DB → AI App
- Full data lineage and audit trail
- Scalable, maintainable data platform
