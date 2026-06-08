"""
AWS Lambda ETL Function - Data Engineering Pipeline
Triggered by: S3 upload or EventBridge schedule
Purpose: Process restaurant reviews CSV and embed into Vector DB
"""

import json
import boto3
import pandas as pd
import chromadb
from io import BytesIO
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
s3_client = boto3.client('s3')
rds_client = boto3.client('rds')

# Constants
S3_BUCKET = 'pizza-bot-data-bucket'
S3_KEY = 'realistic_restaurant_reviews.csv'
VECTOR_DB_HOST = 'vector-db-endpoint.us-east-1.rds.amazonaws.com'


def lambda_handler(event, context):
    """
    Main Lambda handler function
    
    Event sources:
    - S3 upload event
    - EventBridge scheduled event
    """
    try:
        logger.info("Starting ETL pipeline...")
        
        # Step 1: Extract - Read CSV from S3
        logger.info("Step 1: Extracting data from S3...")
        df = extract_from_s3(S3_BUCKET, S3_KEY)
        logger.info(f"Extracted {len(df)} records")
        
        # Step 2: Transform - Clean and prepare data
        logger.info("Step 2: Transforming data...")
        df_transformed = transform_data(df)
        
        # Step 3: Load - Store in Vector DB
        logger.info("Step 3: Loading into Vector DB...")
        load_to_vector_db(df_transformed)
        
        # Step 4: Log metadata to RDS
        logger.info("Step 4: Logging metadata...")
        log_etl_metadata(len(df), 'SUCCESS')
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'ETL Pipeline Completed Successfully',
                'records_processed': len(df)
            })
        }
        
    except Exception as e:
        logger.error(f"ETL Pipeline Failed: {str(e)}")
        log_etl_metadata(0, f'FAILED: {str(e)}')
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }


def extract_from_s3(bucket, key):
    """Extract CSV from S3"""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(obj['Body'])
        return df
    except Exception as e:
        logger.error(f"Error reading S3 file: {e}")
        raise


def transform_data(df):
    """Transform and clean data"""
    try:
        # Data cleaning
        df['review'] = df['review'].str.strip()
        df = df[df['review'].notna()]
        df = df[df['review'].str.len() > 10]
        
        # Add metadata
        df['processed_at'] = pd.Timestamp.now()
        df['embedding_required'] = True
        
        logger.info(f"Cleaned data: {len(df)} valid records")
        return df
        
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise


def load_to_vector_db(df):
    """Load data into ChromaDB Vector Database"""
    try:
        # Connect to ChromaDB (can be local or remote)
        client = chromadb.HttpClient(host=VECTOR_DB_HOST, port=8000)
        
        # Get or create collection
        collection = client.get_or_create_collection(
            name="restaurant_reviews",
            metadata={"hnsw:space": "cosine"}
        )
        
        # Generate embeddings and add to collection
        for idx, row in df.iterrows():
            collection.add(
                ids=[f"review_{idx}"],
                documents=[row['review']],
                metadatas=[{
                    'rating': float(row.get('rating', 0)),
                    'restaurant': str(row.get('restaurant', 'Unknown')),
                    'date': str(row.get('date', '')),
                    'processed_at': str(row['processed_at'])
                }]
            )
        
        logger.info(f"Added {len(df)} documents to vector DB")
        
    except Exception as e:
        logger.error(f"Error loading to vector DB: {e}")
        raise


def log_etl_metadata(record_count, status):
    """Log ETL execution metadata to RDS"""
    try:
        # This would connect to RDS and log pipeline execution
        # For now, we'll log to CloudWatch
        logger.info(f"ETL Metadata - Records: {record_count}, Status: {status}")
        
        # Example RDS insert (requires DB connection):
        # INSERT INTO etl_pipeline_logs 
        # (execution_date, record_count, status, duration)
        # VALUES (NOW(), {record_count}, '{status}', '...')
        
    except Exception as e:
        logger.error(f"Error logging metadata: {e}")


# Local testing
if __name__ == "__main__":
    # Mock event for local testing
    mock_event = {
        'Records': [
            {
                's3': {
                    'bucket': {'name': S3_BUCKET},
                    'object': {'key': S3_KEY}
                }
            }
        ]
    }
    
    result = lambda_handler(mock_event, None)
    print(json.dumps(result, indent=2))
