"""
AWS Infrastructure as Code (IaC) Setup
Using AWS CloudFormation or Terraform-like structure
"""

# AWS ECS Task Definition for main.py
ECS_TASK_DEFINITION = {
    "family": "pizza-bot-app",
    "networkMode": "awsvpc",
    "requiresCompatibilities": ["FARGATE"],
    "cpu": "512",
    "memory": "1024",
    "containerDefinitions": [
        {
            "name": "pizza-bot-container",
            "image": "YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/pizza-bot:latest",
            "portMappings": [
                {
                    "containerPort": 8000,
                    "hostPort": 8000,
                    "protocol": "tcp"
                }
            ],
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": "/ecs/pizza-bot",
                    "awslogs-region": "us-east-1",
                    "awslogs-stream-prefix": "ecs"
                }
            },
            "environment": [
                {
                    "name": "VECTOR_DB_HOST",
                    "value": "vector-db.us-east-1.rds.amazonaws.com"
                },
                {
                    "name": "OLLAMA_HOST",
                    "value": "http://ollama-service:11434"
                }
            ],
            "mountPoints": [
                {
                    "containerPath": "/app/chrome_langchain_db",
                    "sourceVolume": "vector-db-volume",
                    "readOnly": False
                }
            ]
        }
    ],
    "volumes": [
        {
            "name": "vector-db-volume",
            "efsVolumeConfiguration": {
                "fileSystemId": "fs-1234567890",
                "transitEncryption": "ENABLED"
            }
        }
    ]
}

# Lambda ETL Function Configuration
LAMBDA_ETL_CONFIG = {
    "FunctionName": "pizza-bot-etl",
    "Runtime": "python3.9",
    "Handler": "lambda_etl.lambda_handler",
    "Timeout": 300,
    "MemorySize": 1024,
    "Environment": {
        "Variables": {
            "S3_BUCKET": "pizza-bot-data-bucket",
            "VECTOR_DB_HOST": "vector-db.us-east-1.rds.amazonaws.com"
        }
    },
    "VpcConfig": {
        "SubnetIds": ["subnet-12345"],
        "SecurityGroupIds": ["sg-12345"]
    }
}

# EventBridge Rule for Daily ETL Trigger
EVENTBRIDGE_RULE = {
    "Name": "pizza-bot-daily-etl",
    "ScheduleExpression": "cron(0 2 * * ? *)",  # 2 AM UTC daily
    "State": "ENABLED",
    "Targets": [
        {
            "Arn": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:pizza-bot-etl",
            "RoleArn": "arn:aws:iam::ACCOUNT_ID:role/eventbridge-lambda-role"
        }
    ]
}

# S3 Bucket Configuration
S3_BUCKET_CONFIG = {
    "Bucket": "pizza-bot-data-bucket",
    "VersioningConfiguration": {
        "Status": "Enabled"
    },
    "LifecycleConfiguration": {
        "Rules": [
            {
                "Id": "archive-old-data",
                "Status": "Enabled",
                "Transitions": [
                    {
                        "Days": 90,
                        "StorageClass": "GLACIER"
                    }
                ]
            }
        ]
    },
    "PublicAccessBlockConfiguration": {
        "BlockPublicAcls": True,
        "BlockPublicPolicy": True,
        "IgnorePublicAcls": True,
        "RestrictPublicBuckets": True
    }
}

# RDS Configuration for Metadata
RDS_CONFIG = {
    "DBInstanceIdentifier": "pizza-bot-metadata",
    "DBInstanceClass": "db.t3.micro",
    "Engine": "postgres",
    "MasterUsername": "admin",
    "AllocatedStorage": "20",
    "StorageType": "gp2",
    "BackupRetentionPeriod": 7,
    "MultiAZ": False,
    "EnableIAMDatabaseAuthentication": True
}

# CloudWatch Log Group
CLOUDWATCH_LOG_GROUP = {
    "logGroupName": "/ecs/pizza-bot",
    "retentionInDays": 7
}

# VPC & Security Group
SECURITY_GROUP = {
    "GroupName": "pizza-bot-sg",
    "Description": "Security group for pizza bot application",
    "IpPermissions": [
        {
            "IpProtocol": "tcp",
            "FromPort": 8000,
            "ToPort": 8000,
            "IpRanges": [{"CidrIp": "10.0.0.0/8"}]
        },
        {
            "IpProtocol": "tcp",
            "FromPort": 11434,
            "ToPort": 11434,
            "IpRanges": [{"CidrIp": "10.0.0.0/8"}]
        }
    ]
}
