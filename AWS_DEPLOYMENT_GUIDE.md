# AWS Deployment Guide for Pizza Bot AI

## Prerequisites
- AWS Account
- AWS CLI configured with credentials
- Docker installed locally
- ECR (Elastic Container Registry) access

## Step 1: Build Docker Image Locally
```bash
cd /Users/ramakanth/Desktop/"LOCAL AI AGENT"
docker build -t pizza-bot:latest .
```

## Step 2: Create AWS ECR Repository
```bash
aws ecr create-repository --repository-name pizza-bot --region us-east-1
```
Note: Replace `us-east-1` with your desired region

## Step 3: Get ECR Login Token
```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin YOUR_AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com
```
Replace `YOUR_AWS_ACCOUNT_ID` with your actual AWS Account ID

## Step 4: Tag Docker Image
```bash
docker tag pizza-bot:latest YOUR_AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/pizza-bot:latest
```

## Step 5: Push to ECR
```bash
docker push YOUR_AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/pizza-bot:latest
```

## Step 6: Deploy on AWS (Choose One Option)

### Option A: ECS (Elastic Container Service)
1. Go to AWS ECS Console
2. Create a new Task Definition
3. Select Docker image from ECR
4. Create a service and run the task

### Option B: EC2
1. Launch EC2 instance
2. Install Docker on instance
3. Pull image: `docker pull YOUR_AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/pizza-bot:latest`
4. Run: `docker run pizza-bot:latest`

### Option C: App Runner (Simplest)
1. Go to AWS App Runner
2. Create service
3. Select ECR repository
4. Configure and deploy

## Step 7: Commit and Push Code
```bash
git add Dockerfile .dockerignore
git commit -m "Add Docker configuration for AWS deployment"
git push origin feature/pizza-bot-improvements
```

## Environment Variables (if needed)
Create a `.env` file for sensitive data:
```
OLLAMA_BASE_URL=http://your-ollama-server:11434
```

## Monitoring
- CloudWatch Logs for container logs
- CloudWatch Metrics for performance
