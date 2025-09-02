# Detailed Setup Guide

This guide provides step-by-step instructions for setting up the Marketo to Snowflake ETL pipeline.

## 📋 Prerequisites Checklist

### System Requirements
- [ ] Docker Desktop installed and running
- [ ] Docker Compose v2.0+ available
- [ ] Python 3.8+ installed
- [ ] At least 8GB RAM available for Docker
- [ ] At least 10GB disk space available

### Accounts Required
- [ ] Snowflake account with appropriate privileges
- [ ] Git access to clone the repository

## 🔧 Step-by-Step Setup

### Step 1: Environment Preparation

```bash
# Clone the repository
git clone <your-repository-url>
cd Kafka-streaming-project

# Create Python virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Verify Python virtual environment is active
python --version
which python  # Should show path to venv/Scripts/python or venv/bin/python

# Upgrade pip to latest version
python -m pip install --upgrade pip

# Verify Docker is running
docker --version
docker-compose --version
```

### Step 2: Configure Environment Variables

```bash
# Copy the example environment file
cp .env.example .env

# Edit the environment file with your configuration
# Use your preferred text editor
notepad .env  # Windows
nano .env     # macOS/Linux
```

**Required Configuration:**
```env
# Snowflake Configuration - REQUIRED
SNOWFLAKE_ACCOUNT=your_account_identifier_here
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MARKETO_DB
SNOWFLAKE_SCHEMA=RAW_DATA

# Optional: Adjust batch sizes and intervals
BATCH_SIZE=1000
PROCESSING_INTERVAL=30
```

### Step 3: Start Infrastructure Services

```bash
# Start all Docker services
docker-compose up -d

# Wait for services to start (about 2-3 minutes)
# Check service status
docker-compose ps

# You should see all services as "Up"
# - kafka (running in KRaft mode - no ZooKeeper needed!)
# - schema-registry
# - kafka-connect
# - ksqldb-server
# - control-center
# - minio
# - redis
```

**Verify Services:**
```bash
# Check Kafka is ready
docker-compose logs kafka | grep "started (kafka.server.KafkaServer)"

# Check Schema Registry is ready
curl -s http://localhost:8081/subjects

# Check MinIO is ready
curl -s http://localhost:9000/minio/health/live
```

### Step 4: Install Python Dependencies

**Important**: Make sure your virtual environment is activated before installing dependencies:

```bash
# Verify virtual environment is active (should show (venv) prefix)
# If not active, run:
# Windows: venv\Scripts\activate
# macOS/Linux: source venv/bin/activate

# Install data generator dependencies
cd dataSource
pip install -r requirements.txt

# Install ETL pipeline dependencies  
cd ../scripts
pip install -r requirements.txt

# Return to project root
cd ..

# Verify installations
pip list | grep kafka
pip list | grep pandas
pip list | grep snowflake
```

### Step 5: Initialize Snowflake Schema

Before running the pipeline, ensure your Snowflake environment is ready:

```sql
-- Connect to Snowflake using your SQL client and run:

-- Create warehouse (if not exists)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH 
    WITH WAREHOUSE_SIZE = 'SMALL' 
    INITIALLY_SUSPENDED = FALSE 
    AUTO_SUSPEND = 300;

-- Create database
CREATE DATABASE IF NOT EXISTS MARKETO_DB;

-- Create schema  
CREATE SCHEMA IF NOT EXISTS MARKETO_DB.RAW_DATA;

-- Grant permissions (adjust role as needed)
GRANT ALL ON DATABASE MARKETO_DB TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA MARKETO_DB.RAW_DATA TO ROLE SYSADMIN;
```

### Step 6: Test Individual Components

Before running the full pipeline, test each component:

**Test 1: Data Generator**
```bash
cd dataSource
python marketo_data_generator.py
# Should output JSON data to console
# Press Ctrl+C to stop after seeing data
```

**Test 2: Kafka Producer**
```bash
cd scripts/producers
python marketo_producer.py
# Should show "Message sent successfully" logs
# Press Ctrl+C to stop after 30 seconds
```

**Test 3: Stream Processor**
```bash
# In a new terminal
cd scripts/processors  
python stream_processor.py
# Should show "Processing lead/activity" logs
```

**Test 4: Snowflake Consumer**
```bash
# In a new terminal
cd scripts/consumers
python snowflake_consumer.py
# Should show "Snowflake connection established"
```

### Step 7: Run Full Pipeline

Open **4 separate terminals** and run:

**Terminal 1 - Producer:**
```bash
cd scripts/producers
python marketo_producer.py
```

**Terminal 2 - Stream Processor:**
```bash
cd scripts/processors
python stream_processor.py
```

**Terminal 3 - Data Lake Sink:**
```bash
cd scripts/processors
python data_lake_sink.py
```

**Terminal 4 - Snowflake Consumer:**
```bash
cd scripts/consumers
python snowflake_consumer.py
```

## 🔍 Verification Steps

### 1. Check Kafka Topics
```bash
# List topics
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Should show:
# marketo.leads
# marketo.activities
# marketo.opportunities
# processed.leads  
# processed.activities
# snowflake.sink
```

### 2. Monitor Control Center
- Open http://localhost:9021
- Navigate to Topics
- Verify message throughput on all topics

### 3. Check MinIO Data Lake
- Open http://localhost:9001
- Login: minioadmin / minioadmin
- Navigate to `marketo-data-lake` bucket
- Verify Parquet files in `raw_data/` folder

### 4. Verify Snowflake Data
```sql
-- Connect to Snowflake
USE DATABASE MARKETO_DB;
USE SCHEMA RAW_DATA;

-- Check table row counts
SELECT COUNT(*) FROM leads;
SELECT COUNT(*) FROM activities;  
SELECT COUNT(*) FROM opportunities;

-- Sample data query
SELECT * FROM leads LIMIT 10;
```

### 5. Monitor Redis Cache
```bash
# Connect to Redis
docker-compose exec redis redis-cli

# Check keys
KEYS *

# Should see lead:* and activity:* keys
```

## ⚠️ Troubleshooting

### Issue: Docker services won't start
**Solution:**
```bash
# Check Docker resources
docker system df

# Clean up if needed
docker system prune -a

# Restart Docker Desktop
# Try starting services again
```

### Issue: Kafka connection refused
**Solution:**
```bash
# Wait longer for Kafka to fully start
docker-compose logs kafka

# Look for "started (kafka.server.KafkaServer)"
# If not found, restart Kafka:
docker-compose restart kafka
```

### Issue: Snowflake authentication failed
**Solution:**
1. Verify credentials in `.env` file
2. Test connection using Snowflake web UI
3. Check account identifier format:
   ```
   # Format: account_locator.region.cloud_provider
   # Example: xy12345.us-east-1.aws
   ```

### Issue: Python import errors
**Solution:**
```bash
# Ensure virtual environment is activated
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows

# Reinstall dependencies
pip install -r requirements.txt

# Check Python path
python -c "import sys; print(sys.path)"
```

### Issue: Memory/Performance problems
**Solution:**
```bash
# Reduce batch sizes in .env
BATCH_SIZE=500
PROCESSING_INTERVAL=60

# Increase Docker memory allocation
# Docker Desktop -> Settings -> Resources -> Memory
```

### Issue: Data not appearing in Snowflake
**Checklist:**
1. [ ] All 4 pipeline components running
2. [ ] Kafka topics have messages
3. [ ] No errors in consumer logs
4. [ ] Snowflake credentials correct
5. [ ] Database/schema permissions granted

## 📊 Performance Monitoring

### Key Metrics to Monitor

1. **Message Throughput**
   - Producer: messages/second sent to Kafka
   - Consumer Lag: messages waiting to be processed

2. **Processing Performance**
   - Stream Processor: enrichment latency
   - Batch Processing: records/batch, batch frequency

3. **Storage Performance**
   - MinIO: data lake write performance
   - Snowflake: insert/upsert performance

### Monitoring Commands

```bash
# Monitor Kafka consumer lag
docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group stream_processor_group --describe

# Monitor Docker resource usage
docker stats

# Monitor logs in real-time
docker-compose logs -f --tail=50 kafka
```

## 🚀 Next Steps

Once the pipeline is running successfully:

1. **Customize Data Generation**: Modify `marketo_data_generator.py` for your use case
2. **Add Monitoring**: Implement Prometheus/Grafana for metrics
3. **Scale Components**: Add more consumer instances for parallel processing
4. **Data Quality**: Add more sophisticated validation rules
5. **Production Deployment**: Configure security, SSL, and monitoring

## 📞 Getting Help

If you encounter issues not covered in this guide:

1. Check the main README.md for architecture details
2. Review Docker Compose logs for specific services
3. Verify all environment variables are correctly set
4. Test components individually before running full pipeline

For additional help, create an issue in the project repository with:
- Error messages and logs
- Your environment configuration (without credentials)
- Steps to reproduce the issue