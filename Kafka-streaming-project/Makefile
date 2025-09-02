# Makefile for Marketo to Snowflake ETL Pipeline

.PHONY: help setup start stop restart status clean logs install test

# Default target
help:
	@echo "Marketo to Snowflake ETL Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  setup        - Initial setup (copy .env, create venv, install dependencies)"
	@echo "  venv-setup   - Create Python virtual environment"
	@echo "  start        - Start all infrastructure and pipeline components"
	@echo "  stop         - Stop all components"
	@echo "  restart      - Restart all components"
	@echo "  status       - Show status of all components"
	@echo "  logs         - Show logs from all components"
	@echo "  install      - Install Python dependencies (requires active venv)"
	@echo "  test         - Run basic connectivity tests"
	@echo "  clean        - Clean up Docker volumes and containers"
	@echo "  clean-venv   - Remove Python virtual environment"
	@echo ""
	@echo "Infrastructure commands:"
	@echo "  infra-start  - Start only Docker infrastructure"
	@echo "  infra-stop   - Stop only Docker infrastructure"
	@echo "  infra-logs   - Show infrastructure logs"
	@echo ""
	@echo "Pipeline commands:"
	@echo "  pipeline-start - Start only Python pipeline components"
	@echo "  pipeline-stop  - Stop only Python pipeline components"

# Setup commands
setup:
	@echo "Setting up Marketo to Snowflake ETL Pipeline..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Created .env file from template. Please edit with your configuration."; \
	else \
		echo ".env file already exists."; \
	fi
	@make venv-setup
	@make install
	@echo "Setup complete! Edit .env file with your Snowflake credentials, then run 'make start'"

venv-setup:
	@echo "Setting up Python virtual environment..."
	@if [ ! -d venv ]; then \
		python -m venv venv; \
		echo "Virtual environment created at ./venv"; \
	else \
		echo "Virtual environment already exists."; \
	fi
	@echo "To activate: source venv/bin/activate (Linux/Mac) or venv\\Scripts\\activate (Windows)"

install:
	@echo "Installing Python dependencies..."
	@echo "Make sure your virtual environment is activated!"
	@echo "Windows: venv\\Scripts\\activate"  
	@echo "Linux/Mac: source venv/bin/activate"
	@if command -v python >/dev/null 2>&1; then \
		python -m pip install --upgrade pip; \
		cd dataSource && pip install -r requirements.txt; \
		cd ../scripts && pip install -r requirements.txt; \
		echo "Dependencies installed successfully"; \
	else \
		echo "Python not found. Please ensure Python is installed and virtual environment is activated."; \
		exit 1; \
	fi

# Infrastructure management
infra-start:
	@echo "Starting Docker infrastructure..."
	@docker-compose up -d
	@echo "Waiting for services to start..."
	@sleep 30
	@make status

infra-stop:
	@echo "Stopping Docker infrastructure..."
	@docker-compose down

infra-logs:
	@echo "Showing infrastructure logs..."
	@docker-compose logs -f --tail=50

# Pipeline management  
pipeline-start:
	@echo "Starting pipeline components..."
	@cd scripts && python run_pipeline.py

pipeline-stop:
	@echo "Pipeline components are managed by the orchestrator. Use Ctrl+C to stop."

# Main commands
start: infra-start
	@echo "Infrastructure started. To start the pipeline, run:"
	@echo "cd scripts && python run_pipeline.py"

stop: infra-stop
	@echo "All components stopped"

restart: stop start

status:
	@echo "=== Docker Infrastructure Status ==="
	@docker-compose ps
	@echo ""
	@echo "=== Service Health Checks ==="
	@echo -n "Kafka: "
	@curl -s http://localhost:9092 > /dev/null 2>&1 && echo "✓ Running" || echo "✗ Not responding"
	@echo -n "Schema Registry: "
	@curl -s http://localhost:8081/subjects > /dev/null 2>&1 && echo "✓ Running" || echo "✗ Not responding"
	@echo -n "Control Center: "
	@curl -s http://localhost:9021 > /dev/null 2>&1 && echo "✓ Running" || echo "✗ Not responding"
	@echo -n "MinIO: "
	@curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1 && echo "✓ Running" || echo "✗ Not responding"
	@echo -n "Redis: "
	@redis-cli -h localhost -p 6379 ping > /dev/null 2>&1 && echo "✓ Running" || echo "✗ Not responding"

logs:
	@echo "Showing logs from all components..."
	@docker-compose logs -f --tail=100

test:
	@echo "Running basic connectivity tests..."
	@echo "Testing Kafka connectivity..."
	@cd scripts && python -c "from kafka import KafkaProducer; p = KafkaProducer(bootstrap_servers=['localhost:9092']); print('✓ Kafka connection successful')"
	@echo "Testing Redis connectivity..."
	@cd scripts && python -c "import redis; r = redis.Redis(host='localhost', port=6379); r.ping(); print('✓ Redis connection successful')"
	@echo "Testing MinIO connectivity..."
	@cd scripts && python -c "from minio import Minio; client = Minio('localhost:9000', access_key='minioadmin', secret_key='minioadmin', secure=False); print('✓ MinIO connection successful')"
	@echo "All connectivity tests passed!"

# Cleanup commands
clean:
	@echo "Cleaning up Docker resources..."
	@docker-compose down -v
	@docker system prune -f
	@echo "Cleanup complete"

clean-venv:
	@echo "Removing Python virtual environment..."
	@if [ -d venv ]; then \
		rm -rf venv; \
		echo "Virtual environment removed"; \
	else \
		echo "No virtual environment found"; \
	fi

# Development commands
dev-setup:
	@echo "Setting up development environment..."
	@python -m venv venv
	@echo "Virtual environment created. Activate with:"
	@echo "  source venv/bin/activate  (Linux/Mac)"
	@echo "  venv\\Scripts\\activate     (Windows)"

# Monitoring commands
monitor:
	@echo "Opening monitoring dashboards..."
	@echo "Kafka Control Center: http://localhost:9021"
	@echo "MinIO Console: http://localhost:9001"
	@if command -v open >/dev/null 2>&1; then \
		open http://localhost:9021; \
		open http://localhost:9001; \
	elif command -v xdg-open >/dev/null 2>&1; then \
		xdg-open http://localhost:9021; \
		xdg-open http://localhost:9001; \
	else \
		echo "Please open the URLs manually"; \
	fi

# Data commands
create-topics:
	@echo "Creating Kafka topics..."
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic marketo.leads --partitions 3 --replication-factor 1 --if-not-exists
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic marketo.activities --partitions 3 --replication-factor 1 --if-not-exists
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic marketo.opportunities --partitions 3 --replication-factor 1 --if-not-exists
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic processed.leads --partitions 3 --replication-factor 1 --if-not-exists
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic processed.activities --partitions 3 --replication-factor 1 --if-not-exists
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic snowflake.sink --partitions 3 --replication-factor 1 --if-not-exists
	@echo "Topics created successfully"

list-topics:
	@echo "Listing Kafka topics..."
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Quick start command
quickstart: setup start create-topics
	@echo ""
	@echo "🚀 Quick start complete!"
	@echo ""
	@echo "Next steps:"
	@echo "1. Edit .env file with your Snowflake credentials"
	@echo "2. Run: cd scripts && python run_pipeline.py"
	@echo "3. Monitor: make monitor"
	@echo ""