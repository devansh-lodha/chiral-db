set shell := ["bash", "-c"]

# Install dependencies and setup environment
setup:
    uv sync --all-extras --dev
    uv run pre-commit install

# Run all checks (Format, Lint, Type, Test)
verify: format lint type test
    @echo "Checks passed."

# Format code
format:
    uv run ruff format .

# Lint code
lint:
    uv run ruff check . --fix

# Type check
type:
    uv run ty check

# Run tests
test:
    uv run pytest --cov=src --cov-report=term-missing

# Start database containers
up:
    docker compose up -d

# Stop database containers
down:
    docker compose down

# Clean temporary files
clean:
    rm -rf .ruff_cache .pytest_cache .coverage htmlcov dist build
    find . -type d -name "__pycache__" -exec rm -rf {} +
    rm -f chiral.log simulation.log

# -------------------------------------------------------------------------
# TA Demo Commands
# -------------------------------------------------------------------------

# Run the full end-to-end demo
demo:
    @if [ ! -f .env ]; then echo "Error: .env file not found. Please run 'cp .env.example .env'"; exit 1; fi
    
    @echo "Cleaning up old instances..."
    docker compose down -v
    @bash -c "pkill -f '[u]vicorn' 2>/dev/null || true"
    @rm -f chiral.log simulation.log

    @echo "Checking for remaining port conflicts..."
    @uv run python check_ports.py || (echo "ERROR: Ports occupied. Run 'just stop-ports' or stop local DBs." && exit 1)
    
    @echo "Starting Databases..."
    docker compose up -d
    docker compose up -d
    @echo "Starting Chiral API & Simulation..."
    @bash -c "pkill -f '[u]vicorn' 2>/dev/null || true"
    @rm -f chiral.log simulation.log

    @# Start Chiral API (Port 8000)
    @PYTHONPATH=src nohup uv run uvicorn chiral.main:app --port 8000 > chiral.log 2>&1 &
    @echo "Chiral API started on :8000"

    @# Start Simulation (Port 8001)
    @nohup uv run uvicorn simulation_code:app --port 8001 > simulation.log 2>&1 &
    @echo "Simulation started on :8001"

    @echo "Waiting for services to initialize..."
    
    @echo "1. Waiting for Databases..."
    @bash -c "count=0; until uv run python verify_connections.py >/dev/null 2>&1; do sleep 1; count=\$((count+1)); if [ \$count -ge 30 ]; then echo 'Timeout waiting for DBs'; exit 1; fi; done"
    @echo "   Databases are ready."

    @echo "2. Waiting for Chiral API..."
    @bash -c "count=0; until curl -s http://127.0.0.1:8000/ > /dev/null; do sleep 1; count=\$((count+1)); if [ \$count -ge 30 ]; then echo 'Timeout waiting for API'; exit 1; fi; done"
    @echo "   Chiral API is ready."

    @echo "3. Waiting for Simulation..."
    @bash -c "until curl -s http://127.0.0.1:8001/health > /dev/null; do sleep 1; done"
    @echo "   Simulation is ready."

    @echo "Running Data Feeder..."
    @uv run python feed_data.py

    @echo "Ingestion Complete. Waiting for background workers to finish..."
    @sleep 10

    @echo "Running Verification Report..."
    @uv run python verify_assignment.py

    @echo "Demo Complete. Servers are running in background. Run 'just stop' to kill them."

# Stop the background servers
stop:
    pkill -f uvicorn || true
    @echo "Servers stopped."

# Force kill processes on ports 8000/8001
stop-ports:
    @echo "Attempting to free ports 8000 and 8001..."
    @lsof -ti :8000 | xargs kill -9 2>/dev/null || true
    @lsof -ti :8001 | xargs kill -9 2>/dev/null || true
    @echo "Ports 8000/8001 freed."

# View logs
logs:
    tail -f chiral.log simulation.log
