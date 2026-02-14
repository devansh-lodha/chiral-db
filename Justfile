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
    @echo "Wiping existing data for a clean start..."
    docker compose down -v
    @echo "Starting Databases..."
    docker compose up -d
    @echo "Starting Chiral API & Simulation..."
    @pkill -f uvicorn || true
    @rm -f chiral.log simulation.log

    @# Start Chiral API (Port 8000)
    @PYTHONPATH=src nohup uv run uvicorn chiral.main:app --port 8000 > chiral.log 2>&1 &
    @echo "Chiral API started on :8000"

    @# Start Simulation (Port 8001)
    @nohup uv run uvicorn simulation_code:app --port 8001 > simulation.log 2>&1 &
    @echo "Simulation started on :8001"

    @echo "Waiting 5s for services to initialize..."
    @sleep 5

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

# View logs
logs:
    tail -f chiral.log simulation.log
