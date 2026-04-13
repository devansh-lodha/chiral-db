# Install dependencies and setup environment
setup:
    uv sync --all-extras --dev
    uv run pre-commit install

# Run all checks (Format, Lint, Type, Test)
verify: format lint type test
    @echo "Checks passed."

# Run ACID integration tests against PostgreSQL
test-acid:
    docker compose up -d postgres
    PYTHONPATH=src uv run pytest tests/test_acid_properties.py -v

# Run the hybrid database performance benchmark
benchmark SESSION_ID SIZE='25' WORKLOAD='all':
    uv run python scripts/performance_benchmark.py --session-id {{SESSION_ID}} --size {{SIZE}} --workload {{WORKLOAD}}

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

# Build and run webapp dashboard in Docker
webapp:
    @echo "Building and starting dashboard container..."
    docker compose --profile webapp up -d --build dashboard
    @echo "Dashboard is running at: http://localhost:5173"

# Stop database containers
down:
    docker compose down

# Stop only the webapp dashboard container
webapp-stop:
    docker compose --profile webapp stop dashboard
    @echo "Dashboard stopped."

# Clean temporary files
clean:
    uv run python scripts/manage.py cleanup

# -------------------------------------------------------------------------
# TA Demo Commands
# -------------------------------------------------------------------------

# Run the full end-to-end demo
demo:
    @uv run python -c "from pathlib import Path; import sys; print('Error: .env not found') or sys.exit(1) if not Path('.env').exists() else None"

    @echo "Cleaning up old instances..."
    docker compose down -v
    @uv run python scripts/manage.py stop

    @echo "Checking for remaining port conflicts..."
    @uv run python check_ports.py || (echo "ERROR: Ports occupied. Stop local DBs." && exit 1)

    @echo "Starting Databases..."
    docker compose up -d

    @echo "Starting Chiral API & Simulation..."
    @uv run python scripts/manage.py demo-start

    @echo "Waiting for services to initialize..."
    @uv run python scripts/manage.py wait

    @echo "Running Data Feeder..."
    @uv run python feed_data.py

    @echo "Ingestion Complete. Waiting for background workers to finish..."
    @uv run python -c "import time; time.sleep(10)"

    @echo "Running Verification Report..."
    @uv run python verify_assignment.py

    @echo "Demo Complete. Servers are running in background. Run 'just stop' to kill them."

# Run full demo + formatted metadata and 5 example queries
demo2:
    @uv run python -c "from pathlib import Path; import sys; print('Error: .env not found') or sys.exit(1) if not Path('.env').exists() else None"

    @echo "Cleaning up old instances..."
    docker compose down -v
    @uv run python scripts/manage.py stop

    @echo "Checking for remaining port conflicts..."
    @uv run python check_ports.py || (echo "ERROR: Ports occupied. Stop local DBs." && exit 1)

    @echo "Starting Databases..."
    docker compose up -d

    @echo "Starting Chiral API & Simulation..."
    @uv run python scripts/manage.py demo-start

    @echo "Waiting for services to initialize..."
    @uv run python scripts/manage.py wait

    @echo "Running Data Feeder (1000 records)..."
    @uv run python feed_data2.py

    @echo "Waiting for background workers to finish..."
    @uv run python -c "import time; time.sleep(10)"

    @echo "Running DEMO2 showcase (formatted metadata + 5 example queries)..."
    @uv run python demo2.py

    @echo "Demo2 Complete. Servers are running in background. Run 'just stop' to kill them."

# Stop the background servers
stop:
    @uv run python scripts/manage.py stop
    @echo "Servers stopped."

# Force kill processes on ports 8000/8001
stop-ports: stop

# View logs
logs:
    docker compose logs -f
