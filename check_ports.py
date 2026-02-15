# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Check if required ports are available."""

import logging
import socket
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def load_env(env_path: str = ".env") -> dict[str, str]:
    """Load environment variables from .env file."""
    path = Path(env_path)
    if not path.exists():
        return {}

    env_vars = {}
    with path.open() as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                env_vars[key.strip()] = value.strip()
    return env_vars


def check_port(port: int) -> bool:
    """Check if a port is in use."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    result = sock.connect_ex(("127.0.0.1", port))
    sock.close()
    return result == 0


def main() -> None:
    """Run the port check."""
    env = load_env()

    # Define ports to check
    api_port = 8000
    sim_port = 8001

    pg_port = int(env.get("POSTGRES_PORT", "5432"))
    mongo_port = int(env.get("MONGO_PORT", "27017"))

    errors = []

    # Check Application Ports
    if check_port(api_port):
        errors.append(f"Port {api_port} is busy (Chiral API).")

    if check_port(sim_port):
        errors.append(f"Port {sim_port} is busy (Simulation Server).")

    # Check Database Ports (Host binding)
    # Note: If running in Docker, these ports on localhost must be free for binding
    if check_port(pg_port):
        errors.append(f"Port {pg_port} is busy (PostgreSQL). Local DB running?")

    if check_port(mongo_port):
        errors.append(f"Port {mongo_port} is busy (MongoDB). Local DB running?")

    if errors:
        logger.error("\nERROR: Port conflicts detected!")
        for error in errors:
            logger.error(" - %s", error)

        logger.info("\nSUGGESTIONS:")
        logger.info("1. If these are old instances of Chiral, run: 'just stop-ports'")
        logger.info("2. If these are other services (e.g., local Postgres), stop them manually.")
        logger.info("3. Or configure different ports in .env (POSTGRES_PORT, MONGO_PORT).")
        sys.exit(1)

    logger.info("Port check passed. All ports available.")
    sys.exit(0)


if __name__ == "__main__":
    main()
