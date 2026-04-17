# Copyright (c) 2026 Chiral Contributors
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""Cross-platform service management script."""

import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def kill_process_on_port(port: int) -> None:
    """Kill processes listening on the specified port (cross-platform)."""
    if sys.platform == "win32":
        try:
            output = subprocess.check_output(["netstat", "-ano", "-p", "tcp"], text=True)
            for line in output.splitlines():
                if f":{port}" in line and "LISTENING" in line:
                    pid = line.strip().split()[-1]
                    print(f"Killing process {pid} on port {port}...")
                    subprocess.run(["taskkill", "/F", "/PID", pid], check=False)
        except Exception:
            print(f"Error killing process on port {port}")
    else:
        try:
            # Using lsof to find PID
            output = subprocess.check_output(["lsof", "-t", f"-i:{port}"], text=True)
            for pid in output.splitlines():
                print(f"Killing process {pid} on port {port}...")
                os.kill(int(pid), signal.SIGKILL)
        except subprocess.CalledProcessError:
            pass  # No process on port
        except Exception:
            print(f"Error killing process on port {port}")


def cleanup() -> None:
    """Remove logs and temporary files."""
    files_to_remove = ["chiral.log", "simulation.log", ".coverage", "coverage.xml"]
    for f in files_to_remove:
        path = Path(f)
        if path.exists():
            path.unlink()

    # Remove pycache
    for p in Path().rglob("__pycache__"):
        try:
            shutil.rmtree(p)
        except Exception:
            print(f"Error removing {p}")


def start_service(command: list[str], log_file: str, env: dict[str, str] | None = None) -> subprocess.Popen:
    """Start a service in the background and redirect output to log file."""
    log_path = Path(log_file)
    f = log_path.open("a")

    # Use subprocess.Popen for background execution
    return subprocess.Popen(
        command,
        stdout=f,
        stderr=subprocess.STDOUT,
        env={**os.environ, **(env or {})},
        start_new_session=sys.platform != "win32",
    )


def wait_for_url(url: str, timeout: int = 30, label: str = "Service") -> bool:
    """Wait for a URL to return a 200 OK status."""
    print(f"Waiting for {label} at {url}...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with urllib.request.urlopen(url) as response:
                if response.getcode() == 200:
                    print(f"   {label} is ready.")
                    return True
        except Exception:
            print(f"   {label} not ready yet...")
        time.sleep(1)
    print(f"Timeout waiting for {label}.")
    return False


def wait_for_db(timeout: int = 30) -> bool:
    """Wait for databases to be ready using verify_connections.py."""
    print("Waiting for Databases...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = subprocess.run([sys.executable, "verify_connections.py"], capture_output=True, text=True, check=False)
        if result.returncode == 0:
            print("   Databases are ready.")
            return True
        time.sleep(1)
    print("Timeout waiting for Databases.")
    return False


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""

    if cmd == "stop":
        kill_process_on_port(8000)
        kill_process_on_port(8001)
    elif cmd == "wait-db":
        if not wait_for_db():
            sys.exit(1)
    elif cmd == "cleanup":
        cleanup()
    elif cmd == "demo-start":
        # Ensure ports are free
        kill_process_on_port(8000)
        kill_process_on_port(8001)
        cleanup()

        # Start API
        print("Starting Chiral API on :8000...")
        start_service(
            [sys.executable, "-m", "uvicorn", "chiral.main:app", "--port", "8000"],
            "chiral.log",
            env={"PYTHONPATH": "src"},
        )

        # Start Simulation
        print("Starting Simulation on :8001...")
        start_service([sys.executable, "-m", "uvicorn", "simulation_code:app", "--port", "8001"], "simulation.log")
    elif cmd == "wait":
        if not wait_for_db():
            sys.exit(1)
        if not wait_for_url("http://127.0.0.1:8000/api/health", label="Chiral API"):
            sys.exit(1)
        if not wait_for_url("http://127.0.0.1:8001/health", label="Simulation"):
            sys.exit(1)
    else:
        print("Unknown command")
        sys.exit(1)
