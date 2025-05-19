import pytest
import subprocess
import sys
from pathlib import Path

# Constants used by the manage_db_container fixture
# These paths are relative to the project root, assuming pytest is run from there.
PROJECT_ROOT = Path(__file__).parent.parent.parent
SYSTEM_TEST_DIR = Path("tests/system")
DOCKER_COMPOSE_FILE = SYSTEM_TEST_DIR / "docker-compose.yml"
COMBINE_SCRIPT = SYSTEM_TEST_DIR / "combine_sql_files.py"

@pytest.fixture(scope="session", autouse=True)
def manage_db_container():
    """Starts and stops the PostgreSQL test container using Docker Compose."""
    compose_file_path_relative_to_cwd = DOCKER_COMPOSE_FILE.name
    print(f"\nManaging DB container using {compose_file_path_relative_to_cwd} from {SYSTEM_TEST_DIR}...")
    
    # Run compose commands from the directory containing the compose file
    # This is PROJECT_ROOT / SYSTEM_TEST_DIR
    cwd_for_docker_compose = PROJECT_ROOT / SYSTEM_TEST_DIR
    
    # First, run the script to combine SQL files
    print(f"Combining SQL files using {COMBINE_SCRIPT}...")
    combine_script_abs_path = PROJECT_ROOT / COMBINE_SCRIPT
    combine_cmd = [sys.executable, str(combine_script_abs_path)]
    combine_result = subprocess.run(combine_cmd, check=True, capture_output=True, text=True, cwd=PROJECT_ROOT)
    print("Combine script stdout:", combine_result.stdout)
    print("Combine script stderr:", combine_result.stderr)

    compose_cmd_base = ["docker", "compose", "-f", compose_file_path_relative_to_cwd]

    try:
        print(f"Attempting to run Docker Compose commands from: {cwd_for_docker_compose}")
        # Ensure clean slate
        print("Running docker compose down...")
        down_initial_result = subprocess.run(
            compose_cmd_base + ["down", "--volumes", "--remove-orphans"],
            check=False, capture_output=True, text=True, cwd=cwd_for_docker_compose
        )
        print(f"Initial 'docker compose down' stdout: {down_initial_result.stdout}")
        print(f"Initial 'docker compose down' stderr: {down_initial_result.stderr}")

        # Start the container
        print("Running docker compose up...")
        up_result = subprocess.run(
            compose_cmd_base + ["up", "-d", "--wait"], # --wait uses the healthcheck
            check=True, capture_output=True, text=True, cwd=cwd_for_docker_compose
        )
        print("Docker Compose Up Output:", up_result.stdout)
        if up_result.returncode != 0:
             print("Docker Compose Up Error:", up_result.stderr)
             # Explicitly raise to ensure test run stops
             raise RuntimeError(f"Docker compose up failed with exit code {up_result.returncode}: {up_result.stderr}")

        print("PostgreSQL container started and reported healthy.")
        yield # Tests run here
    except FileNotFoundError as e:
        print(f"Error: docker command not found. Is Docker installed and in PATH? Details: {e}")
        raise
    except subprocess.CalledProcessError as e:
        print(f"Error during docker compose execution: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred in manage_db_container: {e}")
        raise
    finally:
        print("\nStopping PostgreSQL container...")
        down_final_result = subprocess.run(
            compose_cmd_base + ["down", "--volumes", "--remove-orphans"],
            check=False, capture_output=True, text=True, cwd=cwd_for_docker_compose
        )
        print(f"Final 'docker compose down' stdout: {down_final_result.stdout}")
        print(f"Final 'docker compose down' stderr: {down_final_result.stderr}") 