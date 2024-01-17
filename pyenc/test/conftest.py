import pathlib
import subprocess


def pytest_sessionstart():
    """Pytest plugin to trigger maturin builds before running tests."""
    working_dir = pathlib.Path(__file__).parent.parent
    subprocess.check_call(["maturin", "develop", "--skip-install"], cwd=working_dir)
