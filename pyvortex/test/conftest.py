import logging
import pathlib
import subprocess

logging.basicConfig(level=logging.DEBUG)


def pytest_sessionstart():
    """Pytest plugin to trigger maturin builds before running tests."""
    working_dir = pathlib.Path(__file__).parent.parent
    subprocess.check_call(["maturin", "develop", "-v", "--skip-install", "--locked", "--frozen"], cwd=working_dir)
