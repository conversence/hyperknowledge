import asyncio

import pytest

@pytest.fixture(scope="session")
def ini_file():
    from configparser import ConfigParser
    from pathlib import Path
    CONFIG_FILE = Path('config.ini')
    assert CONFIG_FILE.exists, "Please run initial_setup"
    ini_file = ConfigParser()
    with CONFIG_FILE.open() as f:
        ini_file.read_file(f)

    return ini_file


@pytest.fixture(scope="session")
def anyio_backend():
    return 'asyncio'



@pytest.fixture(scope="session")
def event_loop():
    """
    Creates an instance of the default event loop for the test session.
    """
    # h/t https://github.com/igortg/pytest-async-sqlalchemy
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

from .fixtures import *
