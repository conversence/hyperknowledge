from configparser import ConfigParser
from pathlib import Path
import os

config = ConfigParser()
# print(Path(__file__).parent.joinpath("config.ini"))
config.read(Path(__file__).parent.parent.joinpath("config.ini"))
production = os.environ.get("PRODUCTION", False)
target_db = os.environ.get(
    "TARGET_DB",
    (
        "test"
        if "PYTEST_CURRENT_TEST" in os.environ
        else ("production" if production else "development")
    ),
)


_db_config_get_nonce = object()


def db_config_get(key: str, default=_db_config_get_nonce):
    if default is _db_config_get_nonce or config.has_option(target_db, key):
        return config.get(target_db, key)
    return default


db_name = db_config_get("database")


def engine_url(db=target_db, owner=True):
    user = "owner" if owner else "client"
    return f"postgresql+asyncpg://{config.get(db, user)}:{config.get(db, f'{user}_password')}@{config.get('postgres', 'host')}:{config.get('postgres', 'port')}/{db_name}"
