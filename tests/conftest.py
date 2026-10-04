import pytest
from pymongo.errors import ServerSelectionTimeoutError

from common.db import ensure_indexes, get_db

TEST_DB = "disaster_test"


@pytest.fixture
def db():
    d = get_db(TEST_DB)
    try:
        d.client.admin.command("ping")
    except ServerSelectionTimeoutError:
        pytest.skip("MongoDB not running")
    d.client.drop_database(TEST_DB)
    ensure_indexes(d)
    yield d
    d.client.drop_database(TEST_DB)
