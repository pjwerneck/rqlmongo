import json
from copy import deepcopy
from pathlib import Path

import pytest

HERE = Path(__file__).parent
fpath = HERE / "users.json"


with open(fpath) as f:
    USERS = json.load(f)

USERS.sort(key=lambda x: x["_id"])
for u in USERS:
    u["balance"] = float(u["balance"][1:].replace(",", ""))


@pytest.fixture
def users():
    return deepcopy(USERS)


@pytest.fixture(scope="session")
def db(mongo_server_sess):

    db_ = mongo_server_sess.api.testdb

    db_.create_collection("users")
    db_.users.insert_many(USERS)

    return db_
