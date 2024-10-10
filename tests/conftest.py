import pytest
from pymongo import MongoClient


@pytest.fixture(scope="function")
def mongodb_client():
   client = MongoClient('mongodb://localhost:27017/')
   yield client
   client.close()
