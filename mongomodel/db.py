from pymongo.collection import Collection
from pymongo.database import Database as PyMongoDatabase
from pymongo.errors import CollectionInvalid
from pymongo.mongo_client import MongoClient as PyMongoClient


class Database(PyMongoDatabase):
    """Mongo database extended from pymongo to use custom Collection class."""

    def __getattr__(self, name):
        return Collection(self, name)

    def create_collection(self, name, **kwargs):
        opts = {"create": True}
        opts.update(kwargs)

        if name in self.collection_names():
            raise CollectionInvalid("collection %s already exists" % name)

        return Collection(self, name, **opts)


class Client(PyMongoClient):
    """Mongo client extended from pymongo to use custom Database class."""

    def __getattr__(self, name):
        return Database(self, name)
