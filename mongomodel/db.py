from pymongo.cursor import Cursor
from pymongo.collection import Collection as PyMongoCollection
from pymongo.database import Database as PyMongoDatabase
from pymongo.errors import CollectionInvalid
from pymongo.mongo_client import MongoClient as PyMongoClient


class Collection(PyMongoCollection):
    """Mongo collection extended from pymongo to use custom Cursor class."""

    def __init__(self, database, name, model_cls=None, *args, **kwargs):
        self.model_cls = model_cls
        super(Collection, self).__init__(
            database, name, model_cls, *args, **kwargs)

    def find(self, *args, **kwargs):
        if not 'slave_okay' in kwargs:
            kwargs['slave_okay'] = self.slave_okay
        if not 'read_preference' in kwargs:
            kwargs['read_preference'] = self.read_preference
        if not 'tag_sets' in kwargs:
            kwargs['tag_sets'] = self.tag_sets
        if not 'secondary_acceptable_latency_ms' in kwargs:
            kwargs['secondary_acceptable_latency_ms'] = (
                self.secondary_acceptable_latency_ms)
        kwargs['as_class'] = self.model_cls
        return Cursor(self, *args, **kwargs)


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
