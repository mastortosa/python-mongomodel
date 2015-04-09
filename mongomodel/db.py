from pymongo.collection import Collection as PyMongoCollection
from pymongo.cursor import Cursor as PyMongoCursor
from pymongo.database import Database as PyMongoDatabase
from pymongo.errors import CollectionInvalid
from pymongo.mongo_client import MongoClient as PyMongoClient


class Cursor(PyMongoCursor):

    def __init__(self, collection, *args, **kwargs):
        self._document_class = collection.document_class
        super(Cursor, self).__init__(collection, *args, **kwargs)

    def __getitem__(self, index):
        doc = super(Cursor, self).__getitem__(index)
        if isinstance(doc, dict):
            doc = self._document_class(_validate_required=False, **doc)
            doc.as_python()
        return doc

    def __iter__(self):
        for doc in self.clone():
            doc = self._document_class(_validate_required=False, **doc)
            doc.as_python()
            yield doc

    def __len__(self):
        return self.count()


class Collection(PyMongoCollection):

    def find(self, *args, **kwargs):
        return Cursor(self, *args, **kwargs)


class Database(PyMongoDatabase):
    """Mongo database extended from pymongo to use custom Collection class."""

    def __getitem__(self, name):
        return Collection(self, name)

    def create_collection(self, name, **kwargs):
        opts = {"create": True}
        opts.update(kwargs)

        if name in self.collection_names():
            raise CollectionInvalid("collection %s already exists" % name)

        return Collection(self, name, **opts)


class Client(PyMongoClient):
    """Mongo client extended from pymongo to use custom Database class."""

    def __getitem__(self, name):
        return Database(self, name)
