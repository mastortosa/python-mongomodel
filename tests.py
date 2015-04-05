import json
import os
import re
import sys
import time
import unittest
import urllib
import uuid
from datetime import datetime, date, timedelta
from StringIO import StringIO

from bson.objectid import ObjectId
from bson.binary import Binary
import dateutil.parser

from mongomodel import model, fields, utils


class TestFields(unittest.TestCase):

    def _assert_field_to_mongo_valid(self, field_class, value_in, value_out,
                                     **kwargs):
        field = field_class(**kwargs)
        self.assertEqual(field.to_mongo(value_in), value_out)

    def _assert_field_to_mongo_invalid(self, field_class, value, **kwargs):
        field = field_class(**kwargs)
        with self.assertRaises(fields.FieldValidationError):
            field.to_mongo(value)

    def test_text(self):
        self._assert_field_to_mongo_valid(fields.TextField, 'txt', 'txt')
        self._assert_field_to_mongo_valid(fields.TextField, None, None, required=False)

        self._assert_field_to_mongo_invalid(fields.TextField, None)
        self._assert_field_to_mongo_invalid(fields.TextField, '')

    def test_email(self):
        self._assert_field_to_mongo_valid(fields.EmailField, 'foo@bar.com', 'foo@bar.com')
        self._assert_field_to_mongo_valid(fields.EmailField, None, None, required=False)

        self._assert_field_to_mongo_invalid(fields.EmailField, '')
        self._assert_field_to_mongo_invalid(fields.EmailField, 'foobar')
        self._assert_field_to_mongo_invalid(fields.EmailField, 'foo@bar')
        self._assert_field_to_mongo_invalid(fields.EmailField, 'foo@bar.c')
        self._assert_field_to_mongo_invalid(fields.EmailField, 'f@bar')
        self._assert_field_to_mongo_invalid(fields.EmailField, 'f@bar.com')

    def test_url(self):
        self._assert_field_to_mongo_valid(fields.URLField, 'http://www.web.com', 'http://www.web.com')
        self._assert_field_to_mongo_valid(fields.URLField, 'https://www.web.com', 'https://www.web.com', https=True)
        self._assert_field_to_mongo_valid(fields.URLField, 'www.web.com', 'http://www.web.com')
        self._assert_field_to_mongo_valid(fields.URLField, 'www.web.com', 'https://www.web.com', https=True)
        # TODO: think about this.
        # self._assert_field_to_mongo_valid(fields.URLField, 'http://www.web.com', 'https://www.web.com', https=True)

        self._assert_field_to_mongo_invalid(fields.URLField, '')
        self._assert_field_to_mongo_invalid(fields.URLField, 'txt')
        self._assert_field_to_mongo_invalid(fields.URLField, 'txt.c')
        # TODO: think about this.
        # self._assert_field_to_mongo_invalid(fields.URLField, 'http://www.web.com', https=True)

    def test_int(self):
        self._assert_field_to_mongo_valid(fields.IntegerField, 42, 42)
        self._assert_field_to_mongo_valid(fields.IntegerField, '42', 42)
        self._assert_field_to_mongo_valid(fields.IntegerField, 42., 42)

        self._assert_field_to_mongo_invalid(fields.IntegerField, '')
        self._assert_field_to_mongo_invalid(fields.IntegerField, 'xxx')

    def test_float(self):
        self._assert_field_to_mongo_valid(fields.FloatField, 42., 42.)
        self._assert_field_to_mongo_valid(fields.FloatField, 42, 42.)
        self._assert_field_to_mongo_valid(fields.FloatField, '42', 42.)
        self._assert_field_to_mongo_valid(fields.FloatField, '42.', 42.)

        self._assert_field_to_mongo_invalid(fields.FloatField, '')
        self._assert_field_to_mongo_invalid(fields.FloatField, 'xxx')

    def test_json(self):
        self._assert_field_to_mongo_valid(fields.JSONField, {'xxx': 23}, '{"xxx": 23}')

        self._assert_field_to_mongo_invalid(fields.JSONField, '')

    def test_datetime(self):
        self._assert_field_to_mongo_valid(fields.DateTimeField, datetime(2000, 1, 1), datetime(2000, 1, 1).isoformat())

    # ...

    def test_embedded_document(self):

        class Doc(model.Document):
            field = fields.IntegerField(default=1)

        self._assert_field_to_mongo_valid(fields.EmbeddedDocumentField, {'field': 42}, {'field': 42}, document_class=Doc)
        self._assert_field_to_mongo_valid(fields.EmbeddedDocumentField, {'field': '42'}, {'field': 42}, document_class=Doc)
        self._assert_field_to_mongo_valid(fields.EmbeddedDocumentField, {}, {'field': 1}, document_class=Doc)



if __name__ == '__main__':
    unittest.main()
