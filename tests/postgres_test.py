import os
import sys
import unittest

from slurpy.database import table_from_string
from slurpy.databases.postgres import PgDatabase
from slurpy.catalog import Catalog

testDir = os.path.dirname(__file__)

class TestPg(unittest.TestCase):
    def test_001_conversions(self):
        db = PgDatabase()
        stmt = 'select ?,? from blah'
        args = ['abc', 'def']
        self.assertEqual(db._convert_query_stmt(stmt), 'select %s,%s from blah')
        
    def test_002_connect(self):
        kwargs = {'dbname': 'slurpy', 'user': 'slurpy', 'password': 'slurpy' }
        db = PgDatabase()
        self.assertEqual(db.connect(**kwargs), True)
        self.assertEqual(db.close(), True)
        kwargs = {'dbname': 'slurpy', 'user': 'slurpy', 'password': 'slurpy2' }
        self.assertEqual(db.connect(**kwargs), False)
        self.assertEqual(db.close(), True)

    def test_003_sql_conversion(self):
        from tests.files.schema_1 import testData
        db = PgDatabase()
        for t in testData:        
            _tbl = table_from_string(t[0])
            self.assertNotEqual(_tbl, None)
            self.assertEqual(len(_tbl.fields), t[1])
            self.assertEqual(db.create_string(_tbl), t[2])

    def test_004_creation(self):
        from tests.files.schema_1 import testData
        kwargs = {'dbname': 'slurpy', 'user': 'slurpy', 'password': 'slurpy' }
        db = PgDatabase()
        self.assertEqual(db.connect(**kwargs), True)
        self.assertEqual(db.dropall(), True)
        for t in testData:        
            _tbl = table_from_string(t[0])
            # Drop the table before we start
            self.assertEqual(db.drop(_tbl), True)
            self.assertEqual(db.create_string(_tbl), t[2])
            # Simple create, no drop before creation
            self.assertEqual(db.create(_tbl), t[3])
            if not t[3]:
                continue
            # Create, dropping table before creation, should succeed
            self.assertEqual(db.create(_tbl, True), True)
            # Simple create, no drop before creation, should fail
            self.assertEqual(db.create(_tbl), False)
            # Drop the table
            self.assertEqual(db.drop(_tbl), True)
        self.assertEqual(db.close(), True)

    def test_005_catalog(self):
        c = Catalog(os.path.join(testDir, 'files', 'test.lrcat'))
        self.assertEqual(c.version, '0300025')
        self.assertEqual(c.get_schema_from_catalog(), True)
                
        kwargs = {'dbname': 'slurpy', 'user': 'slurpy', 'password': 'slurpy' }
        db = PgDatabase()
        self.assertEqual(db.connect(**kwargs), True)

        self.assertEqual(c.create_database(db, True), True)
        
