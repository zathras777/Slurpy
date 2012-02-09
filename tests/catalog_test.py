import os
import sys
import unittest

from slurpy.catalog import Catalog
from slurpy.databases.postgres import PgDatabase

testDir = os.path.dirname(__file__)

class TestCatalog(unittest.TestCase):
    def test_001_empty(self):
        c = Catalog()
        
        self.assertNotEqual(c, None)
        self.assertEqual(c.is_connected, False)
        self.assertEqual(c.filename, '')

    def test_002_open(self):
        c = Catalog(os.path.join(testDir, 'files', 'test.lrcat'))
        self.assertEqual(c.version, '0300025')
        c.get_schema_from_catalog()
        self.assertNotEqual(c.tables, [])
        self.assertNotEqual(c.ordered_table_list, None)
        self.assertEqual(c.ordered_table_list[0], 'AgLibraryRootFolder')
        self.assertEqual(c.get_table('blah'), None)
        self.assertNotEqual(c.get_table('Adobe_images'), None)
        ai = c.get_table('Adobe_images')
        self.assertEqual(ai.name, 'Adobe_images')

    def test_003_import(self):
        c = Catalog(os.path.join(testDir, 'files', 'test2.lrcat'))
        self.assertEqual(c.version, '0300025')
        self.assertEqual(c.get_schema_from_catalog(), True)        

        kwargs = {'dbname': 'slurpy', 'user': 'slurpy', 'password': 'slurpy' }
        db = PgDatabase()
        self.assertEqual(db.connect(**kwargs), True)

        self.assertEqual(c.create_database(db, True), True)
        self.assertEqual(c.import_all(db), True)
        self.assertEqual(c.import_all(db), True)
        

