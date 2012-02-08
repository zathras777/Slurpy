import os
import sys
import unittest

from slurpy.databases.sqlite import SqliteDatabase

testDir = os.path.dirname(__file__)

class TestDB(unittest.TestCase):
    def setUp(self):
        self.db = SqliteDatabase()
        
    def test_001(self):
        self.assertNotEqual(self.db, None)
        self.assertRaises(ValueError, self.db.connect)
        fn = os.path.join(testDir, 'tests.db')
        if os.path.exists(fn):
            os.unlink(fn)
        self.assertEqual(self.db.connect(dbname = fn), True)
        self.assertEqual(self.db.close(), True)
        self.assertEqual(self.db.close(), True)
        
    def test_002_simple(self):
        self.assertNotEqual(self.db, None)
        self.assertEqual(self.db.connect(dbname = ':memory:'), True)
                
