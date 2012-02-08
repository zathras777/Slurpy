import os
import sys
import unittest

from slurpy.databases.sqlite import SqliteDatabase

class TestDB(unittest.TestCase):
    def test_001(self):
        db = SqliteDatabase()
        self.assertNotEqual(db, None)
        self.assertRaises(ValueError, db.connect)
        os.unlink('tests.db')
        self.assertEqual(db.connect(dbname = 'tests.db'), True)
        self.assertEqual(db.close(), True)
        self.assertEqual(db.close(), True)
        
    def test_002_simple(self):
        db = SqliteDatabase()
        self.assertNotEqual(db, None)
        self.assertEqual(db.connect(dbname = ':memory:'), True)
                
