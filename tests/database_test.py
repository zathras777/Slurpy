import sys
import unittest

from slurpy.database import *

# Generic database tests. Tests for individual database classes should go
# in their own files.

class TestGenericDB(unittest.TestCase):
    def test_001_simple(self):
        df = DatabaseField()
        self.assertEqual(df.dbtype, DB_UNKNOWN)
        self.assertEqual(df.name, '')
        self.assertEqual(df.unique, False)

    def test_002_columns(self):
        STRS = [
            [ 'simple', 'simple', DB_UNKNOWN ],
            [ 'id_local INTEGER PRIMARY KEY', 'id_local', DB_INTEGER ],
            [ ' id_global UNIQUE NOT NULL', 'id_global', DB_UNKNOWN ],
            [ 'additionalInfoSet INTEGER NOT NULL DEFAULT 0,', 'additionalInfoSet', DB_INTEGER ],
            [ "xmp NOT NULL DEFAULT ' '", 'xmp', DB_UNKNOWN ]
        ]
        for s in STRS:
            _df = field_from_string(s[0])
            self.assertEqual(_df.name, s[1])
            self.assertEqual(_df.dbtype, s[2])

    def test_003_tables(self):
        tests = [
            ['create table simple (id integer primary key autoincrement)',
             {'name': 'simple', 'temp': False}, ['id']],
            ['create table if not exists simple (id integer, foo text)',
             {'name': 'simple', 'temp': False}, ['id','foo']],
             
        ]
        for t in tests:
            _tbl = table_from_string(t[0])
            self.assertNotEqual(_tbl, None)
            for k,v in t[1].items():
                self.assertEqual(getattr(_tbl, k), v)
            self.assertEqual(len(_tbl.fields), len(t[2]))
            for c in t[2]:
                self.assertEqual(_tbl.has_field(c), True)

