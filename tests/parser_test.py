import sys
import unittest

from slurpy.parser import *
from files.parser_tests import *

# Generic database tests. Tests for individual database classes should go
# in their own files.

class TestParser(unittest.TestCase):
    def test_001_columns(self):
        tests = [
            [ 'abc123', { 'identifier': 'abc123' }],
            [ 'abc123 integer', { 'identifier': 'abc123', 
                                  'type': 'INTEGER'}],
            [ 'abc123 varchar (20)', { 'identifier': 'abc123', 
                                       'type': 'VARCHAR',
                                       'size': ['20']}],
            [ 'abc123 varchar (20, 40)', { 'identifier': 'abc123', 
                                       'type': 'VARCHAR',
                                       'size': ['20','40']}],
            [ 'abc123 integer primary key', 
                                     { 'identifier': 'abc123', 
                                       'type': 'INTEGER',
                                       'pkey': ['PRIMARY KEY']}],
            [ 'abc123 integer primary key asc autoincrement', 
                                     { 'identifier': 'abc123', 
                                       'type': 'INTEGER',
                                       'pkey': ['PRIMARY KEY','ASC','AUTOINCREMENT']}],
            [ 'abc123 integer primary key not null', 
                                     { 'identifier': 'abc123', 
                                       'type': 'INTEGER',
                                       'pkey': ['PRIMARY KEY'],
                                       'null': ['NOT', 'NULL']}],
        ]
        for t in tests:
            _ck = parse_column_statement(t[0])
            self.assertNotEqual(_ck, {})
            for k,v in t[1].items():
                self.assertEqual(_ck.has_key(k), True)
                self.assertEqual(_ck[k], v)                


    def test_002_tables(self):
        tests = [
            [ "create table simple (id integer autoincrement)",
            ],
            [ "CREATE TEMP TABLE simple (id integer)", ],
            [ "CREATE TEMPORARY TABLE if not exists simple (id integer, blah text)", ],
        ]
        for t in tests:
            _ck = parse_table_statement(t[0])
            self.assertNotEqual(_ck, {})

    def test_003_indexes(self):
        for t in index_tests:
            _ck = parse_index_statement(t[0])
            self.assertEqual(_ck['name'], t[1])
            self.assertEqual(len(_ck['columns']), t[2])
            self.assertEqual(_ck.has_key('unique'), t[3])
            
                        
