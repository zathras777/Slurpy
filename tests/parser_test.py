import sys
import unittest

from slurpy.parser import *

# Generic database tests. Tests for individual database classes should go
# in their own files.

class TestColumns(unittest.TestCase):
    def test_001(self):
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
#    'abc123 varchar (20)',
#    'abc123 varchar ( 20,  30)',
 #   'abc123 varchar ( 20,  30) primary key',
 ##   'abc123 varchar ( 20,  30) not null primary key',
 #   'abc123 varchar ( 20,  30) primary key unique',
 #   'abc123 varchar ( 20,  30) primary key unique default 0',
 #   "abc123 varchar ( 20,  30) primary key unique default 'abc'",
  #  'abc123 varchar ( 20,  30) primary key desc autoincrement',
  #  
#]
            
