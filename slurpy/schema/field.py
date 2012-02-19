''' Base class for Slurpy database interfaces. '''
import re

from slurpy.schema.parser import parse_column_statement, parse_table_statement

# The types of field we need to know about. These are interpretted by each
# database class.
DB_UNKNOWN = 0
DB_INTEGER = 1
DB_TEXT = 2
DB_UUID = 3
DB_SERIAL = 4
DB_BIGINT = 5
DB_VARCHAR = 6

DB_TYPE_NAMES = ['','INTEGER', 'TEXT', 'UUID', 'SERIAL', 'BIGINT', 'VARCHAR']
DB_TYPES = { 'INTEGER': DB_INTEGER, 'TEXT': DB_TEXT, 'UUID': DB_UUID, 
             'SERIAL': DB_SERIAL, 'BIGINT': DB_BIGINT }

def type_from_string(s):
    if s and DB_TYPES.has_key(s.upper()):
        return DB_TYPES[s.upper()]
    return DB_UNKNOWN

class DatabaseField(object):
    ''' Abstraction for a database field. '''
    def __init__(self, name = '', dbtype = DB_UNKNOWN, **kwargs):
        self.name = name
        self.dbtype = dbtype
        self.size = kwargs.get('size', None)
        self.unique = kwargs.get('unique', False)
        self.null = kwargs.get('null', True)
        self.default = kwargs.get('default', None)
        self.pkey = kwargs.get('primary_key', False)
        self.fk_table = None
        self.fk_field = None
        self.fk_extra = None
        self.slurpy = False
        
    def __repr__(self):
        return u"%s" % self.name

    def from_dict(self, dd):
        self.dbtype = type_from_string(dd.get('type'))
        self.size = dd.get('size') # not used by lightroom...
        self.slurpy = dd.get('slurpy', False)
        if dd.get('pkey', None):
            self.pkey = True
        n = dd.get('null')
        if n and 'NOT' in n:
            self.null = False
        if dd.get('unique'):
            self.unique = True
        dflt = dd.get('default')
        if dflt:
            self.default = dflt[1] if len(dflt) == 2 else "''"
        return True

    def as_dict(self):
        basic = {'name': self.name, 'dbtype': self.dbtype, 'size': self.size,
                'unique': self.unique, 'null': self.null, 
                'default': self.default, 'pkey': self.pkey }
        if self.has_fk:
            basic['foreignkey'] = { 'table': self.fk_table, 
                                    'field': self.fk_field,
                                    'constraints': self.fk_extra }
        return basic
                 
    @property
    def has_fk(self): return bool(self.fk_table)
    
    def add_foreignkey(self, ref, extra):
        ck = re.match("^(.*)\((.*)\);?$", ref)
        if not ck:
            print "FAILED to find reference in foreign key %s" % ref
            return False
        self.fk_table = ck.group(1)
        self.fk_field = ck.group(2)
        self.fk_extra = extra
        return True
        
    def value(self, raw):
        ''' Given a raw input, try and return the correct type for this field. '''
        if raw is None:
            return raw
        if self.dbtype in [DB_INTEGER, DB_BIGINT]:
            return int(raw)
        return str(raw) 

