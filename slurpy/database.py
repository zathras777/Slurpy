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


def _field_from_dict(rv):
    _df = DatabaseField(name = rv.get('identifier'))
    if rv.has_key('type'):
        _df.dbtype = DB_TYPES[rv['type']]
        _df.size = rv.get('size', None)
    _df.unique = rv.has_key('unique')
    for k in ['null', 'pkey', 'default']:
        setattr(_df, k, rv.get(k, None))        
    return _df

def table_from_string(sqlstr):
    ''' Parse an SQL statement into a DatabaseTable. Returns None if fails. '''
    from slurpy.lightroom import lightroom_reference
    if len(sqlstr) == 0 or not sqlstr.upper().strip().startswith("CREATE TABLE"):
        return None
    rv = parse_table_statement(sqlstr)
    if rv == {}:
        return None
    tbl = DatabaseTable(rv.get('identifier'))
    tbl.temp = rv.has_key('temp')
    tbl.exists = rv.has_key('ifexists')
    for c in rv.get('columns', []):
        _fld = _field_from_dict(c)
        _dep = lightroom_reference(_fld.name, _fld.dbtype, tbl.name)
        if _dep[0]:
            _fld.dependancy = _dep[1] 
        tbl.add_field(_fld)
 
    return tbl
    
def field_from_string(sqlstr):
    ''' Parse an SQL statement for a DatabaseField. Returns None if parse
        fails. '''
    if len(sqlstr) == 0:
        return None

    rv = parse_column_statement(sqlstr)
    if rv == {}:
        return None
    return _field_from_dict(rv)
             
class DatabaseBase(object):
    ''' This is a base database and is meant to be subclassed. '''
    def __init__(self):
        self.connected = False
        self.in_transaction = False

    def start_transaction(self):
        if self.connected:
            if self.in_transaction:
                self.commit()
            self.execute("BEGIN")
            self.in_transaction = True
            
    def commit(self):
        ''' Finish the transaction, commit any changes. '''
        raise NotImplementedError
   
    def rollback(self):
        ''' Finish the transaction, discard all changes. '''
        raise NotImplementedError

    def connect(self, **args):
        ''' Connect to database using supplied keyword arguments. '''
        if self.connected:
            if not self.close():
                return False
        if not hasattr(self, '_connect'):
            raise NotImplementedError
        return self._connect(**args)

    def close(self):
        ''' Close a database connection. '''
        if not self.connected:
            return True
        if not hasattr(self, '_close'):
            raise NotImplementedError
        return self._close()

    def query(self, statement, args = []):
        ''' Execute a select query on the database, returning data as a list. '''
        if not self.connected or len(statement) == 0:
            return []
        if '?' in statement and len(args) == 0:
            raise ValueError('Statement requires arguments, but none supplied')
        return self._query(statement, args)

    def execute(self, statement, args=[]):
        ''' Execute a select query on the database, returning True or False. '''
        if not self.connected or len(statement) == 0:
            return False
        if not hasattr(self, '_execute'):
            raise NotImplementedError
        return self._execute(statement, args)

    def create(self, tbl, drop = False):
        ''' Create a table, optionally dropping if it exists. '''
        if not self.connected or tbl is None:
            return False
        if not hasattr(self, '_create'):
            raise NotImplementedError
        if drop and not self.drop(tbl):
            return False
        return self._create(tbl)

    def drop(self, tbl):
        ''' Drop a database table. '''
        if not self.connected or tbl is None:
            return False
        if not hasattr(self, '_drop'):
            return self.execute("drop table if exists %s" % tbl.name)
        return self._drop(tbl)

    def dropall(self):
        ''' Drop all tables and related items... '''
        if not self.connected:
            return False

        if not hasattr(self, '_dropall'):
            raise NotImplementedError
        return self._dropall()
        
    def field_string(self, fld):
        ''' Generate the SQL statement to create the supplied field. '''
        raise NotImplementedError

    def create_string(self, tbl):
        ''' Generate the SQL statement to create the supplied table. '''
        raise NotImplementedError

    def insert_row(self, tblname, cols, vals):
        ''' Insert a row into the table specified with the columns and
            values supplied. Return the id of the newly inserted row. '''
        raise NotImplementedError

    def update_row(self, tblname, cols, vals, id):
        ''' Update a row contents. Returns True or False. '''
        raise NotImplementedError

