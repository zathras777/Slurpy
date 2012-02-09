''' Base class for Slurpy database interfaces. '''

from slurpy.parser import parse_column_statement, parse_table_statement

# The types of field we need to know about. These are interpretted by each
# database class.
DB_UNKNOWN = 0
DB_INTEGER = 1
DB_TEXT = 2
DB_UUID = 3
DB_SERIAL = 4
DB_BIGINT = 5
DB_TYPE_NAMES = ['','INTEGER', 'TEXT', 'UUID', 'SERIAL', 'BIGINT']
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
#    if _df.dbtype == DB_UNKNOWN:
#        _df.dbtype = DB_TEXT
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

class DatabaseField(object):
    ''' Abstraction for a database field. '''
    def __init__(self, name = '', dbtype = DB_UNKNOWN, **kwargs):
        self.name = name
        self.dbtype = dbtype
        self.size = kwargs.get('size', None)
        self.unique = kwargs.get('unique', False)
        self.null = kwargs.get('null', False)
        self.default = kwargs.get('default', None)
        self.pkey = kwargs.get('primary_key', False)
        self.dependancy = None

    def __repr__(self):
        return u"%s" % self.name

    @property
    def has_dependancy(self): return self.dependancy
    
    def dependancy_link(self):
        if self.dependancy:
            return '%s.%s' % (self.dependancy, self.name)
        return ''

    def value(self, raw):
        ''' Given a raw input, try and return the correct type for this field. '''
        if raw is None:
            return raw
        if self.dbtype in [DB_INTEGER, DB_BIGINT]:
            return int(raw)

        return str(raw) 

class DatabaseTable(object):
    def __init__(self, name = ''):
        self.name = name
        self.temp = False
        self.exists = False
        self.fields = []
        self.idlocal = False
        self.idglobal = -1
        self.columns = {}
        self.indexes = {}
        
    def __repr__(self):
        return u'%s' % self.name

    def add_field(self, fld):
        if fld.name == 'id_local':
            self.idlocal = True
        elif fld.name == 'id_global':
            self.idglobal = len(self.fields)
        self.columns[fld.name] = len(self.fields)
        self.fields.append(fld)
                
    def add_index(self, idx):
        ii = {'names': idx['columns']}
        ii['columns'] = [self.columns[n] for n in idx['columns']]
        self.indexes[idx['name']] = ii

    def has_field(self, identifier):
        for f in self.fields:
            if f.name == identifier:
                return True
        return False

    def dependancy_list(self):
        ''' Return a list of dependancies for this table. '''
        dlist = []
        for f in self.fields:
            if f.has_dependancy:
                dlist.append({'table': f.dependancy, 'field': f.name})
        return dlist

    @property
    def has_unique(self):
        if self.idglobal != -1 or self.indexes:
            return True
        return False

    def get_all_rows(self, db):
        ''' Simple query to get all rows from this table in the supplied
            database. '''
        sql = "select %s from %s" % (
                        ','.join([f.name for f in self.fields]), self.name)
        return db.query(sql)

    def select_first(self, db, cols, args):
        ''' Select query on supplied database to get the first field,
            using the cols and args supplied as where clauses. '''
        sql = "select %s from %s where %s" % (self.fields[0].name, self.name,
                                  ' and '.join(["%s=?" % c for c in cols]))
        return db.query(sql, args)

    def insert(self, db, row):
        fld_list = self.fields[1:] if self.idlocal else self.fields
        cols =  ["%s" % f.name for f in fld_list]
        args = self.get_row_values(cols, row)     
        return db.insert_row(self.name, cols, args)
    
    def update(self, db, row, local):
        if self.idlocal:
            cols =  ["%s" % f.name for f in self.fields[1:]]
            sql = "update %s set %s where id_local=?" % (self.name,
                                      ','.join(["%s=?" % f for f in cols]))
        # otherwise???
        args = self.get_row_values(cols, row)
        args.append(local)
        ck = db.execute(sql, args)
        return bool(ck)

    def get_row_values(self, cols, row):
        vals = []
        for name in cols:
            n = self.columns[name]
            vals.append(self.fields[n].value(row[n]))
        return vals
        
    def check_unique(self, row, db):
        ''' If we have some way of checking if the row is already listed in
            the database, check using it now. Return -1 if no match, or the
            id_local of the matching row (0 if no id_local field). '''
        for k,cols in self.indexes.items():
            _r = self.select_first(db, cols['names'], 
                                   self.get_row_values(cols['names'], row))
            if _r:
                if self.idlocal:
                    return _r[0][0]
                return 0
        if self.idglobal != -1:
            _r = self.select_first(db, ['id_global'], 
                                          [row[self.idglobal]])
            if _r:
                if self.idlocal:
                    return _r[0][0]
                return 0
        return -1

    def update_row_links(self, row, ids):
        rv = []
        for n in xrange(len(self.fields)):
            fld = self.fields[n]
            if fld.dependancy:
                rv.append(ids.get_value(fld.dependancy, row[n]))
            else:
                rv.append(row[n])
        return rv
        
    def move_data(self, ids, fromdb, todb):
        ''' Copy data between databases. '''
        _in = self.get_all_rows(fromdb)

        for r in _in:
            r = self.update_row_links(r, ids)
            if self.has_unique:
                ck = self.check_unique(r, todb)
                if ck != -1:
                    # update our lookup table...
                    ids.set_value(self.name, r[0], ck) 
                    self.update(todb, r, ck)
                    continue
            # This point is only reached if the record does not appear to
            # exist, so we need to insert it.
            newid = self.insert(todb, r)
            if self.idlocal:
                ids.set_value(self.name, r[0], newid)
            
        return True

    def get_id_by_field(self, db, fld, value):
        ''' Get id_local from the database table. Return -1 if not available. '''
        sql = "select id_local from %s where %s=?" % (self.name, fld)
        _vals = db.query(sql, [value,])
        if not _vals:
            return -1
        return _vals[0][0]
        
        
    def transforms(self):
        _txs = RowTransform()
        for f in self.fields:
            _txs.add_field(f)
        return _txs

    def check_global_id(self, db, gid):
        sql = "select id_global from %s where id_global=?" % self.name
        ck = db.query(sql, [gid])
        return bool(ck)
        
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
        if not hasattr(self, 'create_string'):
            raise NotImplementedError
        if drop and not self.drop(tbl):
            return False
        return self.execute(self.create_string(tbl))

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

