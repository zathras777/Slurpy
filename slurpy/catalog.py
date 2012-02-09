''' The Catalog class represents a Lightroom Catalog. '''

from slurpy.translator import IdTranslator
from slurpy.database import table_from_string
from slurpy.databases.sqlite import SqliteDatabase
from slurpy.databases.postgres import PgDatabase

class Catalog(object):
    ''' Catalog '''

    EXCLUDE_TABLES = ['Adobe_namedIdentityPlate','Adobe_variables',
                      'Adobe_variablesTable']

    def __init__(self, filename = ''):
        self.filename = ''
        self.version = '' # version string for catalog
        self.entity_counter = 0.0
        self.tables = []
        self.ordered_table_list = None
        self._catalog = SqliteDatabase()
        if filename:
            self.open(filename)

    @property
    def is_connected(self): return self._catalog.connected

    def open(self, filename):
        ''' Open a catalog. If the filename supplied does ont exist,
            a new catalog will be created. '''
        from os.path import exists
        _setup_rqd = False if exists(filename) else False
        if self._catalog.connect(dbname = filename):
            self.filename = filename
            if not _setup_rqd:
                self._get_catalog_version()
                self._get_entity_counter()
            return True
        return False

    def close(self):
        ''' Close a catalog. '''
        self._catalog.close()

    def get_schema_from_catalog(self):
        ''' Read the database schema from a Catalog. '''
        from slurpy.parser import parse_index_statement
        if not self.is_connected:
            return False
        sql = "select name, sql from sqlite_master order by name"
        rv = self._catalog.query(sql)
        for r in rv:
            if 'sqlite' in r[0]: continue
            _tbl = table_from_string(r[1])
            if _tbl:
                self.tables.append(_tbl)
                continue
            _idx = parse_index_statement(r[1])
            if _idx and _idx.has_key('unique'):
                _tbl = self.get_table(_idx['tablename'])
                if _tbl:
                    _tbl.add_index(_idx)            
        self._get_ordered_table_list()
        return True

    def get_table(self, tblname):
        for t in self.tables:
            if t.name == tblname:
                return t
        return None

    def create_database(self, db, drop = False):
        ''' Create the entire database in the provided database. '''
        ok = []
        for o in self.ordered_table_list:
            _tbl = self.get_table(o)
            if not db.create(_tbl, drop):
                # rollback...
                for oo in ok:
                    db.drop(self.get_table(oo))
                return False
            ok.append(o)
        return True

    def drop_database(self, db):
        ''' Remove the database tables from the provided database. '''
        for o in self.ordered_table_list:
            if not db.drop(self.get_table(oo)):
                return False
        return True

    def import_all(self, db):
        ''' Import data into the supplied database. '''
        ids = IdTranslator()
        db.start_transaction()
        for o in self.ordered_table_list:
             _tbl = self.get_table(o)
             if not _tbl.move_data(ids, self._catalog, db):
                 db.rollback()
                 return False
        db.commit()
        return True
             
    def _get_catalog_version(self):
        rv = self._catalog.query("select value from Adobe_variablesTable where name=?",
                                                       ['Adobe_DBVersion'])
        if rv:
            self.version = rv[0][0]

    def _get_entity_counter(self):
        rv = self._catalog.query("select value from Adobe_variablesTable where name=?",
                                                       ['Adobe_entityIDCounter'])
        if rv:
            self.entity_counter = float(rv[0][0])

    def _get_ordered_table_list(self):
        ''' Order tables using the dependancy data. '''
        def get_all(D, k):
            for ii in D.get(k, []):
                if ii != k:
                    for jj in get_all(D, ii):
                        yield jj
            yield k
   
        ordered = list()
        depData = {}
        for tbl in self.tables:
            d = {}
            for f in tbl.dependancy_list():
                d[f['table']] = 1
            if d:
                depData[tbl.name] = d

        for k in depData.keys():
            for kk in get_all(depData, k):
                if not kk in ordered:
                    ordered.append(kk)
        
        # Make sure we have all tables listed. We will process imports based
        # on this table, so we should have all tables we want to import.
        for t in self.tables:
            if not t.name in ordered and not \
                                   t.name in self.__class__.EXCLUDE_TABLES:
                ordered.append(t.name)        

        self.ordered_table_list = ordered

