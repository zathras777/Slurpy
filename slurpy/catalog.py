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

    def compare_tables(self, todb):
        ''' Debug/test function. '''
        for t in self.ordered_table_list:
            tbl = self.get_table(t)
            if tbl:
                a = tbl.stats(self._catalog)
                b = tbl.stats(todb)
                if a['rows'] != b['rows']:
                    return False
        return True

from platform import node, system
class Catalog2(object):
    ''' Class to represent a Lightroom Catalog file. '''
    def __init__(self, filename = ''):
        self.hostname = node()
        self.system = system()
        self._db = SqliteDatabase()
        if filename:
            self.open(filename)

    def __repr__(self):
        return u'Lightroom Catalog: %s [%s, %s]' % (self.filename, 
                                                self.hostname, self.system)
        
    @property
    def is_connected(self):
        return self._db.is_connected
        
    def open(self, filename):
        ''' Open a catalog file. '''
        if self._db.connect(dbname = filename):
            self.filename = filename
            return True
        return False

    def close(self):
        ''' Close the Catalog. '''
        return self._db.close()

    def stats(self):
        ''' Collect simple stats about the Catalog. '''
        tocollect = {
            'root_folders': "AgLibraryRootFolder",
            'library_folders': "AgLibraryFolder",
            'library_files': "AgLibraryFile",
            'adobe_images': "Adobe_images",
        }
        stats = {}
        for k,v in tocollect.items():
            sql = "select count(id_local) as num from %s" % v
            rv = self._db.query(sql)
            if rv:
                setattr(self, k, rv[0][0])
                stats[k] = rv[0][0]
        return stats

    def get_table_rows(self, name):
        ''' Get all rows from a table. '''
        return self._db.query("select * from %s" % name)

    def get_table_row_count(self, name):
        ''' Return the number of rows in a table within the catalog. '''
        rv = self._db.query("select count(*) as rows from %s" % name)
        if rv:
            return rv[0][0]
        return 0
        
    
