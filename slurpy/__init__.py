from slurpy.databases.postgres import PgDatabase
from slurpy.schema.schema import *
from slurpy.translator import IdTranslator

class SlurpSession(object):
    def __init__(self):
        self.ids = IdTranslator()
        self.hostid = 0
        self.catalogid = 0

    def setup_catalog(self, db, catalog):
        rv = db.query("select id from hosts where hostname=? and system=?",
                                        [catalog.hostname, catalog.system])
        if rv:
            self.hostid = rv[0][0]
        else:
            self.hostid = db.insert_row('hosts', ['hostname', 'system'], 
                                        [catalog.hostname, catalog.system])
        if not self.hostid:
            return False
        rv = db.query("select id_local from catalog hosts where lc_filename=? and host=?",
                                   [catalog.filename.lower(), self.hostid])
        if rv:
            self.catalogid = rv[0][0]
        else: 
            self.catalogid = db.insert_row('catalog', 
                                      ['filename', 'lc_filename', 'host'], 
                 [catalog.filename, catalog.filename.lower(), self.hostid])

        if not self.catalogid:
            return False
        return True

    def set_value(self, table, a, b):
        self.ids.set_value(table, a, b)

    def get_value(self, table, a):
        return self.ids.get_value(table, a)

    def dump(self):
        self.ids.dump()
        
class Slurpy(object):
    ''' The Slurpy class. '''
    def __init__(self):
        self._db = PgDatabase()
        self.config = {}
        self.session = None

    def read_config_file(self, filename):
        import ConfigParser
        config = ConfigParser.RawConfigParser()
        config.read(filename)
        if config.sections() == []:
            return False
        for s in config.sections():
            section = {}
            for k,v in config.items(s):
                if ',' in v:
                    v = [p.strip() for p in v.split(',')]
                section[k] = v
            self.config[s] = section
        return True

    def connect(self, **kwargs):
        if not kwargs and not self.config.has_key('database'):
            print "No args supplied and no database config..."
            return False
        if kwargs:
            return self._db.connect(**kwargs)
        return self._db.connect(**self.config['database'])

    def close(self):
        ''' Close the database connection. '''
        return self._db.close()

    def session_start(self):
        self.session = SlurpSession()
        
    def _check_schema(self, sqlstr):
        self._db.start_transaction()
        for s in [sql.strip() for sql  in sqlstr.split(';')]:
            if not s:
                continue
            if not self._db.execute(s):
                self._db.rollback()
                return False
            self._db.commit()
        return True
    
    def check_schema(self, catalog = None, drop = False):
        self.schema = LrSchema(catalog)

        if not self.schema.setup():
            return False

        if drop:
            self.schema.drop_tables(self._db)

        return self.schema.check_tables(self._db)      
            
    def import_catalog(self, catalog):
        ''' Import the supplied catalog. '''


        self.session_start()

        self.schema.import_catalog(catalog._db, self._db)
        
        # todo - handle delete case...
        self.session.setup_catalog(self._db, catalog)

        # Root folders are the basic places where pictures are stored. They
        # are unique across all catalogs, so we start by importing them
        # into the database.
        self.get_root_folders(catalog)
        self.get_folders(catalog)
        
        for t in self.schema.tables[4:]:
            self.import_table(catalog, t)
                        
        return False
                    
    def get_root_folders(self, catalog):
        roots = catalog.get_table_rows('AgLibraryRootFolder')
        for r in roots:
            # Different catalogs may point to the same folder, but with
            # different id_global UUID's, so we will check for whether we
            # have it already listed by absolutePath and host.
            ck = self._db.query("select id_local from AgLibraryRootFolder "
                                "where absolutePath=%s and host=%s",
                                               [r[2], self.session.hostid])
            if ck:
                self.session.set_value('AgLibraryRootFolder', r[0], ck[0][0])
                continue
            cols = ['id_global','absolutePath','name',
                                         'relativePathFromCatalog', 'host']
            args = list(r[1:5])
            args.append(self.session.hostid)
            _id = self._db.insert_row('AgLibraryRootFolder', cols, args)
            self.session.set_value('AgLibraryRootFolder', r[0], _id)
        print self.session.dump()
                              
#[(18, u'33462EBF-D9FB-4FDB-ADB0-9B4024DC5D4D', u'/Users/davidreid/Pictures/2010/', u'2010', u'../../../2010/'), 
# (2113, u'1D6C97C8-A958-4401-8702-84399727390A', u'/Users/davidreid/Downloads/', u'Downloads', u'../../../../Downloads/')]
#    id_global UUID UNIQUE NOT NULL,
#    absolutePath UNIQUE NOT NULL DEFAULT '',
#    name NOT NULL DEFAULT '',
#    relativePathFromCatalog TEXT,
#    host BIGINT NULL REFERENCES hosts (id) ON DELETE CASCADE

    def get_folders(self, catalog):
        roots = catalog.get_table_rows('AgLibraryFolder')
        for r in roots:
            root = self.session.get_value('AgLibraryRootFolder', r[3])           
            # Different catalogs may point to the same folder, but with
            # different id_global UUID's, so we will check for whether we
            # have it already listed by absolutePath and host.
            ck = self._db.query("select id_local from AgLibraryFolder "
                                "where pathFromRoot=%s and rootFolder=%s",
                                                            [r[2], root])
            if ck:
                self.session.set_value('AgLibraryFolder', r[0], ck[0][0])
                continue
            cols = ['id_global','pathFromRoot','rootFolder']
            args = list(r[1:3])
            args.append(root)
            _id = self._db.insert_row('AgLibraryFolder', cols, args)
            self.session.set_value('AgLibraryFolder', r[0], _id)

        print self.session.dump()

    def get_files(self, catalog):
        roots = catalog.get_table_rows('AgLibraryFile')
        for r in roots:
            root = self.session.get_value('AgLibraryRootFolder', r[3])           
            # Different catalogs may point to the same folder, but with
            # different id_global UUID's, so we will check for whether we
            # have it already listed by absolutePath and host.
            ck = self._db.query("select id_local from AgLibraryFolder "
                                "where pathFromRoot=%s and rootFolder=%s",
                                                            [r[2], root])
            if ck:
                self.session.set_value('AgLibraryFolder', r[0], ck[0][0])
                continue
            cols = ['id_global','pathFromRoot','rootFolder']
            args = list(r[1:3])
            args.append(root)
            _id = self._db.insert_row('AgLibraryFolder', cols, args)
            self.session.set_value('AgLibraryFolder', r[0], _id)
        print self.session.dump()

    def _check_unique(self, table, uniq, columns, row):
        where = []
        args = []
        sql = "select id_local from %s where " % table
        for _u in uniq:
            _val = row[_u['n']]
            _c = columns[_u['n']]
            if _c['data_type'] == 'text' and _val is None:
                if _c['default'] is None:
                    where.append("%s is null " % _c['name'])
                else:
                    where.append("%s=%%s " % _c['name'])
                    args.append("''")
                continue
                    
            where.append("%s=%%s" % _u['column'])
            _dt = ['data_type']
            if _dt in ['integer','bigint']:
                args.append(int(_val))
            else:
                args.append(str(_val))
        sql += ' and '.join(where)
        print sql, args
        ck = self._db.query(sql, args)
        if ck:
            return ck[0][0]
        return -1

    def import_table(self, catalog, table):
        print table
        
        TOCHECK = ['Adobe_imageDevelopSettings',
                   'Adobe_imageDevelopBeforeSettings',
                   'AgLibraryCollectionContent',
                   'AgLibraryIPTC',
                  ]

#        tblinfo = self._db.get_table_columns(table)
#        hostid = self.find_host(catalog.hostname, catalog.system)
#        catalogid = self.find_catalog(catalog, hostid)

        rows = catalog.get_table_rows(table)
        for r in rows:
            rr = list(r)

            if 'catalog' in tblinfo['column_list']:
                rr.append(catalogid)
                
            for _rel in tblinfo['relations']:
                if _rel['foreign_table'] not in ['catalog','host']:
                    rr[_rel['n']] = self.session.get_value(_rel['foreign_table'], 
                                                              r[_rel['n']])
            for _rel in tblinfo['self_relations']:
                rr[_rel['n']] = None
           
            # Different catalogs may point to the same folder, but with
            # different id_global UUID's, so we will check for whether we
            # have it already listed by absolutePath and host.

            if tblinfo['unique']:
                ck = self._check_unique(table, tblinfo['unique'], 
                                        tblinfo['columns'], rr)
                if ck > 0:
                    self.session.set_value(table, r[0], ck)
                    continue

            if table in TOCHECK:
                for c in tblinfo['columns']:
                    if c['n'] < len(r):
                        print "%-30s: %s" % (c['name'], str(r[c['n']])[:50])

            if tblinfo['column_list'][0] == 'id_local':
                _id = self._db.insert_row(table, tblinfo['column_list'][1:], rr[1:])
                self.session.set_value(table, r[0], _id)
            else:
                self._db.insert_row(table, tblinfo['column_list'], rr)

            for _rel in tblinfo['self_relations']:
                _rid = self.session.get_value(_rel['foreign_table'], 
                                                              r[_rel['n']])
                sql = "update %s set %s=%%s where id_local = %%s" % (
                                                     table, _rel['column'])
#                print sql
                self._db.execute(sql, [_rid, _id])
                 
#        print self.session.dump()

    def get_table_row_count(self, name):
        ''' Return the number of rows in a table within the catalog. '''
        rv = self._db.query("select count(*) as rows from %s" % name)
        if rv:
            return rv[0][0]
        return 0

