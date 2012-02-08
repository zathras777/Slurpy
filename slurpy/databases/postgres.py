''' Postgresql Database Class. '''

import psycopg2

from slurpy.database import *

class PgDatabase(DatabaseBase):
    ''' Postgresql Database class for Slurpy. '''
    def __init__(self, **kwargs):
        DatabaseBase.__init__(self, **kwargs)
        self.isolation_level = kwargs.get('isolation_level', 0)

    def _connect(self, **kwargs):
        ''' Connect to a postgresql database '''
        connStr = ''
        for k in ['dbname', 'user', 'password']:
            if not kwargs.has_key(k):
                raise ValueError("%s is required for PostgreSQL" % k)
            connStr += '%s=%s ' % (k, kwargs[k])
        
        for k in ['host', 'port']:
            if kwargs.has_key(k):
                connStr += '%s=%s ' % (k, kwargs[k])    

        try:
            self._db = psycopg2.connect(connStr) #, 
#                                    isolation_level = self.isolation_level)
        except psycopg2.OperationalError:
            return False
            
        self.connected = True
        return True

    def _close(self):
        ''' Close an Sqlite connection. '''
        self._db.close()
        self.connected = False
        return True

    def _query(self, stmt, args = []):
        _cur = self._db.cursor()
        _cur.execute(self._convert_query_stmt(stmt), args)
        _data = _cur.fetchall()
        _cur.close()
        return _data

    def _execute(self, stmt):
        try:
            _cur = self._db.cursor()
            _cur.execute(stmt)
            _cur.close()
            self._db.commit()
            return True
        except psycopg2.ProgrammingError, psycopg2.InternalError:
            # Explicit rollback to return transaction to consistent state
            self._db.rollback()
            return False

    def _drop(self, tbl):
        ''' Drop a database table. '''
        return self._execute("drop table if exists %s cascade" % tbl.name)

    def _convert_query_stmt(self, stmt):
        return stmt.replace('?', '%s')

    def field_string(self, fld):
        ''' Convert a DatabaseField into an SQL string. '''
        sqlparts = [ fld.name ]
        if fld.name == 'id_local' and fld.dbtype == DB_INTEGER:
            sqlparts.append('SERIAL')
        elif fld.name == 'id_global':
            sqlparts.append('UUID')
        elif fld.dbtype == DB_INTEGER:
            if fld.dependancy:
                sqlparts.append('BIGINT')
                sqlparts.extend(['REFERENCES', fld.dependancy, '(id_local)', 
                                 'ON DELETE CASCADE'])
            else:
                sqlparts.append('INTEGER')
        elif fld.dbtype == DB_UNKNOWN:
            sqlparts.append('TEXT')
             
        if fld.null:
            sqlparts.extend(fld.null)
        if fld.unique:
            sqlparts.append('UNIQUE')
        if fld.pkey:
            sqlparts.extend(fld.pkey)
            
#    if self.name == 'id_local':
#      return 'id_local serial PRIMARY KEY'
#    if self.name in self.FIELDS_TO_CONVERT:
#      return '%s BIGINT REFERENCES %s (id_local) ON DELETE CASCADE' % (self.name, self.lookupRef())
#    if self.name == 'id_global':
#      return 'id_global UUID NOT NULL'
      
#    rstr = self.name
#    rstr += ' %s' % (self.type if self.type else 'TEXT')
#    if self.unique: rstr += ' UNIQUE'
#    rstr += ' %s' % ('NULL' if self.null else 'NOT NULL')
#    if self.primaryKey: rstr += ' PRIMARY_KEY'
#    if self.default: rstr += ' DEFAULT %s' % self.default
#    return rstr
        return ' '.join(sqlparts)

    def create_string(self, tbl):
        sqlparts = ['CREATE','TABLE',tbl.name, '(', 
                    ','.join(self.field_string(f) for f in tbl.fields),
                    ');']
        return ' '.join(sqlparts)
            
#    def _insert(self, stmt, args):

    def _get_table_list(self):
        """ Return a list of table names from the current
            databases public schema.
        """
        sql = "select table_name from information_schema.tables " \
              "where table_schema='public'"
        return [name for (name, ) in self._query(sql)]

    def _get_seq_list(self):
        """ Return a list of the sequence names from the current
            databases public schema.
        """
        sql = "select sequence_name from information_schema.sequences "\
              "where sequence_schema='public'"
        return [name for (name, ) in self._query(sql)]
 
    def _dropall(self):
        for t in self._get_table_list():
            self._execute("DROP TABLE %s CASCADE" % t)
        for s in self._get_seq_list():
            self._execute("DROP SEQUENCE %s CASCADE" % s)
        return True

