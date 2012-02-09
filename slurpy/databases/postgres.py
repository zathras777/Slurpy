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
            self._db = psycopg2.connect(connStr)
        except psycopg2.OperationalError:
            return False
            
        self.connected = True
        return True

    def _close(self):
        ''' Close an Sqlite connection. '''
        self._db.close()
        self.connected = False
        return True

    def commit(self):
        self._db.commit()
        self.in_transaction = False
   
    def rollback(self):
        ''' Finish the transaction, discard all changes. '''
        self._db.rollback()
        self.in_transaction = False

    def _actual_execute(self, stmt, args):
        _cur = self._db.cursor()
        _cur.execute(self._convert_query_stmt(stmt), args)
        return _cur
        
    def _query(self, stmt, args = []):
        try:
            _cur = self._actual_execute(stmt, args)
            _data = _cur.fetchall()
            _cur.close()
            return _data
        except psycopg2.ProgrammingError, psycopg2.InternalError:
            return False

    def _execute(self, stmt, args = []):
        try:
            _cur = self._actual_execute(stmt, args)
            _cur.close()
            if not self.in_transaction:
                self._db.commit()
            return True
        except psycopg2.ProgrammingError, psycopg2.InternalError:
            # Explicit rollback to return transaction to consistent state
            if not self.in_transaction:
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

        return ' '.join(sqlparts)

    def create_string(self, tbl):
        sqlparts = ['CREATE','TABLE',tbl.name, '(', 
                    ','.join(self.field_string(f) for f in tbl.fields),
                    ');']
        return ' '.join(sqlparts)
            
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

    def insert_row(self, tblname, cols, vals):
        sql = "INSERT INTO %s (%s) VALUES (%s) RETURNING id_local" % (tblname,
                            ','.join(cols), ','.join(['%s' for f in vals]))
        return self._query(sql, vals)[0][0]

    def update_row(self, tblname, cols, vals, _id):
        sql = "UPDATE %s SET %s WHERE id_local=%%s RETURNING id_local" % (tblname,
                                    ','.join(['%s=%%s' % c for c in cols]))
        vals.append(_id)
        return self._query(sql, vals)[0][0]

