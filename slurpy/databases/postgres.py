''' Postgresql Database Class. '''

import psycopg2

from slurpy.database import *

class PgDatabase(DatabaseBase):
    ''' Postgresql Database class for Slurpy. '''
    def __init__(self, **kwargs):
        DatabaseBase.__init__(self, **kwargs)

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
            self._db.rollback()
            return []

    def _execute(self, stmt, args = []):
        try:
            _cur = self._actual_execute(stmt, args)
            _cur.close()
            if not self.in_transaction:
                self._db.commit()
            return True
        except psycopg2.ProgrammingError, psycopg2.InternalError:
            # Explicit rollback to return transaction to consistent state
            print "ERROR"
            if not self.in_transaction:
                self._db.rollback()
            return False

    def _create(self, tbl, is_catalog = False):
        ''' Create a table using the supplied DatabaseTable object. '''
        sql = "CREATE TABLE IF NOT EXISTS %s (" % tbl.name
        flds = []
        for f in tbl.fields:
            if is_catalog and f.slurpy:
                continue
            flds.append(self._field_statement(f))
        for n, idx in tbl.indexes.items():
            columns = ', '.join(idx['names'])
            flds.append("UNIQUE(%s)" % columns)
        sql += ',\n'.join(flds) + ')'
        return self._execute(sql)
        
    def _drop(self, tblname):
        ''' Drop a database table. '''
        return self._execute("drop table if exists %s cascade" % tblname)

    def _convert_query_stmt(self, stmt):
        return stmt.replace('?', '%s')
           
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

    def insert_row(self, tblname, cols, vals, theid = 'id_local'):
        sql = "INSERT INTO %s (%s) VALUES (%s) RETURNING %s" % (tblname,
                            ','.join(cols), ','.join(['%s' for f in vals]),
                                                                     theid)
        rv = self._query(sql, vals)
        self._db.commit()
        return rv[0][0]

    def update_row(self, tblname, cols, vals, _id):
        sql = "UPDATE %s SET %s WHERE id_local=%%s RETURNING id_local" % (tblname,
                                    ','.join(['%s=%%s' % c for c in cols]))
        vals.append(_id)
        return self._query(sql, vals)[0][0]

    def get_table_columns(self, tblname):
        info = {'columns': [], 'column_names': {}, 'column_list': [], 
                'relations': [], 'unique': [], 'self_relations': []}
        sql = '''select column_name,data_type, column_default from
                 INFORMATION_SCHEMA.COLUMNS where table_name = '%s' order
                 by ordinal_position''' % tblname.lower()
        cols = self._query(sql)
        for c in cols:
            info['column_list'].append(c[0])
            info['column_names'][c[0]] = len(info['columns'])
            info['columns'].append({'name': c[0], 'data_type': c[1],
                                    'default': c[2],
                                    'n': len(info['columns'])})
        sql = '''SELECT kcu.column_name, ccu.table_name AS foreign_table_name,
                 ccu.column_name AS foreign_column_name FROM 
                 information_schema.table_constraints AS tc 
                 JOIN information_schema.key_column_usage AS kcu ON 
                 tc.constraint_name = kcu.constraint_name JOIN 
                 information_schema.constraint_column_usage AS ccu ON 
                 ccu.constraint_name = tc.constraint_name WHERE 
                 constraint_type = 'FOREIGN KEY' and tc.table_name=%s'''
        rels = self._query(sql, [tblname.lower()])
        for r in rels:
            rdata = {'column': r[0], 'foreign_table': r[1],
                     'foreign_column': r[2], 'n': info['column_names'][r[0]]}
            if r[1] == tblname.lower():
                info['self_relations'].append(rdata)
            else:
                info['relations'].append(rdata)
                
        sql = '''SELECT kcu.column_name FROM information_schema.table_constraints
                 AS tc JOIN information_schema.key_column_usage AS kcu ON 
                 tc.constraint_name = kcu.constraint_name WHERE 
                 constraint_type = 'UNIQUE' and tc.table_name=%s'''
        
        ucols = self._query(sql, [tblname.lower()])
        for u in ucols:
            if u[0] == 'id_global' and len(ucols) > 1:
                continue
            info['unique'].append({'column': u[0], 'n': info['column_names'][u[0]]})
        return info

    def _dbtype(self, fld):
        ''' Return the string of datatype to use for field. '''
        if fld.dbtype == DB_UNKNOWN:
            if fld.name == 'id_global':
                return 'UUID'
            return 'TEXT'
        elif fld.dbtype == DB_INTEGER:
            if fld.pkey:
                return 'SERIAL'
            return 'INTEGER'
        elif fld.dbtype == DB_SERIAL:
            return 'SERIAL'
        elif fld.dbtype == DB_VARCHAR:
            return 'VARCHAR (%s)' % fld.size
        elif fld.dbtype == DB_TEXT:
            return 'TEXT'
        return 'TEXT'

    def _field_statement(self, fld):
        ''' Returns the SQL statement to create a column within a table. '''
        sql = "%s %s " % (fld.name, self._dbtype(fld))
        xtras = []
        if fld.unique: xtras.append('UNIQUE')
        if fld.pkey: xtras.append('PRIMARY KEY')
        if not fld.null: xtras.append('NOT NULL')
        if fld.has_fk:
            xtras.extend(['REFERENCES', fld.fk_table, "(%s)" % fld.fk_field])
            if fld.fk_extra:
                xtras.append(fld.fk_extra)
                
        sql += ' '.join(xtras)
        return sql.strip()

