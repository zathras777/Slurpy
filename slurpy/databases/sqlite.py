''' Sqlite3 Database Class. '''

import sqlite3

from slurpy.database import DatabaseBase

class SqliteDatabase(DatabaseBase):
    def __init__(self, **kwargs):
        DatabaseBase.__init__(self, **kwargs)
        self.isolation_level = kwargs.get('isolation_level', None)

    ''' Sqlite3 Database class for Slurpy. '''
    def _connect(self, **kwargs):
        if not kwargs.has_key('dbname'):
            raise ValueError('No dbname supplied')
        try:
            self._db = sqlite3.connect(kwargs['dbname'], 
                                    isolation_level = self.isolation_level)
            self._cursor = self._db.cursor()
        except:
            return False
        self.connected = True
        return True

    def _close(self):
        ''' Close an Sqlite connection. '''
        self._db.close()
        self.connected = False
        return True

    def _query(self, stmt, args):
        self._cursor.execute(stmt, args)
        return self._cursor.fetchall()

#    def _insert(self, stmt, args):
        
