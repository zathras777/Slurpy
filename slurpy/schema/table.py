''' Class to represent a table within the Schema. '''

import re

from slurpy.schema.field import DatabaseField

class DatabaseTable(object):
    def __init__(self, name = ''):
        ''' Create a new schema table object. '''
        self.name = name
        self.temp = False
        self.exists = False
        self.fields = []
        self.idlocal = False
        self.idglobal = -1
        self.columns = {}
        self.indexes = {}
        self.slurpy = False
        
    def __repr__(self):
        return u'%s' % self.name

    @property
    def all_fields(self):
        ''' All fields within the table. '''
        return [f for f in self.fields]
        
    @property
    def catalog_fields(self):
        ''' Only fields that are in the Lightroom Catalog. '''
        return filter(lambda x: not x.slurpy, self.fields)

    @property
    def has_unique(self):
        if self.idglobal != -1 or self.indexes or self.idlocal:
            return True
        return False

    def from_dict(self, dd):
        for fld in dd['columns']:
            self.add_field_from_dict(fld)
        self.slurpy = dd.get('slurpy', False)            
        return True

    def add_field_from_dict(self, flddict):
        _fld = DatabaseField(flddict['identifier'])
        if _fld.from_dict(flddict):
            self.add_field(_fld)

    def add_foreignkey(self, fldname, ref, extra = ''):
        fld = self.get_field(fldname)
        if not fld:
            print "Unable to find field %s" % fldname
            return False
        return fld.add_foreignkey(ref, extra)
        
    def add_field(self, fld):
        if fld.name == 'id_local':
            self.idlocal = True
        elif fld.name == 'id_global':
            self.idglobal = len(self.fields)
        self.columns[fld.name] = len(self.fields)
        self.fields.append(fld)

    def add_indexes(self, idxs):
#              [{'identifier': u'owningModule', 'unique': 'UNIQUE', 'tablename': u'AgSpecialSourceContent', 'name': u'index_AgSpecialSourceContent_sourceModule', 'columns': [u'source', u'owningModule']}]
        for i in idxs:
            ii = {'names': i['columns']}
            ii['columns'] = [self.columns[n] for n in i['columns']]
            self.indexes[i['name']] = ii

    def has_field(self, identifier):
        for f in self.fields:
            if f.name == identifier:
                return True
        return False

    def get_field(self, identifier):
        for f in self.fields:
            if f.name.lower() == identifier.lower():
                return f
        return False

    def as_schema_dict(self):
        rdata = {'name': self.name, 'fields': [], 'indexes': self.indexes}
        for f in self.fields:
            rdata['fields'].append(f.as_dict())
        return rdata
        
    def fk_list(self):
        ''' Return a list of dependancies for this table. '''
        dlist = []
        for f in self.fields:
            if f.has_fk:
                dlist.append({'table': f.fk_table, 'field': f.name})
        return dlist

    # todo - add check for slurpy fields...
    def get_all_rows(self, db, is_catalog = False):
        ''' Simple query to get all rows from this table in the supplied
            database. '''
        flds = self.catalog_fields if is_catalog else self.all_fields
        return db.query("select %s from %s" % (
                              ','.join([f.name for f in flds]), self.name))

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

    def _check_unique(self, db, cols, args):
        _r = self.select_first(db, cols, args)
        if _r:
            if self.idlocal:
                return _r[0][0]
            return 0
        return -1

    def check_unique(self, row, db, localid):
        ''' If we have some way of checking if the row is already listed in
            the database, check using it now. Return -1 if no match, or the
            id_local of the matching row (0 if no id_local field). '''
        for k,cols in self.indexes.items():
            ck = self._check_unique(db, cols['names'], 
                                   self.get_row_values(cols['names'], row))
            if ck != -1:
                return ck
        if self.idglobal != -1:
            ck = self._check_unique(db, ['id_global'], [row[self.idglobal]])
            if ck != -1:
                return ck
        if self.idlocal:
            ck = self._check_unique(db, ['id_local'], [localid])
            if ck != -1:
                return ck
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
                ck = self.check_unique(r, todb, ids.get_value(self.name, r[0]))
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

    def stats(self, db):
        ''' Statistics for the table in database. '''
        rows = self.get_all_rows(db)
        print self.name, rows
        return {'rows': len(rows)}

