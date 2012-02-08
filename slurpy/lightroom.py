''' Lightroom specific functions. '''

from slurpy.database import *

def lightroom_reference(fld_name, fld_type, tbl_name):
    ''' Check if a table/field relation is related to another field within
        the database. Returns a tuple containing a true/false value and
        the table name that is referenced.
    '''
    if len(fld_name) == 0 or len(tbl_name) == 0:
        return (False, '')
    if fld_name in ['image','photo'] and fld_type == DB_INTEGER:
        return (True, u'Adobe_images')
    elif fld_name == 'rootFile':
        return (True, u'AgLibraryFile')
    elif fld_name == 'rootFolder':
        return (True, u'AgLibraryRootFolder')
    elif fld_name == 'folder':
        return (True, u'AgLibraryFolder')
    elif fld_name in ['tag','tag1','tag2']:
        return (True, u'AgLibraryKeyword')
    elif fld_name == 'collection':
        if 'Published' in tbl_name or 'Remote' in tbl_name:
            return (True, u'AgLibraryPublishedCollection')
        return (True, u'AgLibraryCollection')
    elif fld_name == 'parent':
        return (True, tbl_name)
    if fld_name.endswith('Ref'):
        tName = fld_name[0].upper() + fld_name[1:-3]
        if fld_name in ['cameraModelRef', 'cameraSNRef','lensRef']:
            return (True, u'AgInternedExif%s' % tName)
        return (True, u'AgInternedIptc%s' % tName)
    return (False, '')

