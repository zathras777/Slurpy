index_tests = [
    ['''CREATE INDEX index_AgSearchablePhotoProperty_lc_idx_internalValue ON AgSearchablePhotoProperty( lc_idx_internalValue )''',
     'index_AgSearchablePhotoProperty_lc_idx_internalValue', 1, False],
    ['''CREATE UNIQUE INDEX index_AgSearchablePhotoProperty_pluginKey ON AgSearchablePhotoProperty( photo, propertySpec )''',
     'index_AgSearchablePhotoProperty_pluginKey', 2, True],
    ['''CREATE INDEX index_AgSearchablePhotoProperty_propertyValue ON AgSearchablePhotoProperty( propertySpec, internalValue )''',
     'index_AgSearchablePhotoProperty_propertyValue', 2, False ],
    ['''CREATE INDEX index_AgSearchablePhotoProperty_propertyValue_lc ON AgSearchablePhotoProperty( propertySpec, lc_idx_internalValue )''', 'index_AgSearchablePhotoProperty_propertyValue_lc', 2, False],
    ['''CREATE INDEX index_AgSpecialSourceContent_owningModule ON AgSpecialSourceContent( owningModule )''',
     'index_AgSpecialSourceContent_owningModule', 1, False],
    ['''CREATE UNIQUE INDEX index_AgSpecialSourceContent_sourceModule ON AgSpecialSourceContent( source, owningModule )''',
     'index_AgSpecialSourceContent_sourceModule', 2, True]
]

