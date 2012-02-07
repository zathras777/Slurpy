testData = [
    ['''CREATE TABLE Adobe_imageDevelopBeforeSettings (
    id_local INTEGER PRIMARY KEY,
    beforeDigest,
    beforeHasDevelopAdjustments,
    beforePresetID,
    beforeText,
    developSettings INTEGER
);''', 6, '''CREATE TABLE Adobe_imageDevelopBeforeSettings ( id_local SERIAL PRIMARY KEY,beforeDigest TEXT,beforeHasDevelopAdjustments TEXT,beforePresetID TEXT,beforeText TEXT,developSettings INTEGER );''', True],
    ['''CREATE TABLE Adobe_imageProperties (
    id_local INTEGER PRIMARY KEY,
    id_global UNIQUE NOT NULL,
    image INTEGER,
    propertiesString
);''', 4, '''CREATE TABLE Adobe_imageProperties ( id_local SERIAL PRIMARY KEY,id_global UUID NOT NULL UNIQUE,image BIGINT REFERENCES Adobe_images (id_local) ON DELETE CASCADE,propertiesString TEXT );''', False]
]


