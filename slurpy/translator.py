#! /usr/bin/env python


class IdTranslator(object):
    ''' Class that manages translations between table id's '''

    def __init__(self):
        ''' Create a new IdTranslator object '''
        self._translators = {}

    class _Translator(object):
        ''' Class that manages transaltions. '''
        def __init__(self):
            ''' Simple constructor '''
            self.keys = {}

        def set_value(self, key, val):
            ''' Set a value for a given key. Overwrites existing values. '''
            self.keys[str(key)] = val

        def get_value(self, key):
            ''' Get the translated value for a key. Returns -1 if not available. '''
            if self.keys.has_key(str(key)):
                return self.keys[str(key)]
            return -1

    def get_translator(self, name, create = True):
        if not self._translators.has_key(name):
            if not create:
                return None
            self._translators[name] = self._Translator()
        return self._translators[name]

    def set_value(self, name, key, val):
        self.get_translator(name).set_value(key, val)
    
    def get_value(self, name, key):
        translator = self.get_translator(name, False)
        if translator:
            return translator.get_value(key)
        return -1

