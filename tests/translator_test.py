import sys
import unittest

from slurpy.translator import IdTranslator

class TestLookup(unittest.TestCase):

    def setUp(self):
        self.translator = IdTranslator()

    def test_translate(self):
        self.assertIsNotNone(self.translator)

        pass_1 = [ ['abc', 1, 10], ['abc', 2, 20] ]
        for vals in pass_1:
            self.translator.set_value(vals[0], vals[1], vals[2])

        for vals in pass_1:
            self.assertEqual(self.translator.get_value(vals[0], vals[1]), vals[2])
            self.assertEqual(self.translator.get_value(vals[0], vals[2]), -1)
            self.assertEqual(self.translator.get_value('hello', vals[2]), -1)

        pass_2 = [ ['abc', 1, 11], ['cba', 2, 20] ]
        for vals in pass_2:
            self.translator.set_value(vals[0], vals[1], vals[2])
        
        for vals in pass_2:
            self.assertEqual(self.translator.get_value(vals[0], vals[1]), vals[2])
            self.assertEqual(self.translator.get_value(vals[0], vals[2]), -1)
            self.assertEqual(self.translator.get_value('hello', vals[2]), -1)
     
    
if __name__ == '__main__':
    unittest.main()

