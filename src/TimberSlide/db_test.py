'''
Created on 02/01/2015

@author: asieira
'''
import unittest
from db import is_valid_id

class Test(unittest.TestCase):
    def testValidTable(self):
        self.assertTrue(is_valid_id('a'))
        self.assertTrue(is_valid_id('a9'))
        self.assertTrue(is_valid_id('a_9'))
        self.assertFalse(is_valid_id('_a9'))
        self.assertFalse(is_valid_id(''))
        self.assertFalse(is_valid_id('table;EVILINJECT'))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()