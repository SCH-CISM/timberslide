'''
Created on 02/01/2015

@author: asieira
'''
import unittest
from db import validtable

class Test(unittest.TestCase):
    def testValidTable(self):
        self.assertTrue(validtable('a'))
        self.assertTrue(validtable('a9'))
        self.assertTrue(validtable('a_9'))
        self.assertFalse(validtable('_a9'))
        self.assertFalse(validtable(''))
        self.assertFalse(validtable('table;EVILINJECT'))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()