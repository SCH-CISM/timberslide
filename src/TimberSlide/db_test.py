'''
Created on 02/01/2015

@author: asieira
'''
import unittest
from db import is_valid_id, escape, connection_string
from argparse import ArgumentTypeError

class Test(unittest.TestCase):
    def testValidTable(self):
        self.assertEquals(is_valid_id('a'), 'a')
        self.assertEquals(is_valid_id('a9'), 'a9')
        self.assertEquals(is_valid_id('a_9'), 'a_9')
        with self.assertRaises(ArgumentTypeError):
            is_valid_id('_a9')
        with self.assertRaises(ArgumentTypeError):
            is_valid_id('')
        with self.assertRaises(ArgumentTypeError):
            is_valid_id('table;EVILINJECT')
        
    def testEscape(self):
        self.assertEquals(escape(''), '\'\'')
        self.assertEquals(escape('blah'), '\'blah\'')
        self.assertEquals(escape('\'blah\''), '\'\\\'blah\\\'\'')
        self.assertEquals(escape('bl\\ah'), '\'bl\\\\ah\'')
        
    def testConnectionString(self):
        self.assertEquals(connection_string('localhost:65000', 'username', 'password'),
                          "user='username' password='password' host='localhost' port=65000")
        self.assertEquals(connection_string('localhost:65000', 'username', 'password', 'database'),
                          "user='username' password='password' host='localhost' port=65000 dbname='database'")
        self.assertEquals(connection_string('localhost:65000', 'username', 'password', 'database', 'sslmode'),
                          "user='username' password='password' host='localhost' port=65000 dbname='database' sslmode='sslmode'")

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()