'''
Created on 31/12/2014

@author: asieira
'''
import unittest
from parse import TSVIterator
from StringIO import StringIO

class ParseTest(unittest.TestCase):
    def testParse(self):
        text = ('"a"\t"b"\t"c"\t"d"\t"e"\n' 
                '""\t"NA"\t12.5\t"12"\tTRUE\n')
        func = { 'c':float, 'd':int, 'e':bool }
        nonevals = set(["", "NA"])
        tsv = TSVIterator(StringIO(text), func, nonevals)
        self.assertEquals(tsv.next(), {'a': None, 'b': None, 'c': 12.5, 'd': 12, 'e': True})
        with self.assertRaises(StopIteration):
            tsv.next()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()