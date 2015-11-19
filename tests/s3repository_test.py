'''
Created on 30/12/2014

@author: asieira
'''
import unittest
from timberslide.s3repository import S3Repository, BZ2KeyIterator
from timberslide.slots import Slot
from bz2 import BZ2Compressor
from random import randrange

# Streaming compression class that simulates the 'read' behavior of an S3 key that we use
# to test the BZ2KeyIterator class. It purposedfully tries to split the data in weird ways
# so we can simulate the behavior of a file that is a lot larger than the buffer we use. 
class _S3RepositoryTestKey(object):
    def __init__(self, text):
        self.text = text
        self.comp = BZ2Compressor()
        
    def read(self, size):
        while len(self.text) > 0:
            if min(size, len(self.text)) == 1:
                numbytes = 1
            else:
                numbytes = randrange(1,min(size, len(self.text)))
            retval = self.comp.compress(self.text[0:numbytes])
            self.text = self.text[numbytes:len(self.text)]
            if retval is not None:
                return retval
        
        if self.comp is None:
            return None
        else:
            retval = self.comp.flush()
            self.comp = None
            return retval

class S3RepositoryTest(unittest.TestCase):
    def testInit(self):
        repo = S3Repository("s3://bucket-name/prefix1/prefix2/")
        self.assertEquals(repo.bucket, "bucket-name")
        self.assertEquals(repo.prefix, "prefix1/prefix2/")

    def testPrefix(self):
        repo = S3Repository("s3://bucket-name/prefix1/prefix2/")
        self.assertEquals(repo.get_slot_prefix(Slot("2014")), "prefix1/prefix2/2014/")
        self.assertEquals(repo.get_slot_prefix(Slot("201401")), "prefix1/prefix2/2014/01/")
        self.assertEquals(repo.get_slot_prefix(Slot("20140102")), "prefix1/prefix2/2014/01/02/")
        self.assertEquals(repo.get_slot_prefix(Slot("2014010212")), "prefix1/prefix2/2014/01/02/12/")

    def testBZ2decomp(self):
        i = BZ2KeyIterator(_S3RepositoryTestKey("blahblahblah"), 4)
        self.assertEquals("blahblahblah", i.next())
        with self.assertRaises(StopIteration):
            i.next()
        i = BZ2KeyIterator(_S3RepositoryTestKey("blahblahblah\nblehblehbleh"), 4)
        self.assertEquals("blahblahblah\n", i.next())
        self.assertEquals("blehblehbleh", i.next())
        with self.assertRaises(StopIteration):
            i.next()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()