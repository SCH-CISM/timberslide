'''
Created on 30/12/2014

@author: asieira
'''
import unittest
from TimberSlide.s3repository import S3Repository
from slots import Slot

class S3RepositoryTest(unittest.TestCase):
    def testInit(self):
        repo = S3Repository("s3://bucket-name/prefix1/prefix2/")
        assert repo.bucket == "bucket-name"
        assert repo.prefix == "prefix1/prefix2/"

    def testPrefix(self):
        repo = S3Repository("s3://bucket-name/prefix1/prefix2/")
        assert repo.slotprefix(Slot("2014")) == "prefix1/prefix2/2014/"
        assert repo.slotprefix(Slot("201401")) == "prefix1/prefix2/2014/01/"
        assert repo.slotprefix(Slot("20140102")) == "prefix1/prefix2/2014/01/02/"
        assert repo.slotprefix(Slot("2014010212")) == "prefix1/prefix2/2014/01/02/12/"

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()