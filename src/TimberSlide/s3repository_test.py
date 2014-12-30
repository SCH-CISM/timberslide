'''
Created on 30/12/2014

@author: asieira
'''
import unittest
from TimberSlide.s3repository import S3Repository

class S3RepositoryTest(unittest.TestCase):
    def testBasic(self):
        assert S3Repository("s3://bucket-name/prefix1/prefix2/").bucket == "bucket-name"
        assert S3Repository("s3://bucket-name/prefix1/prefix2/").prefix == "prefix1/prefix2/"

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()