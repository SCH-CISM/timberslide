'''
Created on 30/12/2014

@author: asieira
'''

from re import compile
from boto.s3.connection import S3Connection
from slots import Slot

_bucketregex = compile("^s3://(?P<bucket>[^/]+)/(?P<prefix>.*?)/?$")
_yregex = compile("/(?P<val>[0-9]{4})/$")
_mdhregex = compile("/(?P<val>[0-9]{2})/$")

class S3Repository:
    def __init__(self, location):
        m = _bucketregex.match(location)
        if m is None:
            raise ValueError("location is not valid")
        self.location = location
        self.bucket = m.group('bucket')
        self.prefix = m.group('prefix')+"/"
        self._minslot = None
        self._maxslot = None
        self._conn = S3Connection()
        self._bucket = self._conn.get_bucket(self.bucket)
    
    def minslot(self):
        if self._minslot is None:
            # find smallest year
            year = [int(_yregex.search(s.name).group('val')) 
                       for s in self._bucket.list(self.prefix, '/')]
            if len(year) == 0:
                raise Exception("repository empty or not consistent (year)")
            year = format(min(year), "04")
            
            # find smallest month
            month = [int(_mdhregex.search(s.name).group('val')) 
                     for s in self._bucket.list(self.prefix+year+'/', '/')]
            if len(month) == 0:
                raise Exception("repository empty or not consistent (month)")
            month = format(min(month), "02")

            # find smallest day
            day = [int(_mdhregex.search(s.name).group('val')) 
                   for s in self._bucket.list(self.prefix+year+'/'+month+'/', '/')]
            if len(day) == 0:
                raise Exception("repository empty or not consistent (day)")
            day = format(min(day), "02")
            
            # find smallest hour
            hour = [int(_mdhregex.search(s.name).group('val')) 
                    for s in self._bucket.list(self.prefix+year+'/'+month+'/'+day+'/', '/')]
            if len(hour) == 0:
                raise Exception("repository empty or not consistent (hour)")
            hour = format(min(hour), "02")
            
            self._minslot = Slot(year+month+day+hour)
            print "Smallest slot in repository is "+str(self._minslot)
        return self._minslot
    
    def maxslot(self):
        if self._maxslot is None:
            # find smallest year
            year = [int(_yregex.search(s.name).group('val')) 
                       for s in self._bucket.list(self.prefix, '/')]
            if len(year) == 0:
                raise Exception("repository empty or not consistent (year)")
            year = format(max(year), "04")
            
            # find smallest month
            month = [int(_mdhregex.search(s.name).group('val')) 
                     for s in self._bucket.list(self.prefix+year+'/', '/')]
            if len(month) == 0:
                raise Exception("repository empty or not consistent (month)")
            month = format(max(month), "02")

            # find smallest day
            day = [int(_mdhregex.search(s.name).group('val')) 
                   for s in self._bucket.list(self.prefix+year+'/'+month+'/', '/')]
            if len(day) == 0:
                raise Exception("repository empty or not consistent (day)")
            day = format(max(day), "02")
            
            # find smallest hour
            hour = [int(_mdhregex.search(s.name).group('val')) 
                    for s in self._bucket.list(self.prefix+year+'/'+month+'/'+day+'/', '/')]
            if len(hour) == 0:
                raise Exception("repository empty or not consistent (hour)")
            hour = format(max(hour), "02")
            
            self._maxslot = Slot(year+month+day+hour)
            print "Biggest slot in repository is "+str(self._maxslot)
        return self._maxslot