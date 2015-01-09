'''
Created on 30/12/2014

@author: asieira
'''

from re import compile
from boto.s3.connection import S3Connection
from bz2 import BZ2Decompressor
from slots import Slot

_bucketregex = compile("^s3://(?P<bucket>[^/]+)/(?P<prefix>.*?)/?$")
_yregex = compile("/(?P<val>[0-9]{4})/$")
_mdhregex = compile("/(?P<val>[0-9]{2})/$")

'''
This class encapsulates access to an S3 bucket and prefix where data files are stored
using the <prefix>/<YYYY>/<MM>/<DD>/<HH>/ prefixes according to the slot.

It will allow all S3 Key objects associated with a Slot set to be easily obtained.
'''
class S3Repository(object):
    def __init__(self, location):
        m = _bucketregex.match(location)
        if m is None:
            raise ValueError("location is not valid")
        self.location = location
        self.bucket = m.group('bucket')
        self.prefix = m.group('prefix')+"/"
        self._minslot = None
        self._maxslot = None
        self._conn = None
        self._bucket = None
    
    def _open(self):
        if self._conn is None:
            self._conn = S3Connection()
            self._bucket = self._conn.get_bucket(self.bucket, validate=False)
    
    # Returns the earliest slot for which there is data in the repository,
    # in YYYYMMDDHH format.
    def get_min_slot(self):
        if self._minslot is None:
            self._open()
            
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
    
    # Returns the latest slot for which there is data in the repository,
    # in YYYYMMDDHH format.
    def get_max_slot(self):
        if self._maxslot is None:
            self._open()

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
    
    # Returns the S3 prefix associated with a given slot.
    def get_slot_prefix(self, slot):
        retval = self.prefix + format(slot.year(), "04") + '/'
        if slot.month() is None:
            return retval
        retval = retval + format(slot.month(), "02") + '/'
        if slot.day() is None:
            return retval
        retval = retval + format(slot.day(), "02") + '/'
        if slot.hour() is None:
            return retval
        return retval + format(slot.hour(), "02") + '/'
    
    # Returns a list of boto S3 Key class instances associated with a given slot.
    def get_slot_keys(self, slots):            
        self._open()
        retval = set()
        if isinstance(slots, Slot):
            slots = [slots]
        for slot in slots:
            for key in self._bucket.list(self.get_slot_prefix(slot)):
                retval.add(key)
        return list(retval)
    
    # Returns a boto S3 Key class instance for a given prefix.
    def get_prefix_key(self, prefix):
        self._open()
        return self._bucket.get_key(prefix)

'''
This class is an iterator (https://docs.python.org/2/glossary.html#term-iterator) 
over the lines of an S3 Key object that also decompresses the contents using BZ2.
'''
class BZ2KeyIterator(object):
    def __init__(self, key, bufsize=100*1024):
        self.key = key
        self.bufsize = bufsize
        self._decomp = BZ2Decompressor()
        self._lines = []
        self._done = False
        
    def __iter__(self):
        return self
    
    def next(self):
        while True:
            if len(self._lines) > 1:
                return self._lines.pop(0) + '\n'
            elif self._done and len(self._lines) == 1:
                return self._lines.pop(0)
            elif self._done and len(self._lines) == 0:
                raise StopIteration
            else:
                chunk = self.key.read(self.bufsize)
                if chunk is not None:
                    try:
                        lines = self._decomp.decompress(chunk).split('\n')
                        if len(self._lines) > 0:
                            self._lines[len(self._lines)-1] = self._lines[len(self._lines)-1] + lines.pop(0) 
                        for l in lines:
                            self._lines.append(l)
                    except EOFError:
                        self._done = True
                else:
                    self._done = True
