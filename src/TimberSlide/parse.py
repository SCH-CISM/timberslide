'''
Created on 31/12/2014

@author: asieira
'''

from csv import DictReader
from s3repository import BZ2KeyIterator, S3Repository
from slots import Slot

# Default transformations to be applied to fields read from TSV files
_default_func = {'net.src.port': int, 'net.dst.port': int, 'net.blocked': bool,
                   'net.src.ip.asnumber': long, 'net.dst.ip.asnumber': long,
                   'net.src.ip.mmgeo_locationId': int, 'net.dst.ip.mmgeo_locationId': int,
                   'net.src.ip.mmgeo_latitude': float, 'net.dst.ip.mmgeo_latitude': float,
                   'net.src.ip.mmgeo_longitude': float, 'net.dst.ip.mmgeo_longitude': float,
                   'net.src.ip.torExitNode': bool, 'net.dst.ip.torExitNode': bool,
                   'agg.count': int}

# Default list of values that must be replaced with None
_default_nonevals = set([ "NA", "" ])

'''
This class is an iterator (https://docs.python.org/2/glossary.html#term-iterator) 
over the lines of a given iterator, which will read each line each line using a CSV 
reader for tab-delimited fields.

Will replace any of the values in 'nonevals' by None, and will apply any function mapped to
the column name in 'func' to every non-None value.

Will return one dict for each row, mapping column names to values.
'''
class TSVKeyIterator:
    def __init__(self, reader, func=_default_func, nonevals=_default_nonevals):
        self._reader = DictReader(reader, delimiter='\t')
        self.func = func
        self.nonevals = nonevals
    
    def __iter__(self):
        return self
    
    def next(self):
        retval = self._reader.next()
        for k in retval.keys():
            if retval[k] in self.nonevals:
                retval[k] = None
            elif k in self.func.keys():
                retval[k] = self.func[k](retval[k])
        return retval

    
if __name__ == "__main__":
    repo = S3Repository("s3://nevermind-logs/export")
    k = repo.slotkeys(Slot("2014122901")).pop()
    fi = TSVKeyIterator(BZ2KeyIterator(k))
    for i in range(10):
        print fi.next()