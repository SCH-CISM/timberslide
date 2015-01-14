'''
Created on 31/12/2014

@author: asieira
'''

import csv
import logging

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

Will return one list for each row, and the 'colnames' attribute will contain the 
corresponding column names.
'''
class TSVIterator(object):
    def __init__(self, reader, func=_default_func, nonevals=_default_nonevals):
        self._reader = csv.reader(reader, delimiter='\t')
        self.nonevals = nonevals
        self.colnames = self._reader.next()
        self.func = [ None ] * len(self.colnames)
        for i in range(len(self.colnames)):
            if self.colnames[i] in func.keys():
                self.func[i] = func[self.colnames[i]]
        self._row = 0
    
    def __iter__(self):
        return self
    
    def next(self):
        retval = self._reader.next()
        self._row = self._row + 1
        while len(retval) == 0:
            retval = self._reader.next()            
            self._row = self._row + 1

        # just a sanity check, need to do proper exception here later
        assert(len(retval) == len(self.colnames))

        for i in range(len(retval)):
            if retval[i] in self.nonevals:
                retval[i] = None
            elif self.func[i] is not None:
                try:
                    retval[i] = self.func[i](retval[i])
                except Exception, e:
                    logging.fatal('Error in row {0} processing column {1} with value {2}'.format(str(self._row),
                                                                                                 self.colnames[i],
                                                                                                 retval[i]))
                    raise e
        return retval
