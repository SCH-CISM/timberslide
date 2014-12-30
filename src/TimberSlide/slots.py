'''
Created on 23/12/2014

@author: asieira
'''
from types import StringType, IntType
from datetime import datetime, timedelta
import pytz
from calendar import monthrange

class Slot:
    def __init__(self, slot):
        self.slot = slot
        if not type(slot) is StringType or not slot.isdigit() or not len(slot) in [4, 6, 8, 10]: 
            raise ValueError("slot must be a string with 4, 6, 8 or 10 digits only")
        params = {'tzinfo': pytz.utc, 'year': self.year(), 'month': 1, 'day': 1}
        if self.month() is not None:
            params['month'] = self.month()
        if self.day() is not None:
            params['day'] = self.day()
        if self.hour() is not None:
            params['hour'] = self.hour()
        datetime(**params)
        
    def year(self):
        if self.slot is None:
            return None
        return int(self.slot[0:4])

    def month(self):
        if self.slot is None or len(self.slot) < 6:
            return None
        return int(self.slot[4:6])

    def day(self):
        if self.slot is None or len(self.slot) < 8:
            return None
        return int(self.slot[6:8])

    def hour(self):
        if self.slot is None or len(self.slot) < 10:
            return None
        return int(self.slot[8:10])

    def parent(self):
        if len(self) == 4:
            return None
        else:
            return Slot(self.slot[0:len(self)-2])
    
    def childrenstart(self):
        if len(self) == 4 or len(self) == 6:
            return Slot(self.slot + "01")
        elif len(self) == 8:
            return Slot(self.slot + "00")
        else:
            return None
    
    def childrenend(self):
        if len(self) == 4:
            return Slot(self.slot + "12")
        elif len(self) == 6:
            return Slot(self.slot + format(monthrange(self.year(), self.month())[1], "02"))
        elif len(self) == 8:
            return Slot(self.slot + "23")
        else:
            return None
        
    def rangeto(self, other):
        if not isinstance(other, Slot):
            other = Slot(other)
        
        # make sizes equal, and check if proper order was used
        start = self
        end = other
        while len(start) < len(end):
            start = start.childrenstart()
        while len(start) > len(end):
            end = end.childrenend()
        if start > end:
            return other.rangeto(self)
        
        return _rangeto(start, end)
        
    def __repr__(self):
        return "Slot(\""+self.slot+"\")"
    
    def __str__(self):
        return self.slot
        
    def __lt__(self, other):
        return self.slot < other.slot

    def __le__(self, other):
        return self.slot <= other.slot
        
    def __gt__(self, other):
        return self.slot > other.slot

    def __ge__(self, other):
        return self.slot >= other.slot

    def __eq__(self, other):
        return self.slot == other.slot

    def __neq__(self, other):
        return self.slot != other.slot

    def __len__(self):
        return len(self.slot)

    def __hash__(self):
        return self.slot.__hash__()
    
    def __add__(self, other):
        if not type(other) is IntType:
            raise ValueError("slots should be added to integers")
        if len(self) == 4:
            return Slot(format(self.year()+other, "04"))
        elif len(self) == 6:
            year = self.year()
            month = self.month() + other
            while month < 1:
                year = year - 1
                month = month + 12
            while month > 12:
                year = year + 1
                month = month + 12
            return Slot(format(year, "04")+format(month, "02"))
        elif len(self) == 8:
            dt = datetime(self.year(), self.month(), self.day()) + timedelta(days=other)
            return Slot(format(dt, "%Y%m%d"))
        else:
            dt = datetime(self.year(), self.month(), self.day(), self.hour()) + timedelta(hours=other)
            return Slot(format(dt, "%Y%m%d%H"))

    def __sub__(self, other):
        return self.__add__(-other)
 
 
def _rangeto(start, end):
    # trivial cases
    if start == end:
        return set([start])
    if start > end:
        return set()
    if len(start) == 4:
        return set([Slot(format(i, "04")) for i in range(start.year(), end.year()+1)])
    
    # now handle more complex cases
    startpar = start.parent()
    endpar = end.parent()
    if startpar == endpar:
        # same parent
        if start == startpar.childrenstart() and end == startpar.childrenend():
            # completely cover parent, so return it
            return set([startpar])
        else:
            # same parent, doesn't cover all of it so we range through last component
            return set([Slot(startpar.slot+format(i, "02"))
                        for i in range(int(start.slot[len(start.slot)-2:len(start.slot)]),
                                   int(end.slot[len(end.slot)-2:len(end.slot)])+1)])
    else:
        # parents are different, recurse
        if startpar+1 > endpar-1:
            return (start.rangeto(startpar.childrenend()) 
                    | endpar.childrenstart().rangeto(end))
        else:
            return (start.rangeto(startpar.childrenend()) 
                    | (startpar+1).rangeto(endpar-1)
                    | endpar.childrenstart().rangeto(end))
