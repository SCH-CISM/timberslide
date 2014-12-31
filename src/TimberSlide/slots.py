'''
Created on 23/12/2014

@author: asieira
'''
from types import StringType, IntType
from datetime import datetime, timedelta
import pytz
from calendar import monthrange

'''
This class represents a time slot that might contain files. It is represented as a string
in YYYY, YYYYMM, YYYYMMDD or YYYYMMDDHH formats (UTC).
'''
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
        
    def parents(self):
        par = self.parent()
        if par is None:
            return set()
        else:
            return set([par]) | par.parents()
    
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
        
    def children(self):
        if len(self) == 10:
            return None
        retval = self.childrenstart().rangeto(self.childrenend()-1)
        retval.add(self.childrenend())
        return retval
        
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
 
'''
Called internally by Slot.rangeto to handle the recursive resolution. 

Expects two Slot instances as parameters, and returns a set of Slot instances.
'''
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

'''
Parses a slot range string in the '<slot>:<slot>' format, replacing any missing slots by the
boundaries of existing slots in the given repository (repo) which must be a S3Repository
instance.

Returns a set of Slot instances.
'''
def parseSlotRange(text, repo):
    text = text.split(':')
    if len(text) == 1:
        return set([Slot(text[0])])
    elif len(text) == 2:
        if len(text[0]) == 0:
            text[0] = repo.minslot()
            if len(text[1]) > 0:
                while len(text[0]) > len(text[1]):
                    text[0] = text[0].parent()
        else:
            text[0] = Slot(text[0])
        if len(text[1]) == 0:
            text[1] = repo.maxslot()
            if len(text[0]) > 0:
                while len(text[1]) > len(text[0]):
                    text[1] = text[1].parent()
        else:
            text[1] = Slot(text[1])
        return text[0].rangeto(text[1])
    else:
        raise ValueError('slot \"'+text+'\" is invalid')

'''
Receives a list of sets of Slot instances, and consolidates all of them into a single set
of Slot instances.

The processing will ensure that the slots with the smallest length possible are used, and that
slots contained by other slots will be removed.
'''
def mergeSlotSets(slotSetList):
    # merge entries
    allSlots = set()
    for slotSet in slotSetList:
        allSlots.update(slotSet)
        
    # remove entries with parents already in the list
    redundant = set()
    for slot in allSlots:
        if not allSlots.isdisjoint(slot.parents()):
            redundant.add(slot)
    allSlots.difference_update(redundant)
    redundant.clear()
    
    # consolidate entries by using parent if all of its children 
    # are present
    parents = set()
    done = False
    while not done:
        done = True
        for slot in allSlots:
            par = slot.parent()
            if par is not None and not par in parents:
                siblings = par.children()
                if siblings.issubset(allSlots):
                    done = False
                    parents.add(par)
                    redundant.update(siblings)
        allSlots.difference_update(redundant)
        redundant.clear()
        allSlots.update(parents)
        parents.clear()
    
    # we're done
    return allSlots
    