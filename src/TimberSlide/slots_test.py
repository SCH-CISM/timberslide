'''
Created on 30/12/2014

@author: asieira
'''
import unittest
from slots import Slot

class SlotTest(unittest.TestCase):
    def testSlotComponents(self):
        slot = Slot("2014")
        assert slot.slot == "2014"
        assert slot.year() == 2014
        assert slot.month() is None
        assert slot.day() is None
        assert slot.hour() is None

        slot = Slot("201412")
        assert slot.slot == "201412"
        assert slot.year() == 2014
        assert slot.month() == 12
        assert slot.day() is None
        assert slot.hour() is None

        slot = Slot("20141230")
        assert slot.slot == "20141230"
        assert slot.year() == 2014
        assert slot.month() == 12
        assert slot.day() == 30
        assert slot.hour() is None

        slot = Slot("2014123010")
        assert slot.slot == "2014123010"
        assert slot.year() == 2014
        assert slot.month() == 12
        assert slot.day() == 30
        assert slot.hour() == 10

    def testSlotParent(self):
        assert Slot("2014").parent() is None
        assert Slot("201412").parent() == Slot("2014")
        assert Slot("20141230").parent() == Slot("201412")
        assert Slot("2014123010").parent() == Slot("20141230")

    def testSlotParents(self):
        assert Slot("2014").parents() == set()
        assert Slot("201412").parents() == set([Slot("2014")])
        assert Slot("20141230").parents() == set([Slot("201412"), Slot("2014")])
        assert Slot("2014123010").parents() == set([Slot("20141230"), Slot("201412"), 
                                                    Slot("2014")])

    def testSlotChildrenStart(self):
        assert Slot("2014").childrenstart() == Slot("201401")
        assert Slot("201412").childrenstart() == Slot("20141201")
        assert Slot("20141230").childrenstart() == Slot("2014123000")
        assert Slot("2014123010").childrenstart() is None

    def testSlotChildrenEnd(self):
        assert Slot("2014").childrenend() == Slot("201412")
        assert Slot("201412").childrenend() == Slot("20141231")
        assert Slot("20141230").childrenend() == Slot("2014123023")
        assert Slot("2014123010").childrenend() is None

    def testSlotChildren(self):
        assert Slot("2014").children() == set([Slot("2014"+format(i+1, "02"))
                                               for i in range(12)])
        assert Slot("201412").children() == set([Slot("201412"+format(i+1, "02"))
                                               for i in range(31)])
        assert Slot("20141201").children() == set([Slot("20141201"+format(i, "02"))
                                               for i in range(24)])

    def testSlotAdd(self):
        assert Slot("2014") + 1 == Slot("2015")
        assert Slot("2014") - 1 == Slot("2013")
        assert Slot("201401") + 1 == Slot("201402")
        assert Slot("201401") - 1 == Slot("201312")
        assert Slot("20141201") + 1 == Slot("20141202")
        assert Slot("20141201") - 1 == Slot("20141130")
        assert Slot("2014120100") + 1 == Slot("2014120101")
        assert Slot("2014120100") - 1 == Slot("2014113023")

    def testRangeTo(self):
        # basic tests
        assert Slot("2014").rangeto("2014") == set([Slot("2014")])
        assert Slot("2014").rangeto("2016") == set([Slot("2014"), Slot("2015"), Slot("2016")])
        assert Slot("201401").rangeto("201403") == set([Slot("201401"), Slot("201402"), Slot("201403")])
        assert Slot("20140101").rangeto("20140103") == set([Slot("20140101"), Slot("20140102"), Slot("20140103")])
        assert Slot("2014010100").rangeto("2014010102") == set([Slot("2014010100"), Slot("2014010101"), Slot("2014010102")])

        # reverse order
        assert Slot("2016").rangeto("2014") == Slot("2014").rangeto("2016")
        assert Slot("201401").rangeto("201403") == Slot("201403").rangeto("201401")
        assert Slot("20140101").rangeto("20140103") == Slot("20140103").rangeto("20140101")
        assert Slot("2014010100").rangeto("2014010102") == Slot("2014010102").rangeto("2014010100")
        
        # entire parent is covered - should simplify
        assert Slot("201401").rangeto("201412") == set([Slot("2014")])
        assert Slot("20141201").rangeto("20141231") == set([Slot("201412")])
        assert Slot("2014120100").rangeto("2014120123") == set([Slot("20141201")])
        
        # across different parents
        assert Slot("201311").rangeto("201402") == set([Slot("201311"), Slot("201312"), 
                                                        Slot("201401"), Slot("201402")])
        assert Slot("201311").rangeto("201502") == set([Slot("201311"), Slot("201312"), 
                                                        Slot("2014"), Slot("201501"), 
                                                        Slot("201502")])
        assert Slot("201311").rangeto("201602") == set([Slot("201311"), Slot("201312"), 
                                                        Slot("2014"), Slot("2015"),
                                                        Slot("201601"), Slot("201602")])
        assert Slot("20131129").rangeto("20140202") == set([Slot("20131129"), Slot("20131130"), 
                                                        Slot("201312"), Slot("201401"),
                                                        Slot("20140201"), Slot("20140202")])
        
        # different sizes
        assert Slot("2014").rangeto("201502") == Slot("201401").rangeto("201502")
        assert Slot("2014").rangeto("201502") ==  Slot("201502").rangeto("2014")
        assert Slot("2014").rangeto("2017010100") == Slot("2014010100").rangeto("2017010100")

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testSlots']
    unittest.main()