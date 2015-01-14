'''
Created on 30/12/2014

@author: asieira
'''
import unittest
from slots import Slot
from s3repository import S3Repository
from TimberSlide.slots import parseSlotRange

class DummyS3Repository(S3Repository):
    def __init__(self, location):
        S3Repository.__init__(self, location)
    
    def get_min_slot(self):
        return Slot("2000010100")

    def get_max_slot(self):
        return Slot("2100123123")

class SlotTest(unittest.TestCase):
    def testSlotComponents(self):
        slot = Slot("2014")
        self.assertEquals(slot.slot, "2014")
        self.assertEquals(slot.year(), 2014)
        self.assertIsNone(slot.month())
        self.assertIsNone(slot.day())
        self.assertIsNone(slot.hour())

        slot = Slot("201412")
        self.assertEquals(slot.slot, "201412")
        self.assertEquals(slot.year(), 2014)
        self.assertEquals(slot.month(), 12)
        self.assertIsNone(slot.day())
        self.assertIsNone(slot.hour())

        slot = Slot("20141230")
        self.assertEquals(slot.slot, "20141230")
        self.assertEquals(slot.year(), 2014)
        self.assertEquals(slot.month(), 12)
        self.assertEquals(slot.day(), 30)
        self.assertIsNone(slot.hour())

        slot = Slot("2014123010")
        self.assertEquals(slot.slot, "2014123010")
        self.assertEquals(slot.year(), 2014)
        self.assertEquals(slot.month(), 12)
        self.assertEquals(slot.day(), 30)
        self.assertEquals(slot.hour(), 10)

    def testSlotParent(self):
        self.assertIsNone(Slot("2014").parent())
        self.assertEquals(Slot("201412").parent(), Slot("2014"))
        self.assertEquals(Slot("20141230").parent(), Slot("201412"))
        self.assertEquals(Slot("2014123010").parent(), Slot("20141230"))

    def testSlotParents(self):
        self.assertEquals(Slot("2014").parents(), set())
        self.assertEquals(Slot("201412").parents(), set([Slot("2014")]))
        self.assertEquals(Slot("20141230").parents(), set([Slot("201412"), Slot("2014")]))
        self.assertEquals(Slot("2014123010").parents(), 
                          set([Slot("20141230"), Slot("201412"), Slot("2014")]))

    def testSlotChildrenStart(self):
        self.assertEquals(Slot("2014").children_start(), Slot("201401"))
        self.assertEquals(Slot("201412").children_start(), Slot("20141201"))
        self.assertEquals(Slot("20141230").children_start(), Slot("2014123000"))
        self.assertIsNone(Slot("2014123010").children_start())

    def testSlotChildrenEnd(self):
        self.assertEquals(Slot("2014").children_end(), Slot("201412"))
        self.assertEquals(Slot("201412").children_end(), Slot("20141231"))
        self.assertEquals(Slot("20141230").children_end(), Slot("2014123023"))
        self.assertIsNone(Slot("2014123010").children_end())

    def testSlotChildren(self):
        self.assertEquals(Slot("2014").children(), 
                          set([Slot("2014"+format(i+1, "02")) for i in range(12)]))
        self.assertEquals(Slot("201412").children(), 
                          set([Slot("201412"+format(i+1, "02")) for i in range(31)]))
        self.assertEquals(Slot("20141201").children(), 
                          set([Slot("20141201"+format(i, "02")) for i in range(24)]))

    def testSlotAdd(self):
        self.assertEquals(Slot("2014") + 1, Slot("2015"))
        self.assertEquals(Slot("2014") - 1, Slot("2013"))
        self.assertEquals(Slot("201401") + 1, Slot("201402"))
        self.assertEquals(Slot("201401") - 1, Slot("201312"))
        self.assertEquals(Slot("201412") + 1, Slot("201501"))
        self.assertEquals(Slot("20141201") + 1, Slot("20141202"))
        self.assertEquals(Slot("20141201") - 1, Slot("20141130"))
        self.assertEquals(Slot("2014120100") + 1, Slot("2014120101"))
        self.assertEquals(Slot("2014120100") - 1, Slot("2014113023"))

    def testRangeTo(self):
        # basic tests
        self.assertEquals(Slot("2014").rangeto("2014"), set([Slot("2014")]))
        self.assertEquals(Slot("2014").rangeto("2016"), 
                          set([Slot("2014"), Slot("2015"), Slot("2016")]))
        self.assertEquals(Slot("201401").rangeto("201403"), 
                          set([Slot("201401"), Slot("201402"), Slot("201403")]))
        self.assertEquals(Slot("20140101").rangeto("20140103"), 
                          set([Slot("20140101"), Slot("20140102"), Slot("20140103")]))
        self.assertEquals(Slot("2014010100").rangeto("2014010102"), 
                          set([Slot("2014010100"), Slot("2014010101"), Slot("2014010102")]))
        self.assertEquals(Slot("20141201").rangeto("20150101"), 
                          set([Slot("201412"), Slot("20150101")]))

        # reverse order
        self.assertEquals(Slot("2016").rangeto("2014"), Slot("2014").rangeto("2016"))
        self.assertEquals(Slot("201401").rangeto("201403"), Slot("201403").rangeto("201401"))
        self.assertEquals(Slot("20140101").rangeto("20140103"), 
                          Slot("20140103").rangeto("20140101"))
        self.assertEquals(Slot("2014010100").rangeto("2014010102"), 
                          Slot("2014010102").rangeto("2014010100"))
        
        # entire parent is covered - should simplify
        self.assertEquals(Slot("201401").rangeto("201412"), set([Slot("2014")]))
        self.assertEquals(Slot("20141201").rangeto("20141231"), set([Slot("201412")]))
        self.assertEquals(Slot("2014120100").rangeto("2014120123"), set([Slot("20141201")]))
        
        # across different parents
        self.assertEquals(Slot("201311").rangeto("201402"), 
                          set([Slot("201311"), Slot("201312"), Slot("201401"), Slot("201402")]))
        self.assertEquals(Slot("201311").rangeto("201502"), 
                          set([Slot("201311"), Slot("201312"), Slot("2014"), Slot("201501"), 
                               Slot("201502")]))
        self.assertEquals(Slot("201311").rangeto("201602"), 
                          set([Slot("201311"), Slot("201312"), Slot("2014"), Slot("2015"),
                               Slot("201601"), Slot("201602")]))
        self.assertEquals(Slot("20131129").rangeto("20140202"), 
                          set([Slot("20131129"), Slot("20131130"), Slot("201312"), 
                               Slot("201401"), Slot("20140201"), Slot("20140202")]))
        
        # different sizes
        self.assertEquals(Slot("2014").rangeto("201502"), Slot("201401").rangeto("201502"))
        self.assertEquals(Slot("2014").rangeto("201502"),  Slot("201502").rangeto("2014"))
        self.assertEquals(Slot("2014").rangeto("2017010100"), 
                          Slot("2014010100").rangeto("2017010100"))

    def testParseSlot(self):
        repo = DummyS3Repository("s3://bucket/prefix")
        self.assertEquals(parseSlotRange(":2001", repo), set([Slot("2000"), Slot("2001")]))
        self.assertEquals(parseSlotRange(":200002", repo), 
                          set([Slot("200001"), Slot("200002")]))
        self.assertEquals(parseSlotRange(":20000102", repo), 
                          set([Slot("20000101"), Slot("20000102")]))
        self.assertEquals(parseSlotRange(":2000010101", repo), 
                          repo.get_min_slot().rangeto("2000010101"))
        self.assertEquals(parseSlotRange("2099:", repo), set([Slot("2099"), Slot("2100")]))
        self.assertEquals(parseSlotRange("210011:", repo), 
                          set([Slot("210011"), Slot("210012")]))
        self.assertEquals(parseSlotRange("21001230:", repo), 
                          set([Slot("21001230"), Slot("21001231")]))
        self.assertEquals(parseSlotRange("2100123122:", repo), 
                          Slot("2100123122").rangeto(repo.get_max_slot()))
        self.assertEquals(parseSlotRange(":", repo), repo.get_min_slot().rangeto(repo.get_max_slot()))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testSlots']
    unittest.main()