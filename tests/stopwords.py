#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

from tinasoft import TinaApp
from tinasoft.pytextminer import *

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = TinaApp(configFile='config.yaml',storage='tinabsddb://100125-fetopen.bsddb')

    def test_unit(self):
        stop = stopwords.StopWords( arg="file://shared/stopwords/en.txt" )
        testngram = ['i', 'am', 'a', 'test']
        obj = stop.add( testngram )
        stop.savePickle( 'tests/stopwords_save_test.pickle' )
        del stop
        fromPickle = stopwords.StopWords( arg="pickle://tests/stopwords_save_test.pickle" )
        self.assertEqual( fromPickle.contains(obj), True )

    def test_filter(self):
        selectgenerator = self.tinasoft.storage.select('NGram')
        filterstop = stopwords.StopWordFilter(
            'file:///home/elishowk/TINA/Datas/100126-fetopen-stopwords-from-david.csv'
        )
        filtergenerator = filterstop.filter(selectgenerator)
        count = 0
        try:
            filterrecord = filtergenerator.next()
            while filterrecord:
                count += 1
                filterrecord = filtergenerator.next()
        except StopIteration, si:
            print "the filter accepted : ", count
            return

if __name__ == '__main__':
    unittest.main()
