#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

from tinasoft import TinaApp
from tinasoft.pytextminer import *

tinaapp = TinaApp(configFile='config.yaml',storage='tinabsddb://fetopen.test.bsddb')
mock =

class TestsTestCase(unittest.TestCase):
    def setUp(self): pass

    def test_stopword_save(self):
        """tests addition and pickling of a StopWord object"""
        stop = stopwords.StopWords( arg="file://shared/stopwords/en.txt" )
        testngram = ['i', 'am', 'a', 'test']
        obj = stop.add( testngram )
        stop.savePickle( 'tests/stopwords_save_test.pickle' )
        del stop
        fromPickle = stopwords.StopWords( arg = "pickle://tests/stopwords_save_test.pickle" )
        self.assertEqual( fromPickle.contains(obj), True )

    def test_contains(self):
        """tests the contains() method of StopWord"""
        stop = stopwords.StopWords( arg="file://shared/stopwords/en.txt" )
        ngramFalse = ngram.NGram(['i', 'am', 'a', 'test'])
        ngramFalse2 = ngram.NGram(['test'])
        ngramTrue = ngram.NGram(['all'])
        self.assertEqual( stop.contains(ngramFalse), False )
        self.assertEqual( stop.contains(ngramFalse2), False )
        self.assertEqual( stop.contains(ngramTrue), True )

    def test_filter(self):
        """db cursor filtering using a user stopwords list"""
        selectgenerator = tinaapp.storage.select('NGram')
        filterstop = stopwords.StopWordFilter(
            'file:///home/elishowk/TINA/Datas/100126-fetopen-stopwords-from-david.csv'
        )
        filtergenerator = filterstop.any(selectgenerator)
        count = 0
        try:
            while count<50:
                count += 1
                filterrecord = filtergenerator.next()
            raise StopIteration()
        except StopIteration, si:
            self.assertEqual(count, 50)
            return

if __name__ == '__main__':
    unittest.main()
