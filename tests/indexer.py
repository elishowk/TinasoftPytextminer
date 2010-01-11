#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"

# core modules
import os
import unittest
import yaml
import locale
from tinasoft.data import Reader
from tinasoft.pytextminer import *

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        # import config yaml
        try:
            self.options = yaml.safe_load( file( "config.yaml", 'rU' ) )
        except yaml.YAMLError, exc:
            print "\nUnable to read ./config.yaml file : ", exc
            return
        try:
            self.locale = self.options['locale']
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'en_US.UTF-8'
            print "locale %s was not found, switching to en_US.UTF-8 by default", self.options['locale']
            locale.setlocale(locale.LC_ALL, self.locale)
        self.indexer =  indexer.TinaIndex( "tests/" )

    def test0index(self):
        csvinputpath = "tina://%s"%self.options['input']
        tinaImporter = Reader(csvinputpath,
            delimiter = self.options['delimiter'],
            quotechar = self.options['quotechar'],
            locale = self.locale,
            fields = self.options['fields']
        )
        corps = corpora.Corpora( name=self.options['name'] )
        corps = tinaImporter.corpora( corps )        # first indexation
        self.indexer.indexDocs( tinaImporter.docDict.values() )
        # second indexation with overwrite=False (default)

        notIndexedTwoTimes = self.indexer.indexDocs( tinaImporter.docDict.values() )
        for doc in notIndexedTwoTimes:
            compare = ( doc in tinaImporter.docDict.values() )
            self.assertEqual( compare, True )
        return

    def test1searchdoc(self):
        docId = u"16462412"
        content = u'issues\ufffd\xee\ufffd\u07eb\ufffdh\ufffd\ufffd\ufffd \ufffd\xee\ufffd\u07eb\ufffdh\ufffd\ufffd\ufffd? ? ? ?\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\x125fV\ufffd\ufffd\ufffdtV2E#\ufffdTW\x03E\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd.002; control = 71 \xb1 8.2 ng/mL; ACTH-immune = 43 \xb1 4.9 ng/mL)'
        for res in self.indexer.searchDoc( docId ):
            self.assertEqual(res['content'], content)
        return

    def test2searchcooc(self):
        ngrams = [[u'general'], [u'balance']]
        results = self.indexer.searchCooc( ngrams )
        self.assertEqual( len(results), 3 )

    def test3countcooc(self):
        ngrams = [[u'general'], [u'balance']]
        results = self.indexer.countCooc( ngrams )
        self.assertEqual( results, 3 )

if __name__ == '__main__':
    os.system('rm -rf tests/_MAIN_*')
    unittest.main()

