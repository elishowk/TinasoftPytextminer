#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 21 2010 5:29:16 PM$"

# core modules
import unittest
import os
import random

from tinasoft import TinaApp
from tinasoft.pytextminer import *

class CoocTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = TinaApp(configFile='config.yaml',\
            storage='tinabsddb://fetopen.test.bsddb')
        self.filtertag = ngram.PosTagFilter()
        self.filterContent = ngram.Filter()
        self.filterstop = stopwords.StopWordFilter(
            'file:///home/elishowk/TINA/Datas/100126-fetopen-stopwords-from-david.csv'
        )

    def testAllFiltersCooc(self):
        """analyse a corpus, applying all filters available"""
        def corpusAnalyse( self, id ):
            cooc = cooccurrences.MapReduce(self.tinasoft.storage, corpus=id, filter=[self.filtertag, self.filterContent, self.filterstop])
            self.tinasoft.logger.debug( "Test : Starting walkCorpus" )
            cooc.walkCorpus()
            self.tinasoft.logger.debug( "Test : Starting writeMatrix" )
            cooc.writeMatrix()
        #corpusAnalyse( self, '1' )
        #corpusAnalyse( self, '2' )
        #corpusAnalyse( self, '3' )
        #corpusAnalyse( self, '4' )
        #corpusAnalyse( self, '5' )
        #corpusAnalyse( self, '6' )
        #corpusAnalyse( self, '7' )
        #corpusAnalyse( self, '8' )


    def testCountCooc(self):
        self.tinasoft.logger.debug( "Test : Starting selectCorpusCooc('1')" )
        corpusId = '1'
        count = 0
        generator = self.tinasoft.storage.selectCorpusCooc(corpusId)
        try:
            record = generator.next()
            while record:
                #self.assertEqual( isinstance(record[1], dict), True)
                #self.tinasoft.logger.debug( record )
                record = generator.next()
                count += 1
        except StopIteration, si:
            #self.tinasoft.logger.debug( "===End of printing the \
            #    20 first Cooc entries in the db" )
            self.tinasoft.logger.debug( "Corpus " +corpusId+ " has Cooc rows = "+ str(count))

if __name__ == '__main__':
    unittest.main()
