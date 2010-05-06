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
        self.tinasoft = TinaApp(configFile='config.yaml')
        self.config = {}
        self.config['userstopwords'] = '/home/elishowk/TINA/Datas/100224-fetopen-user-stopwords.csv'
        self.whitelist = self.tinasoft.get_whitelist(
            'user/100221-fetopen-filteredngrams.csv'
        )
        self.userstopwordfilter=[stopwords.StopWordFilter( "file://%s" % self.config['userstopwords'] )]
        self.periods=['1','2','3','4','5','6','7','8']

    def testAllFiltersCooc(self):
        """analyse a list of corpus, applying all filters available"""
        for id in self.periods:
            try:
                cooc = cooccurrences.MapReduce(self.tinasoft.storage, self.whitelist, \
                corpusid=id, filter=self.userstopwordfilter )
            except Warning, warner:
                continue
            if cooc.walkCorpus() is False:
                self.tinasoft.logger.error("Error in walkCorpus = %s"%id)
            if cooc.writeMatrix(True) is False:
                self.tinasoft.logger.error("Error in writeMatrix = %s"%id)
            del cooc


    def testCountCooc(self):
        return
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
