#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 21 2010 5:29:16 PM$"

# core modules
import unittest
import os
import random

from tinasoft import TinaApp
from tinasoft.pytextminer import stopwords, ngram

class CoocTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = TinaApp(configFile='config.yaml',\
            storage='tinabsddb://fetopen.test.bsddb')

    def testExportGraph(self):
        whitelist = self.tinasoft.importNGrams(
            'tests/test-importNGrams.csv',
            occsCol='occurrences',
        )
        path = 'tests/tinapp-test-exportGraph.gexf'
        periods=['8']
        threshold=[0.4, 0.5]
        self.tinasoft.logger.debug( self.tinasoft.exportGraph(path, periods, threshold, whitelist) )

    # OBSOLETE, moved to data.ngram.py
    def testExportNGrams(self):
        return
        def generate(corpList):
            corpusgenerator = self.tinasoft.storage.select('Corpus')
            try:
                while 1:
                    corp=corpusgenerator.next()
                    if corp[1]['id'] in corpList:
                        yield corp[1], 'fet open'
            except StopIteration, si:
                return
        stopwd = stopwords.StopWords( "file://%s" % self.tinasoft.config['stopwords'] )
        filtertag = ngram.PosTagFilter()
        filterContent = ngram.Filter()
        filterstop = stopwords.StopWordFilter(
            'file:///home/elishowk/TINA/Datas/100126-fetopen-stopwords-from-david.csv'
        )
        filters=[stopwd,filtertag,filterContent,filterstop]
        synthesispath = '100219-fetopen-corpora-synthesis.csv'
        mergepath = '100219-fetopen-ngrams.csv'
        generator = generate(['8'])
        self.tinasoft.exportNGrams(generator, synthesispath, filters=filters, mergepath=mergepath)

if __name__ == '__main__':
    unittest.main()
