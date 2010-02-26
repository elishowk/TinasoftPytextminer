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
        self.tinasoft = TinaApp(configFile='config.yaml')
                    #storage='tinabsddb://fetopen.test.bsddb')
        self.config = {}
        self.config['userstopwords'] = '/home/elishowk/TINA/Datas/100224-fetopen-user-stopwords.csv'

    def testImportFile(self):
        userstopwordfilter=stopwords.StopWordFilter( "file://%s" % self.config['userstopwords'] )
        self.tinasoft.importFile(
            'tests/pubmed_tina_200.csv',
            'import.yaml',
            'unit test corpora',
            'tests/tinaapptests-export.csv',
            overwrite=True,
            index=False,
            format='tina',
            filters=[userstopwordfilter])


    def testProcessCooc(self): pass

    def testExportGraph(self):
        return
        whitelist = self.tinasoft.importNGrams(
            '/home/elishowk/TINA/Datas/100221-fetopen-filteredngrams.csv',
            occsCol='occurrences',
        )
        path = '100223-fetopen-8.gexf'
        periods=['8']
        threshold=[0.0, 1.0]
        self.tinasoft.logger.debug( "created : " + self.tinasoft.exportGraph(path, periods, threshold, whitelist) )

    def testExportCoocMatrix(self):
        return
        whitelist = self.tinasoft.importNGrams(
            '/home/elishowk/TINA/Datas/100221-fetopen-filteredngrams.csv',
            occsCol='occurrences',
        )
        path = '100223-fetopen-8.txt'
        periods=['8']
        self.tinasoft.logger.debug( "created : " + self.tinasoft.exportCooc(path, periods, whitelist) )


if __name__ == '__main__':
    unittest.main()
