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

    def testImportFile(self): pass

    def testProcessCooc(self): pass

    def testExportGraph(self):
        whitelist = self.tinasoft.importNGrams(
            '/home/elishowk/TINA/Datas/100221-fetopen-filteredngrams.csv',
            occsCol='occurrences',
        )
        path = '100223-fetopen-8.gexf'
        periods=['8']
        threshold=[0.0, 1.0]
        self.tinasoft.logger.debug( "created : " + self.tinasoft.exportGraph(path, periods, threshold, whitelist) )

    def testExportCoocMatrix(self):
        whitelist = self.tinasoft.importNGrams(
            '/home/elishowk/TINA/Datas/100221-fetopen-filteredngrams.csv',
            occsCol='occurrences',
        )
        path = '100223-fetopen-8.txt'
        periods=['8']
        self.tinasoft.logger.debug( "created : " + self.tinasoft.exportCooc(path, periods, whitelist) )


if __name__ == '__main__':
    unittest.main()
