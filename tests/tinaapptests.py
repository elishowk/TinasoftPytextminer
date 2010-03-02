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
        self.whitelist = self.tinasoft.getWhitelist(
            '/home/elishowk/TINA/Datas/100226-pubmed_whitelist.csv'
        )
        self.userstopwordfilter=[stopwords.StopWordFilter( "file://%s" % self.config['userstopwords'] )]

    def testImportFile(self):
        return
        self.tinasoft.importFile(
            'tests/pubmed_tina_200.csv',
            'import.yaml',
            'unit test corpora',
            overwrite=True,
            index=False,
            format='tina'
        )

    def testExportCorpora(self):
        #return
        print self.tinasoft.exportCorpora( \
            ['1'], \
            'unit test corpora', \
            'tests/tinaapptests-exportCorpora.csv', \
            self.whitelist, \
            self.userstopwordfilter
        )

    def testProcessCooc(self):
        return
        self.tinasoft.processCooc( \
            self.whitelist, \
            'unit test corpora', \
            ['1'], \
            self.userstopwordfilter \
        )
        self.tinasoft.logger.debug( "processCooc test finished " )

    def testExportGraph(self):
        return
        path = 'tests/tinaapptests-exportGraph.gexf'
        periods=['1']
        threshold=[0.0, 1.0]
        self.tinasoft.logger.debug( "created : " + \
            self.tinasoft.exportGraph(path, periods, threshold, self.whitelist) )

    def testExportCoocMatrix(self):
        return
        path = 'tests/tinaapptests-exportCoocMatrix.txt'
        periods=['1']
        self.tinasoft.logger.debug( "created : " + \
            self.tinasoft.exportCooc(path, periods, self.whitelist) )


if __name__ == '__main__':
    unittest.main()
