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
            #'/home/elishowk/TINA/Datas/100226-pubmed_whitelist.csv'
            'user/100221-fetopen-filteredngrams.csv',
            occsCol='occurrences',
            accept='x'
        )
        self.userstopwordfilter=[stopwords.StopWordFilter( "file://%s" % self.config['userstopwords'] )]

    def testAImportFile(self):
        return
        self.tinasoft.importFile(
            'tests/pubmed_tina_200.csv',
            'import.yaml',
            'unit test corpora',
            index=False,
            format='tina',
            overwrite=True
        )

    def testBExportCorpora(self):
        #return
        print self.tinasoft.exportCorpora( \
            ['8'], \
            'fet open', \
            'tests/tinaapptests-exportCorpora.csv', \
            self.whitelist, \
            self.userstopwordfilter
        )

    def testCProcessCooc(self):
        return
        self.tinasoft.processCooc( \
            self.whitelist, \
            'fet open', \
            ['8'], \
            self.userstopwordfilter \
        )
        self.tinasoft.logger.debug( "processCooc test finished " )

    def testDExportGraph(self):
        return
        path = 'tests/tinaapptests-exportGraph.gexf'
        periods=['8']
        threshold=[0.0, 1.0]
        self.tinasoft.logger.debug( "created : " + \
            self.tinasoft.exportGraph(path, periods, threshold, self.whitelist) )

    def testEExportCoocMatrix(self):
        return
        path = 'tests/tinaapptests-exportCoocMatrix.csv'
        periods=['8']
        self.tinasoft.logger.debug( "created : " + \
            self.tinasoft.exportCooc(path, periods, self.whitelist) )

    def testFExportDocuments(self):
        return
        from tinasoft.data import Writer
        path = 'tests/tinaapptests-exportDocuments.csv'
        exporter = Writer( 'ngram://'+path )
        return exporter.exportDocuments( self.tinasoft.storage, \
            ['8'], \
            'unit test corpora', \
        )

if __name__ == '__main__':
    unittest.main()
