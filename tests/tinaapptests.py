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

class TinaAppTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = TinaApp(configFile='config.yaml',
            storage='tinabsddb://test.bsddb')
        self.config = {}
        self.config['userstopwords'] = 'user/100224-fetopen-user-stopwords.csv'
        self.whitelist = self.tinasoft.getWhitelist(
            #'/home/elishowk/TINA/Datas/100226-pubmed_whitelist.csv'
            'user/100221-fetopen-filteredngrams.csv'
        )
        self.userstopwordfilter=[stopwords.StopWordFilter( "file://%s" % self.config['userstopwords'] )]

    def testExtractWhitelist(self):
        self.tinasoft.extractWhitelist(
            '/home/elishowk/TINA/Datas/pubmed_cancer_tina_toydb.txt',
            'import.yaml',
            'pubmed cancer',
            index=False,
            format='medline',
            overwrite=False
        )

    def testAImportFile(self):
        return
        self.tinasoft.importFile(
            '/home/elishowk/TINA/Datas/pubmed_cancer_tina_toydb.txt',
            'import.yaml',
            'pubmed cancer',
            index=False,
            format='medline',
            overwrite=False
        )

    def testBExportCorpora(self):
        return
        print self.tinasoft.exportCorpora( \
            ['8'], \
            'fet open', \
            'tests/tinaapptests-exportCorpora.csv', \
            self.whitelist, \
            self.userstopwordfilter
        )

    def testCProcessCooc(self):
        return
        #self.tinasoft.logger.debug ( self.tinasoft.storage.loadCorpora( 'pubmed test 200', raw=1 ) )
        self.tinasoft.processCooc( \
            self.whitelist, \
            'pubmed test 1000', \
            ['1','2'], \
            self.userstopwordfilter \
        )
        self.tinasoft.logger.debug( "processCooc test finished " )

    def testDExportGraph(self):
        return
        path = 'tests/tinaapptests1000-exportGraph.gexf'
        periods=['1','2']
        opts={
            'DocumentGraph': {
                'threshold': [0, 'inf'],
            },
            'NGramGraph': {
                'threshold': [0.0, 1.0],
            }
        }
        self.tinasoft.exportGraph(path, periods, opts, self.whitelist)

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
