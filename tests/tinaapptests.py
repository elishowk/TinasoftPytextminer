#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 21 2010 5:29:16 PM$"

# core modules
import unittest

from tinasoft import TinaApp
from tinasoft.pytextminer import stopwords

class TinaAppTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = TinaApp(storage='tinabsddb://test.bsddb')
        self.config = { 'datasets': {} }
        self.config['datasets']['userstopwords'] = 'user/100224-fetopen-user-stopwords.csv'
        self.whitelist = self.tinasoft.get_whitelist(
            #'/home/elishowk/TINA/Datas/100226-pubmed_whitelist.csv'
            'tests/data/pubmed_whitelist.csv'
        )
        self.userstopwordfilter=[stopwords.StopWordFilter( "file://%s" % self.config['datasets']['userstopwords'] )]

    def testA_ExtractFile(self):
        """testing extract_file"""
        #return
        self.tinasoft.extract_file(
            "tests/data/pubmed_tina_test.csv",
            "test data set",
            index=False,
            format='tina',
            overwrite=False
        )

    def testB_ImportFile(self):
        """testing import_file"""
        #return
        self.tinasoft.import_file(
            "tests/data/pubmed_tina_test.csv",
            "test data set",
            index=False,
            format="tina",
            overwrite=False
        )

    def testC_ExportWhitelist(self):
        return
        self.tinasoft.export_whitelist( \
            ['7','23'], \
            "test data set", \
            'tests/tinaapptests-exportCorpora.csv', \
            self.whitelist, \
            self.userstopwordfilter, \
            minOccs=1
        )

    def testD_ProcessCooc(self):
        #return
        #self.tinasoft.logger.debug ( self.tinasoft.storage.loadCorpora( 'pubmed test 200', raw=1 ) )
        self.tinasoft.process_cooc( \
            self.whitelist, \
            "test data set", \
            ['7','23'], \
            self.userstopwordfilter \
        )
        self.tinasoft.logger.debug( "processCooc test finished " )

    def testE_ExportGraph(self):
        #return
        corporaid = "test data set"
        path = 'tests/tinaapptests-exportGraph.gexf'
        periods=['7','23']
        self.tinasoft.export_graph(path, corporaid, periods, self.whitelist)

    def testF_ExportCoocMatrix(self):
        return
        path = 'tests/tinaapptests-exportCoocMatrix.csv'
        periods=['8']
        self.tinasoft.logger.debug( "created : " + \
            self.tinasoft.export_cooc(path, periods, self.whitelist) )

    def testG_ExportDocuments(self):
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
