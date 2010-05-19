#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 21 2010 5:29:16 PM$"

# core modules
import unittest

from tinasoft import TinaApp
from tinasoft.pytextminer import stopwords
from tinasoft.data import Writer

class TinaAppTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = TinaApp(storage='tinabsddb://test.bsddb')
        self.datasetId = "test data set"
        self.periods = ['1','2']
        #self.config = { 'datasets': {} }
        #self.config['datasets']['userstopwords'] = 'user/100224-fetopen-user-stopwords.csv'
        self.whitelist = self.tinasoft.get_whitelist(
            #'/home/elishowk/TINA/Datas/100226-pubmed_whitelist.csv'
            'tests/data/pubmed_whitelist.csv'
        )
        self.userstopwordfilter=[stopwords.StopWordFilter( "file://%s" % self.tinasoft.config['datasets']['userstopwords'] )]

    def testA_ExtractFile(self):
        """testing extract_file"""
        #return
        self.tinasoft.extract_file(
            "tests/data/pubmed_tina_test.csv",
            self.datasetId,
            index=False,
            format="tina",
            overwrite=False
        )

    def testB_ImportFile(self):
        """testing import_file"""
        #return
        self.tinasoft.import_file(
            "tests/data/pubmed_tina_test.csv",
            self.datasetId,
            index=False,
            format="tina",
            overwrite=False
        )

    def testC_export_whitelist(self):
        #return
        print self.tinasoft.export_whitelist( \
            self.periods, \
            self.datasetId, \
            'tinaapptests-exportCorpora.csv', \
            self.whitelist, \
            self.userstopwordfilter, \
            minOccs=1
        )

    def testD_ProcessCooc(self):
        #return
        #self.tinasoft.logger.debug ( self.tinasoft.storage.loadCorpora( 'pubmed test 200', raw=1 ) )
        print self.tinasoft.process_cooc( \
            self.whitelist, \
            self.datasetId, \
            self.periods, \
            self.userstopwordfilter
        )
        self.tinasoft.logger.debug( "processCooc test finished " )

    def testE_ExportGraph(self):
        #return
        path = 'tinaapptests-exportGraph.gexf'
        print self.tinasoft.export_graph(path, self.datasetId, self.periods, self.whitelist)


    def testF_ExportCoocMatrix(self):
        return
        path = 'tinaapptests-exportCoocMatrix.csv'
        self.tinasoft.logger.debug( "created : " + \
            self.tinasoft.export_cooc(path, self.periods, self.whitelist) )

    def testG_ExportDocuments(self):
        return
        path = 'tinaapptests-exportDocuments.csv'
        exporter = Writer( 'ngram://'+path )
        return exporter.exportDocuments( self.tinasoft.storage, \
            self.periods, \
            self.datasetId, \
        )

if __name__ == '__main__':
    unittest.main()
