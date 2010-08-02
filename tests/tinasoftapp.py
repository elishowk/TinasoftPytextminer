#!/usr/bin/python
# -*- coding: utf-8 -*-
#  Copyright (C) 2010 elishowk
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.

__author__="Elias Showk"
__date__ ="$Jan, 21 2010 5:29:16 PM$"

# core modules
import unittest

#import tinasoft
#import tinasoft.pytextminer

from tinasoft import TinaApp
from tinasoft.pytextminer import stopwords
from tinasoft.data import Writer


tinasoftSingleton = TinaApp("config_unix.yaml")
#whitelistSingleton = tinasoftSingleton.import_whitelist(
#    #'/home/elishowk/TINA/Datas/100226-pubmed_whitelist.csv'
#    'tests/data/20100630-pubmed_whitelist-extract_dataset.csv', 'test_whitelist'
#)
#stopwordsSingleton = [stopwords.StopWordFilter( "file://%s" % tinasoftSingleton.config['general']['userstopwords'] )]

class TinaAppTestCase(unittest.TestCase):
    def setUp(self):
        self.tinasoft = tinasoftSingleton
        self.datasetId = "test_data_set"
        self.periods = ['1','pubmed1','2']
        #self.path = "tinacsv_test_200.csv"
        self.path = "pubmed_tina_1000.csv"
        self.format = "tinacsv"
        #self.path = "/home/elishowk/TINA/Datas/MedlineCancer/pubmed_cancer_tina_toydb.txt"
        #self.format = "medline"
        #self.whitelist = whitelistSingleton
        #self.userstopwordfilter = stopwordsSingleton
        self.extracted_whitelist = 'tests/date-pubmed_test_whitelist-extract_file.csv'

    def testA_ExtractFile(self):
        """testA_ExtractFile : testing extract_file"""
        return
        print self.tinasoft.extract_file(
                self.path,
                self.datasetId,
                outpath=self.extracted_whitelist,
                format=self.format,
                minoccs=1,
        )
        self.failIfEqual(self.extracted_whitelist, TinaApp.STATUS_ERROR)

    def testB_IndexFile(self):
        """testH_IndexFile : testing index_file"""
        return
        self.failIfEqual( self.tinasoft.index_file(
                self.path,
                self.datasetId,
                self.extracted_whitelist,
                format=self.format,
                overwrite=False
            ), TinaApp.STATUS_ERROR
        )


    def testZ_ImportFile(self):
        """testB_ImportFile : testing import_file"""
        return
        self.failIfEqual( self.tinasoft.import_file(
                path=self.path,
                dataset=self.datasetId,
                format=self.format
            ), TinaApp.STATUS_ERROR
        )

    def testC_export_whitelist(self):
        """testC_export_whitelist : Exports a whitelist file"""
        return
        print self.tinasoft.export_whitelist(
            self.periods,
            self.datasetId,
            'test export whitelist',
            'tinaapptests-export_whitelist.csv'
        )

    def testD_ProcessCooc(self):
        """testD_ProcessCooc : processes and stores the cooccurrence matrix"""
        return
        print self.tinasoft.process_cooc(
            self.datasetId,
            self.periods
        )
        self.tinasoft.logger.debug( "processCooc test finished " )

    def testE_ExportGraph(self):
        """testE_ExportGraph : exports a gexf graph, after cooccurrences processing"""
        #return
        print self.tinasoft.export_graph(
            self.datasetId,
            self.periods,
            whitelistpath=self.extracted_whitelist
        )

    def testF_ExportCoocMatrix(self):
        """testF_ExportCoocMatrix : TODO"""
        return
        path = 'tinaapptests-exportCoocMatrix.csv'
        self.tinasoft.logger.debug( "created : " + \
            self.tinasoft.export_cooc(path, self.periods, self.whitelist) )

    def testG_ExportDocuments(self):
        """testG_ExportDocuments :OBSOLETE"""
        return
        path = 'tinaapptests-exportDocuments.csv'
        exporter = Writer( 'ngram://'+path )
        return exporter.export_documents( self.tinasoft.storage, \
            self.periods, \
            self.datasetId, \
        )

if __name__ == '__main__':
    unittest.main()
