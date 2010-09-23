#!/usr/bin/python
# -*- coding: utf-8 -*-
#  Copyright (C) 2009-2011 CREA Lab, CNRS/Ecole Polytechnique UMR 7656 (Fr)
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

__author__="elishowk@nonutc.fr"
__date__ ="$Jan, 21 2010 5:29:16 PM$"

# core modules
import unittest
import sys

from tinasoft import TinaApp


class TinaAppTests(unittest.TestCase):
    def setUp(self):
        self.tinasoft = tinasoftSingleton
        self.datasetId = "test_data_set"
        self.periods = ['1','pubmed1','2']
        #self.path = "tinacsv_test_200.csv"
        #self.path = "pubmed_tina_1000.csv"
        #self.format = "tinacsv"
        #self.path = "pubmed_cancer_tina_toydb.txt"
        #self.format = "medline"
        self.path = sourceFile
        self.format = sourceFormat
        self.extracted_whitelist = 'date-test_whitelist-extract_file.csv'

class ExtractFile(TinaAppTests):
    def runTest(self):
        """ExtractFile : testing extract_file"""
        print self.tinasoft.extract_file(
                self.path,
                self.datasetId,
                #outpath=self.extracted_whitelist,
                format=self.format,
                minoccs=1,
        )
        self.failIfEqual(self.extracted_whitelist, TinaApp.STATUS_ERROR)

class IndexFile(TinaAppTests):
    def runTest(self):
        """IndexFile : testing index_file"""
        self.failIfEqual( self.tinasoft.index_file(
                self.path,
                self.datasetId,
                self.extracted_whitelist,
                format=self.format,
                overwrite=False
            ), TinaApp.STATUS_ERROR
        )



class ProcessCooc(TinaAppTests):
    def runTest(self):
        """ProcessCooc : processes and stores the cooccurrence matrix"""
        print self.tinasoft.process_cooc(
            self.datasetId,
            self.periods
        )
        self.tinasoft.logger.debug( "processCooc test finished " )

class GenerateGraph(TinaAppTests):
    def runTest(self):
        """ExportGraph : exports a gexf graph, after cooccurrences processing"""
        print self.tinasoft.generate_graph(
            self.datasetId,
            self.periods,
            whitelistpath=self.extracted_whitelist,
            outpath='test_graph',
            ngramgraphconfig={
                #'edgethreshold': [0.0,1.0],
                #'nodethreshold': [1,0],
                'alpha': 0.1,
                'proximity': 'pseudoInclusion'
            },
            documentgraphconfig={
                #'edgethreshold': [0.0,1.0],
                #'nodethreshold': [1,0],
                'proximity': 'logJaccard'
            },
        )

class ExportCoocMatrix(TinaAppTests):
    def runTest(self):
        """testF_ExportCoocMatrix : TODO"""
        print self.tinasoft.export_cooc(self.datasetId, "Pubmed_1980[dp]")

class IndexArchive(TinaAppTests):
    def runTest(self):
        print self.tinasoft.index_archive(
                self.path,
                self.datasetId,
                ["03"],
                self.extracted_whitelist,
                self.format,
                outpath=True,
                minCooc=10
            )

class ImportFile(TinaAppTests):
    def runTest(self):
        """OBSOLETE ImportFile : testing import_file"""
        return
        self.failIfEqual( self.tinasoft.import_file(
                path=self.path,
                dataset=self.datasetId,
                format=self.format
            ), TinaApp.STATUS_ERROR
        )

class ExportWhitelist(TinaAppTests):
    def runTest(self):
        """OBSOLETE ExportWhitelist : Exports a whitelist file"""
        return
        print self.tinasoft.export_whitelist(
            self.periods,
            self.datasetId,
            'test export whitelist',
            'tinaapptests-export_whitelist.csv'
        )

def usage():
    print "USAGE : python tests.py TestClass configuration_file_path source_filename file_format"

if __name__ == '__main__':
    print sys.argv
    try:
        confFile = sys.argv[2]
        sourceFile = sys.argv[3]
        sourceFormat = sys.argv[4]
        tinasoftSingleton = TinaApp(confFile)
        del sys.argv[2:]
    except:
        usage()
        exit()
    unittest.main()
