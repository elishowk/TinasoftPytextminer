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

__author__ = "elishowk@nonutc.fr"

# core modules
import unittest
import sys
from datetime import datetime
from tinasoft import PytextminerApi


class PytextminerApiTest(unittest.TestCase):

    def setUp(self):
        self.tinasoft = tinasoftSingleton
        self.datasetId = "test_data_set"
        self.periods = ['Batch_09']
        self.path = sourcePath
        self.format = sourceFormat
        self.extracted_whitelist = '%s-test_whitelist-extract_file.csv'%datetime.now().strftime("%Y%m%d")

class ExtractFile(PytextminerApiTest):

    def runTest(self):
        """ExtractFile : extract a source file's word vectors"""
        print self.tinasoft.extract_file(
                self.path,
                self.datasetId,
                outpath=self.extracted_whitelist,
                format=self.format,
                minoccs=1,
        )
        self.failIfEqual(self.extracted_whitelist, PytextminerApi.STATUS_ERROR)


class IndexFile(PytextminerApiTest):
    def runTest(self):
        """
        IndexFile :
        index a whitelist against a source file
         and its network in the db
        """
        self.failIfEqual(self.tinasoft.index_file(
                self.path,
                self.datasetId,
                self.extracted_whitelist,
                format=self.format,
                overwrite=False
            ), PytextminerApi.STATUS_ERROR
        )


class GenerateGraph(PytextminerApiTest):
    def runTest(self):
        """ExportGraph : processes a graph, and exports a gexf graph,"""
        print self.tinasoft.generate_graph(
            self.datasetId,
            self.periods,
            whitelistpath = self.extracted_whitelist,
            outpath = 'test_graph',
            ngramgraphconfig={
            #    'edgethreshold': [1.0,'inf'],
            #    'nodethreshold': [1,'inf'],
            #    'alpha': 0.1,
                'proximity': sourcePath
            },
            documentgraphconfig={
            #    'edgethreshold': [1.0,'inf'],
            #    'nodethreshold': [1,'inf'],
                'proximity': sourceFormat
            },
            exportedges=True
        )


class IndexArchive(PytextminerApiTest):
    def runTest(self):
        """
        IndexArchive : process cooccurences on an archive datatype (see tinasoft/data/*archive.py)
        example :
        python apitests.py IndexArchive config_unix.yaml directory_name_of_archive_in_source_files medlinearchive
        """
        def getAllSubdirectories(source_dir):
            from glob import glob
            from os.path import join
            allP=glob(join('source_files',source_dir,'*'))
            filtered = [p.split("/")[-1] for p in allP]
            return filtered

        #print getAllSubdirectories(self.path)
        print self.tinasoft.index_archive(
                self.path,
                self.datasetId,
                #getAllSubdirectories(self.path),
                ["Pubmed_2003[dp]"],
                self.extracted_whitelist,
                self.format,
                outpath=True,
                minCooc=10
            )


class ExportCoocMatrix(PytextminerApiTest):
    def runTest(self):
        """testF_ExportCoocMatrix"""
        print self.tinasoft.export_cooc(self.datasetId, "Pubmed_2003[dp]")

def usage():
    print "USAGE : python apitests.py TestClass configuration_file_path source_filename file_format"

if __name__ == '__main__':
    print sys.argv
    try:
        confFile = sys.argv[2]
        sourcePath = sys.argv[3]
        sourceFormat = sys.argv[4]
        del sys.argv[2:]
    except:
        usage()
        exit()
    else:
        tinasoftSingleton = PytextminerApi(confFile)
    unittest.main()
