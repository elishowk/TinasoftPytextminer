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
        self.datasetId = argument3
        self.period = argument5
        self.whitelist = argument4
        self.argument1 = argument1
        self.argument2 = argument2


class ExtractFile(PytextminerApiTest):

    def runTest(self):
        """ExtractFile : extract a source file's word vectors"""
        print self.tinasoft.extract_file(
                self.argument1,
                self.datasetId,
                outpath=self.whitelist,
                format=self.argument2,
                minoccs=1,
        )
        self.failIfEqual(self.whitelist, PytextminerApi.STATUS_ERROR)


class IndexFile(PytextminerApiTest):
    def runTest(self):
        """
        IndexFile :
        index a whitelist against a source file
         and its network in the db
        """
        self.failIfEqual(self.tinasoft.index_file(
                self.argument1,
                self.datasetId,
                self.whitelist,
                format=self.argument2,
                overwrite=False
            ), PytextminerApi.STATUS_ERROR
        )


class GenerateGraph(PytextminerApiTest):
    def runTest(self):
        """ExportGraph : processes a graph, and exports a gexf graph,"""
        print self.tinasoft.generate_graph(
            self.datasetId,
            self.period,
            whitelistpath = self.whitelist,
            outpath = 'test_graph',
            ngramgraphconfig={
            #    'edgethreshold': [1.0,'inf'],
            #    'nodethreshold': [1,'inf'],
            #    'alpha': 0.1,
                'proximity': self.argument1
            },
            documentgraphconfig={
            #    'edgethreshold': [1.0,'inf'],
            #    'nodethreshold': [1,'inf'],
                'proximity': self.argument2
            },
            exportedges=True
        )


class IndexArchive(PytextminerApiTest):
    def runTest(self):
        """
        IndexArchive : process cooccurences on an archive data type (see tinasoft/data/*archive.py)
        """
        def getAllSubdirectories(source_dir):
            from glob import glob
            from os.path import join
            allP=glob(join('source_files',source_dir,'*'))
            filtered = [p.split("/")[-1] for p in allP]
            return filtered

        if self.period == "all":
            self.period = getAllSubdirectories(self.argument1)
        else:
            self.period = [self.period]

        print self.period

        print self.tinasoft.index_archive(
                self.argument1,
                self.datasetId,
                self.period,
                self.whitelist,
                self.argument2,
                outpath=True,
                minCooc=10
            )


class ExportCoocMatrix(PytextminerApiTest):
    def runTest(self):
        """testF_ExportCoocMatrix"""
        print self.tinasoft.export_cooc(self.datasetId, "Pubmed_2003[dp]")

def usage():
    print " apitests.py USAGE :\n"
    print " python apitests.py ExtractFile configuration_file_path source_filename source_file_format dataset_name whitelist_out_path\n"
    print " python apitests.py IndexFile configuration_file_path source_filename source_file_format dataset_name whitelist_path\n"
    print " python apitests.py GenerateGraph configuration_file_path ngram_proximity_name document_proximity_name dataset_name whitelist_path period\n"
    print " python apitests.py IndexArchive configuration_file_path source_filename source_file_format dataset_name whitelist_path period\n"

if __name__ == '__main__':
    print sys.argv
    argument1 = None
    argument2 = None
    argument3 = None
    argument4 = None
    argument5 = None
    try:
        testclass = sys.argv[1]
        confFile = sys.argv[2]
    except Exception, e:
        print e
        usage()
        exit()
    if testclass in ['ExtractFile','IndexFile']:
        try:
            argument1 = sys.argv[3]
            argument2 = sys.argv[4]
            argument3 = sys.argv[5]
            argument4 = sys.argv[6]
            del sys.argv[2:]
        except Exception, e:
            print e
            usage()
            exit()
    if testclass in ['GenerateGraph','IndexArchive']:
        try:
            argument1 = sys.argv[3]
            argument2 = sys.argv[4]
            argument3 = sys.argv[5]
            argument4 = sys.argv[6]
            argument5 = sys.argv[7]
            del sys.argv[2:]
        except Exception, e:
            print e
            usage()
            exit()

    tinasoftSingleton = PytextminerApi(confFile)
    unittest.main()
