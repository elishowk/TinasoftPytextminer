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
import httplib
import urllib
httplib.HTTPConnection.debuglevel = 1

class ServerTest(unittest.TestCase):
    def setUp(self):
        self.headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }
        self.connection = httplib.HTTPConnection("localhost", 8888)
        self.datasetId = "test_data_set"
        self.periods = ['1', 'pubmed1', '2', 'FET', 'Batch_10']
        self.path = sourcePath
        self.format = sourceFormat
        self.extracted_whitelist = 'date-test_whitelist-extract_file.csv'


class ExtractFile(ServerTest):
    def runTest(self):
        """ExtractFile : extract a source file's word vectors"""
        params = urllib.urlencode( {
            'path': self.path,
            'dataset': self.datasetId,
            'outpath': self.extracted_whitelist,
            'format': self.format,
            'minoccs': 1
        } )
        self.connection.request(
                'GET',
                '/file?' + params,
                headers = self.headers
        )
        print self.connection.getresponse().read()


class IndexFile(ServerTest):
    def runTest(self):
        """
        IndexFile :
        index a whitelist against a source file
         and its network in the db
        """
        params = urllib.urlencode( {
            'path': self.path,
            'dataset': self.datasetId,
            'outpath': self.extracted_whitelist,
            'format': self.format,
            'overwrite': False
        } )
        self.connection.request(
            'POST',
            '/file',
            body = params,
            headers=self.headers
        )
        print self.connection.getresponse().read()



class GenerateGraph(ServerTest):
    def runTest(self):
        """ExportGraph : processes a graph, and exports a gexf graph,"""
        params =  urllib.urlencode({
            'id': self.datasetId,
            'periods': self.periods,
            'whitelistpath': self.extracted_whitelist,
            'outpath': 'test_graph',
            'ngramgraphconfig': {
                'proximity': 'equivalenceIndex'
            },
            'documentgraphconfig': {
                'proximity': 'logJaccard'
            },
            'exportedges': True
        } )
        self.connection.request(
            'POST',
            '/graph',
            body = params,
            headers=self.headers
        )
        print self.connection.getresponse().read()

def usage():
    print "USAGE : python servertests.py TestClass source_filename file_format"

if __name__ == '__main__':
    print sys.argv
    try:
        sourcePath = sys.argv[2]
        sourceFormat = sys.argv[3]
        del sys.argv[2:]
    except:
        usage()
        exit()
    unittest.main()
