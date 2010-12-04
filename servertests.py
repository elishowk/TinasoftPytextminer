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
import httplib
import urllib
httplib.HTTPConnection.debuglevel = 1
import jsonpickle

class ServerTest(unittest.TestCase):
    def setUp(self):
        self.headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }
        self.connection = httplib.HTTPConnection("localhost", 8888)
        self.datasetId = argument3
        self.period = argument5
        self.argument1 = argument1
        self.argument2 = argument2
        self.whitelist = argument4


class ExtractFile(ServerTest):
    def runTest(self):
        """ExtractFile : extract a source file's word vectors"""
        params = urllib.urlencode( {
            'path': self.argument1,
            'dataset': self.datasetId,
            'outpath': self.whitelist,
            'format': self.argument2,
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
            'path': self.argument1,
            'dataset': self.datasetId,
            'whitelistpath': self.whitelist,
            'format': self.argument2,
            'overwrite': False
        } )
        self.connection.request(
            'POST',
            '/file',
            body = params,
            headers = self.headers
        )
        print self.connection.getresponse().read()


class GenerateGraph(ServerTest):
    def runTest(self):
        """ExportGraph : processes a graph, and exports a gexf graph,"""
        params =  urllib.urlencode({
            'dataset': self.datasetId,
            'periods': self.period,
            'whitelistpath': self.whitelist,
            'outpath': 'test_graph',
            #'ngramgraphconfig': {
            #    'proximity': self.argument1
            #},
            #'documentgraphconfig': {
            #    'proximity': self.argument2
            #}
        } )
        self.connection.request(
            'POST',
            '/graph',
            body = params,
            headers = self.headers
        )
        print self.connection.getresponse().read()


def getCorpora(connection, headers, dataset_id):
    """getCorpora : gets a corpora from database"""
    params =  urllib.urlencode({
        'dataset': dataset_id,
    })
    connection.request(
        'GET',
        '/dataset?'+params,
        body = params,
        headers = headers
    )
    data = connection.getresponse().read()
    print "DATA RECEIVED - ", data
    obj = jsonpickle.decode( data )
    return obj

class GetNodes(ServerTest):
    def runTest(self):
        """GetNodes : gets nodes from database after having generated a graph"""
        Datasetlabel = getCorpora(self.connection, self.headers, self.datasetId)
        print Datasetlabel
        print "Verifying Dataset with Dataset named %s"%Datasetlabel
        params =  urllib.urlencode({
            'dataset': self.datasetId,
            'id': self.period
        })
        self.connection.request(
            'GET',
            '/corpus?'+params,
            body = params,
            headers = self.headers
        )
        data = self.connection.getresponse().read()
        print "DATA RECEIVED - ", data
        corpusObj = jsonpickle.decode( data )
        print corpusObj
        
        for ngid in corpusObj['edges']['NGram'].iterkeys():
            params =  urllib.urlencode({
                'dataset': self.datasetId,
                'id': ngid
            })
            self.connection.request(
                'GET',
                '/ngram?'+params,
                body = params,
                headers = self.headers
            )
            data = self.connection.getresponse().read()
            print "DATA RECEIVED - ", data
            ngramObj = jsonpickle.decode( data )
            print ngramObj.edges
            
        for docid in corpusObj['edges']['Document'].iterkeys():
            params =  urllib.urlencode({
                'dataset': self.datasetId,
                'id': docid
            })
            self.connection.request(
                'GET',
                '/document?'+params,
                body = params,
                headers = self.headers
            )
            data = self.connection.getresponse().read()
            print "DATA RECEIVED - ", data
            documentObj = jsonpickle.decode( data )
            print documentObj

def usage():
    print " servertests.py USAGE :\n"
    print " first, launch the server : python httpserver.py configuration_file_path \n"
    print " python servertests.py ExtractFile source_filename source_file_format dataset_name whitelist_out_path\n"
    print " python servertests.py IndexFile source_filename source_file_format dataset_name whitelist_path\n"
    print " python servertests.py GenerateGraph dataset_name whitelist_path period\n"
    print " python servertests.py GetNodes dataset_name period\n"


if __name__ == '__main__':
    print sys.argv
    argument1 = None
    argument2 = None
    argument3 = None
    argument4 = None
    argument5 = None
    try:
        testclass = sys.argv[1]
    except Exception, e:
        print e
        usage()
        exit()
    if testclass in ['ExtractFile','IndexFile']:
        try:
            argument1 = sys.argv[2]
            argument2 = sys.argv[3]
            argument3 = sys.argv[4]
            argument4 = sys.argv[5]
            del sys.argv[2:]
        except:
            usage()
            exit()
    elif testclass == 'GenerateGraph':
        try:
            argument3 = sys.argv[2]
            argument4 = sys.argv[3]
            argument5 = sys.argv[4]
            del sys.argv[2:]
        except:
            usage()
            exit()
    elif testclass == 'GetNodes':
        try:
            argument3 = sys.argv[2]
            argument5 = sys.argv[3]
            del sys.argv[2:]
        except:
            usage()
            exit()

    unittest.main()

