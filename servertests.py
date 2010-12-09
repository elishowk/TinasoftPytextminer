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

import tests

from tinasoft.pytextminer import corpora, corpus, document, ngram

class ServerTest(unittest.TestCase):
    def setUp(self):
        self.headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }
        self.connection = httplib.HTTPConnection("localhost", 8888)
        self.datasetId = datasetId
        self.period = period
        self.argument1 = argument1
        self.argument2 = argument2
        self.whitelist = whitelist


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
        params = urllib.urlencode({
            'path': self.argument1,
            'dataset': self.datasetId,
            'whitelistpath': self.whitelist,
            'format': self.argument2,
            'overwrite': False
        })
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
            # DOES ONT WORK : urlencode fails
            #'ngramgraphconfig': {
            #    'proximity': self.argument1
            #},
            #'documentgraphconfig': {
            #    'proximity': self.argument2
            #}
        })
        self.connection.request(
            'POST',
            '/graph',
            body = params,
            headers = self.headers
        )
        print self.connection.getresponse().read()


def getObject(connection, headers, object_type, dataset_id, obj_id=None):
    """getCorpora : gets a corpora from database"""
    if obj_id is None:
        params =  urllib.urlencode({
            'dataset': dataset_id
        })
    else:
        params =  urllib.urlencode({
            'dataset': dataset_id,
            'id': obj_id
        })
    connection.request(
        'GET',
        '/'+object_type+'?'+params,
        body = params,
        headers = headers
    )
    data = connection.getresponse().read()
    obj = jsonpickle.decode(data)
    return obj

class TestGraph(ServerTest):
    def runTest(self):
        """GetNodes : gets nodes from database after having generated a graph"""
        data = tests.get_tinacsv_test_3_data()
        
        corporaResult = getObject(self.connection, self.headers, 'dataset', self.datasetId)
        self.failUnless( isinstance( corporaResult, corpora.Corpora ), "dataset request failed : %s"%self.datasetId )
        
        corpusResult = getObject(self.connection, self.headers, 'corpus', self.datasetId, self.period)
        self.failUnless( isinstance( corpusResult, corpus.Corpus ), "corpus request failed" )
        print "Testing the NGram nodes in period %s"%self.period
        for ngid in corpusResult['edges']['NGram'].iterkeys():
            ngramObj = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid)
            self.failUnless( isinstance( ngramObj, ngram.NGram ), "ngram request failed" )
            print "testing NGram %s (%s)"%(ngramObj['label'],ngramObj['id'])
            self.failUnless( ("NGram::"+ngramObj['id'] in data['nodes']), "ngram not in the graph db" )
            self.failUnlessEqual( ngramObj['label'], data['nodes']["NGram::"+ngramObj['id']]['label'], "ngram label test failed : %s"%ngramObj['label'] )
            self.failUnlessEqual( data['nodes']["NGram::"+ngramObj['id']]['weight'], corpusResult.edges['NGram'][ngramObj['id']], "ngram weight test failed : %s"%corpusResult.edges['NGram'][ngramObj['id']] )
        print "Testing the Document nodes in period %s"%self.period
        for docid in corpusResult['edges']['Document'].iterkeys():
            documentObj = getObject(self.connection, self.headers, 'document', self.datasetId, docid)
            self.failUnless( isinstance( documentObj, document.Document ), "document request failed" )


def usage():
    print " servertests.py USAGE :\n"
    print " first, launch the server : python httpserver.py configuration_file_path \n"
    print " python servertests.py ExtractFile source_filename source_file_format dataset_name whitelist_out_path\n"
    print " python servertests.py IndexFile source_filename source_file_format dataset_name whitelist_path\n"
    print " python servertests.py GenerateGraph dataset_name whitelist_path period\n"
    print " python servertests.py TestGraph dataset_name period\n"


if __name__ == '__main__':
    print sys.argv
    argument1 = None
    argument2 = None
    datasetId = None
    whitelist = None
    period = None
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
            datasetId = sys.argv[4]
            whitelist = sys.argv[5]
            del sys.argv[2:]
        except:
            usage()
            exit()
    elif testclass == 'GenerateGraph':
        try:
            #argument1 = sys.argv[2]
            #argument2 = sys.argv[3]
            datasetId = sys.argv[2]
            whitelist = sys.argv[3]
            period = sys.argv[4]
            del sys.argv[2:]
        except:
            usage()
            exit()
    elif testclass == 'TestGraph':
        try:
            datasetId = sys.argv[2]
            period = sys.argv[3]
            del sys.argv[2:]
        except:
            usage()
            exit()

    unittest.main()

