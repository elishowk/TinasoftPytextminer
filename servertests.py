import json
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
from uuid import uuid4
import httplib
import urllib
#httplib.HTTPConnection.debuglevel = 1
import jsonpickle
import json

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
        self.label = label


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
        paramsdict = {
            'dataset': self.datasetId,
            'outpath': 'test_graph',
            # Works only with json.dumps....
            #'ngramgraphconfig':
            #    'proximity': self.argument1
            #},
            #'documentgraphconfig': {
            #    'proximity': self.argument2
            #}
        }
        if self.period is not None:
            paramsdict['periods'] = self.period
        params =  urllib.urlencode(paramsdict)
        self.connection.request(
            'POST',
            '/graph',
            body = params,
            headers = self.headers
        )
        print self.connection.getresponse().read()

class DeleteOneNGram(ServerTest):
    def runTest(self):
        """
        DeleteOneNGram : chooses the first Document
        and decrements an occurrence to zero
        update database, check new value
        """
        print "selecting documents and ngrams from period %s"%self.period
        corpusObj = getObject(self.connection, self.headers, 'corpus', self.datasetId, self.period)

        documentObj1, ngid1 = self.selectUniqueInDoc(corpusObj)
        ngramObj1 = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid1)
        documentObj2, ngid2 = self.selectUnique(corpusObj)
        ngramObj2 = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid2)
#        documentObj3, ngid3 = self.selectMulti(corpusObj)
#        ngramObj3 = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid3)

        print "test case : Document-NGram == 1 and Corpus-NGram > 1"
        if documentObj1 is not None:
            updateDocumentObj1 = self.deleteNGramFromDocument(documentObj1.id, ngramObj1)
            updateNgramObj1 = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid1)
            #self.failUnlessEqual( updateNgramObj1, "",
            print "testing that Document-NGram edge was removed"
            self.failIf(\
                (ngid1 in updateDocumentObj1['edges']['NGram'].keys()),\
                "Document %s to NGram %s was NOT removed "%(documentObj1.id, ngid1)\
            )
            self.failUnless(\
                (documentObj1.id not in updateNgramObj1['edges']['Document']),\
                "NGram %s to Document %s NOT removed "%(ngid1, documentObj1.id)\
            )
            corpusObj1 = getObject(self.connection, self.headers, 'corpus', self.datasetId, self.period)
            print "testing that Corpus-NGram edge was decremented"
            self.failUnlessEqual(\
                corpusObj1['edges']['NGram'][ngid1],\
                corpusObj['edges']['NGram'][ngid1]-1,\
                "Corpus %s to NGram %s edge was NOT decremented"%(self.period, ngid1)\
            )
            self.failUnlessEqual(\
                corpusObj1['edges']['NGram'][ngid1],\
                updateNgramObj1['edges']['Corpus'][corpusObj1.id],\
                "NGram %s to Corpus %s NOT equal "%(ngid1, corpusObj1.id)\
            )
        else:
            print "Could NOT test case : Document-NGram == 1 and Corpus-NGram > 1, try another Corpus"
        
        print "test case : Document-NGram == 1 and Corpus-NGram == 1"
        if documentObj2 is not None:
            updateDocumentObj2 = self.deleteNGramFromDocument(documentObj2.id, ngramObj2)
            updateNgramObj2 = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid2)
            print "testing that Document-NGram edge was removed"
            self.failIf(\
                (ngid2 in updateDocumentObj2['edges']['NGram'].keys()),\
                "Document %s to NGram %s was NOT removed "%(documentObj1.id, ngid2)\
            )
            self.failUnless(\
                (documentObj2.id not in updateNgramObj2['edges']['Document']),\
                "NGram %s to Document %s NOT removed "%(ngid2, documentObj2.id)\
            )
            corpusObj2 = getObject(self.connection, self.headers, 'corpus', self.datasetId, self.period)
            print "testing that Corpus-NGram edge was removed"
            self.failIf(\
                (ngid2 in corpusObj2['edges']['NGram']),\
                "Corpus %s to NGram %s edge was NOT removed"%(self.period, ngid2)\
            )
            self.failUnless(\
                (corpusObj2.id not in updateNgramObj2['edges']['Corpus']),\
                "NGram %s to Corpus %s NOT removed "%(ngid2, corpusObj2.id)\
            )
        else:
            print "Could NOT test case : Document-NGram == 1 and Corpus-NGram == 1, try another Corpus"

#        print "test case : Document-NGram > 1 and Corpus-NGram > 1"
#        if documentObj3 is not None:
#            updateDocumentObj3 = self.deleteNGramFromDocument(documentObj3.id, ngramObj3)
#            updateNgramObj3 = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid3)
#            print "testing that Document-NGram edge was decremented"
#            self.failUnlessEqual(\
#                updateDocumentObj3['edges']['NGram'][ngid3],\
#                documentObj3['edges']['NGram'][ngid3]-1,\
#                "Document %s to NGram %s was NOT decremented "%(documentObj3.id, ngid3)\
#            )
#            self.failUnlessEqual(\
#                updateDocumentObj3['edges']['NGram'][ngid3],\
#                updateNgramObj3['edges']['Document'][documentObj3.id],\
#                "NGram %s to Document %s NOT equal "%(ngid3, documentObj3.id)\
#            )
#
#            corpusObj3 = getObject(self.connection, self.headers, 'corpus', self.datasetId, self.period)
#            print "testing that Corpus-NGram edge was decremented"
#            self.failUnlessEqual(\
#                corpusObj3['edges']['NGram'][ngid3],\
#                corpusObj['edges']['NGram'][ngid3],\
#                "Corpus %s to NGram %s edge was NOT decremented"%(self.period, ngid3)\
#            )
#            self.failUnlessEqual(\
#                corpusObj3['edges']['NGram'][ngid3],\
#                updateNgramObj3['edges']['Corpus'][corpusObj3.id],\
#                "NGram %s to Corpus %s NOT equal "%(ngid3, corpusObj3.id)\
#            )
#        else:
#            print "Could NOT test case : Document-NGram > 1 and Corpus-NGram > 1, try another Corpus"

    def selectMulti(self, corpusObj):
        for docid in corpusObj['edges']['Document'].keys():
            documentObj = getObject(self.connection, self.headers, 'document', self.datasetId, docid)
            for ngid in documentObj['edges']['NGram'].iterkeys():
                ngObj = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid)
                #if [ngObj.label] != ngObj['edges']['label'].keys(): continue
                if documentObj['edges']['NGram'][ngid] > 1 and corpusObj['edges']['NGram'][ngid] > 1:
                    documentObj3 = documentObj
                    ngid3 = ngid
                    #corpusId3 = corpusObj.id
                    print "choosen NGram %s: Document-NGram >1 and Corpus-NGram > 1"%ngid3
                    return documentObj3, ngid3

    def selectUnique(self, corpusObj):
        for docid in corpusObj['edges']['Document'].keys():
            documentObj = getObject(self.connection, self.headers, 'document', self.datasetId, docid)
            for ngid in documentObj['edges']['NGram'].iterkeys():
                ngObj = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid)
                if [ngObj.label] != ngObj['edges']['label'].keys(): continue
                if corpusObj['edges']['NGram'][ngid] == 1 and documentObj['edges']['NGram'][ngid] == 1:
                    documentObj2 = documentObj
                    ngid2 = ngid
                    #corpusId2 = corpusObj.id
                    print "choosen NGram %s: Document-NGram == 1 and Corpus-NGram == 1"%ngid2
                    return documentObj2, ngid2

    def selectUniqueInDoc(self, corpusObj):
        for docid in corpusObj['edges']['Document'].keys():
            documentObj = getObject(self.connection, self.headers, 'document', self.datasetId, docid)
            for ngid in documentObj['edges']['NGram'].iterkeys():
                ngObj = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid)
                if [ngObj.label] != ngObj['edges']['label'].keys(): continue
                if corpusObj['edges']['NGram'][ngid] > 1  and documentObj['edges']['NGram'][ngid] == 1:
                    documentObj1 = documentObj
                    ngid1 = ngid
                    #corpusId1 = corpusObj.id
                    print "choosen NGram %s:  Document-NGram == 1 and Corpus-NGram > 1"%ngid1
                    return documentObj1, ngid1

    def deleteNGramFromDocument(self, docid, ng):
        print "removing keyword %s from document %s"%(ng.label, docid)
        params = urllib.urlencode({
            'dataset': self.datasetId,
            'id': ng.id,
            'label': ng.label,
            'is_keyword': False,
            'docid': docid
        })
        self.connection.request(
            'DELETE',
            '/ngramform'+'?'+params,
            body = params,
            headers = self.headers
        )
        print self.connection.getresponse().read()

        print "calling graph_preprocess"
        params =  urllib.urlencode({
            'dataset': self.datasetId
        })
        self.connection.request(
            'POST',
            '/graph_preprocess',
            body = params,
            headers = self.headers
        )
        self.connection.getresponse().read()
        return getObject(\
            self.connection,\
            self.headers,\
            'document',\
            self.datasetId,\
            docid\
        )

class NGramForm(ServerTest):
    def runTest(self):
        """
        NGramForm : given a period, chooses the first Document without keyword
        - adds a keywords, update database, check new value,
        - then remove it, update the database, and check the final value
        """
        print "selecting the first document without keyword in period %s"%self.period
        corpusObj = getObject(self.connection, self.headers, 'corpus', self.datasetId, self.period)

        for docid in corpusObj['edges']['Document'].iterkeys():
            documentObj = getObject(self.connection, self.headers, 'document', self.datasetId, docid)
            if 'keyword' not in documentObj['edges'] or len(documentObj['edges']['keyword'].keys()) == 0:
                break

        if self.label is None:
            self.label = uuid4().hex
        print "adding keyword %s to document %s"%(self.label, docid)

        params =  urllib.urlencode({
            'dataset': self.datasetId,
            'id': documentObj['id'],
            'label': self.label,
            'is_keyword': True
        })
        self.connection.request(
            'POST',
            '/ngramform',
            body = params,
            headers = self.headers
        )
        answer = self.connection.getresponse().read()
        print answer
        if answer[0] == 0:
            print "keyword %s was not added"%self.label
            return

        print "calling graph_preprocess"
        params =  urllib.urlencode({
            'dataset': self.datasetId
        })
        self.connection.request(
            'POST',
            '/graph_preprocess',
            body = params,
            headers = self.headers
        )
        print self.connection.getresponse().read()

        print "checking document %s keywords"%docid
        documentObj = getObject(self.connection, self.headers, 'document', self.datasetId, docid)
        #print documentObj['edges']
        for form, ngid in documentObj['edges']['keyword'].iteritems():
            print "testing keyword %s in the graph db"%form
            self.failUnless( (ngid in documentObj['edges']['NGram']), "NGram not in document %s edges"%docid )
            self.failUnlessEqual( documentObj['edges']['NGram'][ngid], 1, "NGram %s edge weight from Document %s is NOT valid"%(ngid, docid) )
            ngramObj = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid)
            #print "NGram edges"
            #print ngramObj['edges']
            self.failUnlessEqual( ngramObj['edges']['Document'][docid], 1, "Document %s edge from NGram %s is NOT valid"%(docid, ngid) )
            print "checking keywords-Corpus edges"
            for corpid in documentObj['edges']['Corpus'].keys():
                corpusObj = getObject(self.connection, self.headers, 'corpus', self.datasetId, corpid)
                self.failUnless( (ngid in corpusObj['edges']['NGram']), "NGram %s not in Corpus %s edges"%(ngid, corpid) )
                self.failUnless( (corpid in ngramObj['edges']['Corpus']), "Corpus %s not in NGram %s edges"%(corpid, ngid) )
                weight = ngramObj['edges']['Corpus'][corpid]
                self.failUnlessEqual( corpusObj['edges']['NGram'][ngid], weight, "NGram %s edge weight from Corpus %s is NOT valid"%(ngid, corpid) )

        print "removing keyword %s from document %s"%(self.label, docid)
        params = urllib.urlencode({
            'dataset': self.datasetId,
            'id': ngid,
            'label': self.label,
            'is_keyword': True
        })
        self.connection.request(
            'DELETE',
            '/ngramform'+'?'+params,
            body = params,
            headers = self.headers
        )
        print self.connection.getresponse().read()

        print "calling graph_preprocess"
        params =  urllib.urlencode({
            'dataset': self.datasetId
        })
        self.connection.request(
            'POST',
            '/graph_preprocess',
            body = params,
            headers = self.headers
        )
        print self.connection.getresponse().read()
        print "testing NGram form %s removal from database"%self.label
        ngramObj = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid)
        # check edges of all other documents and corpus
        if ngramObj =="":
            print "NGram %s was erased from the database, testing its removal in the whole database"%ngid
            corporaResult = getObject(self.connection, self.headers, 'dataset', self.datasetId)
            for corpid in corporaResult['edges']['Corpus'].keys():
                corpusObj = getObject(self.connection, self.headers, 'corpus', self.datasetId, corpid)
                self.failIf( (ngid in corpusObj['edges']['NGram']), "NGram %s still is in Corpus %s edges"%(ngid, corpid) )
                for documentid in corpusObj['edges']['Document'].keys():
                    documentObj = getObject(self.connection, self.headers, 'document', self.datasetId, documentid)
                    self.failIf( (ngid in documentObj['edges']['NGram']), "NGram still is in document %s edges"%docid )

        else:
            print "NGram %s is still in database, testing all symmetric edges"%ngid
            for corpid in ngramObj['edges']['Corpus'].keys():
                corpusObj = getObject(self.connection, self.headers, 'corpus', self.datasetId, corpid)
                self.failUnlessEqual( ngramObj['edges']['Corpus'][corpid], corpusObj['edges']['NGram'][ngid], "edge between NGram %s and Corpus %s NOT SYMETRIC"%(ngid, corpid) )
                for documentid in ngramObj['edges']['Document'].keys():
                    documentObj = getObject(self.connection, self.headers, 'document', self.datasetId, documentid)
                    self.failUnlessEqual( ngramObj['edges']['Document'][documentid], documentObj['edges']['NGram'][ngid], "edge between NGram %s and Document %s NOT SYMETRIC"%(ngid, documentid) )



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
        """TestGraph :
        gets nodes from database after having generated a graph
        and test values agains the ones in tests/__init__.py
        """
        data = tests.get_tinacsv_test_3_data()

        corporaResult = getObject(self.connection, self.headers, 'dataset', self.datasetId)
        self.failUnless( isinstance( corporaResult, corpora.Corpora ), "dataset request failed : %s"%self.datasetId )

        corpusResult = getObject(self.connection, self.headers, 'corpus', self.datasetId, self.period)
        self.failUnless( isinstance( corpusResult, corpus.Corpus ), "corpus request failed" )

        print "Testing NGram nodes in period %s"%self.period
        for ngid in corpusResult['edges']['NGram'].iterkeys():
            ngramObj = getObject(self.connection, self.headers, 'ngram', self.datasetId, ngid)
            self.failUnless( isinstance( ngramObj, ngram.NGram ), "ngram request failed" )

            print "testing NGram %s (%s)"%(ngramObj['label'],ngramObj['id'])

            self.failUnless( ("NGram::"+ngramObj['id'] in data['nodes']), "NGram not in the graph db" )
            self.failUnlessEqual( ngramObj['label'], data['nodes']["NGram::"+ngramObj['id']]['label'], "NGram label test failed : %s"%ngramObj['label'] )
            self.failUnlessEqual( data['nodes']["NGram::"+ngramObj['id']]['weight'], corpusResult.edges['NGram'][ngramObj['id']], "Corpus-NGram weight test failed : %s"%corpusResult.edges['NGram'][ngramObj['id']] )

            for targetgraphid, weight in data['edges']["NGram::"+ngid].iteritems():
                category, targetid = targetgraphid.split("::")
                self.failUnless( ( targetid in ngramObj['edges'][category]),
                    "missing edge of NGram %s, target = %s::%s \n %s"%(ngramObj['label'], category, targetid, ngramObj['edges']) )

            for category in ["NGram", "Document"]:
                for targetid, weight in ngramObj['edges'][category].iteritems():

                    print "testing target cat=%s, target id=%s"%(category, targetid )

                    self.failUnless( ( "NGram::"+ngramObj['id'] in data['edges']),
                        "missing all ngram's edges: %s"%ngramObj['label'])
                    self.failUnless( ( category+"::"+targetid in data['edges']["NGram::"+ngramObj['id']]),
                        "invalid edge of NGram: %s, target = %s::%s"%(ngramObj['label'], category, targetid) )
                    self.failUnlessEqual( weight,  data['edges']["NGram::"+ngramObj['id']][category+"::"+targetid],
                        "bad NGram edge weight (source=%s, target=%s) : found %d, should be %d"\
                            %(ngramObj['id'], targetid, weight, data['edges']["NGram::"+ngramObj['id']][category+"::"+targetid]) )

        print "Testing Document nodes in period %s"%self.period
        for docid in corpusResult['edges']['Document'].iterkeys():
            documentObj = getObject(self.connection, self.headers, 'document', self.datasetId, docid)
            self.failUnless( isinstance( documentObj, document.Document ), "document request failed" )
            print "testing Document %s (%s)"%(documentObj['label'],documentObj['id'])

            self.failUnless( ("Document::"+documentObj['id'] in data['nodes']), "Document not in the graph db" )
            self.failUnlessEqual( documentObj['label'], data['nodes']["Document::"+documentObj['id']]['label'], "Document label test failed : %s"%documentObj['label'] )
            self.failUnlessEqual( data['nodes']["Document::"+documentObj['id']]['weight'], corpusResult.edges['Document'][documentObj['id']], "Corpus-Document weight test failed : %s"%corpusResult.edges['Document'][documentObj['id']] )

            for targetgraphid, weight in data['edges']["Document::"+docid].iteritems():
                category, targetid = targetgraphid.split("::")
                self.failUnless( ( targetid in documentObj['edges'][category]),
                    "missing edge of Document: %s, target = %s::%s"%(documentObj['label'], category, targetid) )

            for category in ["NGram", "Document"]:
                for targetid, weight in documentObj['edges'][category].iteritems():

                    print "testing target cat=%s, target id=%s"%(category, targetid)

                    self.failUnless( ( "Document::"+documentObj['id'] in data['edges']),
                        "missing all Document's edges s: %s"%documentObj['id'])
                    self.failUnless( ( category+"::"+targetid in data['edges']["Document::"+documentObj['id']]),
                        "invalid edge of Document %s : target = %s::%s"%(documentObj['id'], category, targetid)  )
                    self.failUnlessEqual( weight,  data['edges']["Document::"+documentObj['id']][category+"::"+targetid],
                        "Document weight test failed : source %s , target = %s"%(documentObj['id'],category+"::"+targetid) )

def usage():
    print " servertests.py USAGE :\n"
    print " first, launch the server : python httpserver.py configuration_file_path \n"
    print " python servertests.py ExtractFile source_filename source_file_format dataset_name whitelist_out_path\n"
    print " python servertests.py IndexFile source_filename source_file_format dataset_name whitelist_path\n"
    print " python servertests.py GenerateGraph dataset_name period\n"
    print " python servertests.py TestGraph dataset_name period\n"


if __name__ == '__main__':
    print sys.argv
    argument1 = None
    argument2 = None
    datasetId = None
    whitelist = None
    period = None
    label = None
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
            datasetId = sys.argv[2]
            if len(sys.argv)==4:
                period = sys.argv[3]
            del sys.argv[2:]
        except:
            usage()
            exit()
    elif testclass == 'NGramForm':
        try:
            datasetId = sys.argv[2]
            period = sys.argv[3]
            if len(sys.argv)>4:
                label = sys.argv[4]
            del sys.argv[2:]
        except:
            usage()
            exit()
    elif testclass == 'DeleteOneNGram':
        try:
            datasetId = sys.argv[2]
            period = sys.argv[3]
            if len(sys.argv)>4:
                label = sys.argv[4]
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

