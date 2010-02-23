#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Feb, 19 2010 5:29:16 PM$"

# core modules
import unittest
import os

from tinasoft.pytextminer import corpora, corpus
from tinasoft.data import Reader
import pickle
import jsonpickle

class FakeReader():
    def __init__(self, corpusDict):
        self.corpusDict = corpusDict

class CoocTestCase(unittest.TestCase):
    def setUp(self):
        self.corpus = {}
        self.corpus['7'] = corpus.Corpus( '7', content={ '16594642':1,'16462412':1,'16462411':1 } )
        self.corpus['23'] = corpus.Corpus( '23', content={ '16462411':1 } )

    def testExtractor(self):
        reader = FakeReader( self.corpus )
        ext = corpora.Extractor( reader, corpora.Corpora('unit test corpora') )
        for corpusid, corpusObj in self.corpus.iteritems():
            print "testing corpus = ", corpusid
            for doc in corpusObj['content']:
                f=open('tests/data/'+doc+'-DocumentObj.pickle', 'r')
                document = pickle.load(f)
                #dumpfile = open('tests/data/'+doc+'-DocumentNGrams.json', 'w')
                document, ngrams = ext.extractNGrams(None, document, corpusid, 1, 5, None, None)
                #dumpfile.write( jsonpickle.encode( ngrams ) )
                self.assertEqual( len(ngrams), len(document['edges']['NGram']) )
                if document['id'] == '16594642':
                    self.assertEqual( len(document['edges']['NGram']), 15 )
                    self.assertEqual( len(ngrams), 15)
                if document['id'] == '16462412':
                    self.assertEqual( len(document['edges']['NGram']), 30 )
                    self.assertEqual( len(ngrams), 30)
                if document['id'] == '16462411':
                    self.assertEqual( len(document['edges']['NGram']), 530 )
                    self.assertEqual( len(ngrams), 530)
                #TODO test ngram present more than 1 time
                for ngid, ng in ngrams.iteritems():
                    self.assertTrue( len(ng['content']) in range(1,6) )
                    self.assertEqual( len(ng['content']), len(ng['label'].split()) )
            if corpusid == '23':
                self.assertEqual( 530, len( reader.corpusDict[corpusid]['edges']['NGram'].keys() ) )
                self.assertEqual( 530, reduce(lambda x, y: x+y, reader.corpusDict[corpusid]['edges']['NGram'].values() ) )
            if corpusid == '7':
                self.assertEqual( 575, reduce(lambda x, y: x+y, reader.corpusDict[corpusid]['edges']['NGram'].values() ) )
                moreThanOneDocument = [(ngid, occ) for ngid, occ in reader.corpusDict[corpusid]['edges']['NGram'].iteritems() if occ > 1]
                print moreThanOneDocument
                self.assertEqual( 572, len( reader.corpusDict[corpusid]['edges']['NGram'].keys() ) )


if __name__ == '__main__':
    unittest.main()
