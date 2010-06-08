#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Feb, 19 2010 5:29:16 PM$"

# core modules
import unittest
import os

from tinasoft.pytextminer import document, corpus, corpora
from tinasoft.data import Reader
import pickle

class CoocTestCase(unittest.TestCase):
    def setUp(self):
        self.corpora = corpora.Corpora('test corpora')
        self.textcsv = 'tests/data/pubmed_tina_test.csv'
        self.corpus7docs=['16594642','16462412','16462411']
        self.corpus23docs=['16462411']

    def testImporter(self):
        tinaReader = Reader( "tina://"+self.textcsv, fields={
            'titleField': 'doc_titl',
            'contentField': 'doc_abst',
            'authorField': 'doc_acrnm',
            'corpusNumberField': 'corp_num',
            'docNumberField': 'doc_num',
            'index1Field': 'index_1',
            'index2Field': 'index_2',
            'dateField': 'date',
            'keywordsField': 'doc_keywords',
        })
        fileGenerator = tinaReader.parseFile( self.corpora )
        try:
            doccount=0
            while 1:
                doc, corpusNum = fileGenerator.next()
                doccount+=1
                self.assertTrue( isinstance( doc, document.Document ) )
                if corpusNum == '7':
                    self.assertEqual( doc['edges']['Corpus']['7'], 1 )
                elif corpusNum == '23':
                    self.assertEqual( doc['edges']['Corpus']['23'], 1 )
                #f=open( 'tests/data/'+doc['id']+'-DocumentObj.pickle', 'w' )
                #pickle.dump( doc, f )
                #del f
        except StopIteration, stop:
            self.assertEqual( doccount, 4 )
            updatedCorpora = tinaReader.corpora
            self.assertTrue( updatedCorpora == self.corpora )
            self.assertTrue( '7' in updatedCorpora['content'] )
            self.assertTrue( '23' in updatedCorpora['content'] )
            self.assertTrue( 'Corpus' in updatedCorpora['edges'] )
            self.assertTrue( '7' in updatedCorpora['edges']['Corpus'] )
            self.assertTrue( '23' in updatedCorpora['edges']['Corpus'] )
            self.assertEqual( len(updatedCorpora['edges']['Corpus']), 2 )
            self.assertTrue( self.corpora['label'] == updatedCorpora['label'] )
            self.assertTrue( self.corpora['id'] == updatedCorpora['id'] )
            self.assertTrue( isinstance( updatedCorpora, corpora.Corpora ) )
            corpuscontentcount=0
            for corpusNum in updatedCorpora['content']:
                corpuscontentcount += 1
                corp = tinaReader.corpusDict[ corpusNum ]
                self.assertTrue( isinstance( corp, corpus.Corpus ) )
                #print corp['edges']
                if corp['id'] == '7':
                    self.assertEqual( len(corp['edges']['Document']), 3 )
                    self.assertTrue( '16594642' in corp['edges']['Document'] )
                    self.assertTrue( '16462412' in corp['edges']['Document'] )
                self.assertTrue( '16462411' in corp['edges']['Document'] )
            self.assertEqual( corpuscontentcount, 4 )




if __name__ == '__main__':
    unittest.main()
