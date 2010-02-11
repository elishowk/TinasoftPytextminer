#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 15 2010 5:29:16 PM$"

# core modules
import unittest
import os, shutil
import random
import cProfile

from tinasoft import TinaApp
from tinasoft.pytextminer import *
from tinasoft.data import tinabsddb

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        #shutil.copy( 'last-fetopen.bsddb', 'tests/tinabsddb.test.bsddb' )
        self.tinasoft = TinaApp(configFile='config.yaml',storage='tinabsddb://tinabsddb.test.bsddb')
        return

    def testRead(self):
        """Test reading all NGram objects in db"""
        generator = self.tinasoft.storage.select('NGram')
        try:
            record = generator.next()
            while record:
                self.assertEqual( isinstance(record[1], dict), True)
                record = generator.next()
        except StopIteration, si:
            print "testRead ended"
            return

    def testDelete(self):
        """Tests deleting objects in the DB"""
        generator = self.tinasoft.storage.select('Ngram')
        try:
            record = generator.next()
            while record:
                self.tinasoft.storage.safedelete(record[0])
                record = generator.next()
        except StopIteration, si:
            print "testDelete ended"
            return


    def testInsert(self):
        """tests inserting objects in the db"""
        return
        self.tinasoft.storage.insertCorpora( corpora.Corpora( 'test corpora id' ) )
        self.tinasoft.storage.insertCorpus( corpus.Corpus( 'test corpus' ) )
        # creates a new entry
        self.tinasoft.storage.insertmanyCorpus( [corpus.Corpus( 'test corpus 2', period_start='2009-06-06' )] )
        self.tinasoft.storage.insertDocument( document.Document( {}, '1', 'test title', targets=['document testing content']) )
        # updates the previous entry
        self.tinasoft.storage.insertDocument( document.Document( {}, '1', 'test title 2', targets=['document testing content']) )
        # creates 100  new documents
        iter=[]
        for i in range (2,101):
            iter += [document.Document( {}, str(i), 'test title', targets=['document testing content '+str(i)])]
        self.tinasoft.storage.insertmanyDocument( iter )
        # insert one ngram
        self.tinasoft.storage.insertNGram( ngram.NGram( ['test', 'ngram'], postag=[['test','TAG1'],['ngram','TAG2']] ))
        iter=[]
        iditer=[]
        # creates 1000 new ngram records
        for i in range(1, 1000):
            ng = ngram.NGram( ['test', 'ngram'+str(i)], postag=[['test','TAG1'],['ngram'+str(i),'TAG2']] )
            iter.append( ng )
            iditer.append( ng.id )
        self.tinasoft.storage.insertmanyNGram( iter )
        del iter
        print "testInsert ended"

    def testAssoc(self):
        """test inserting association between object in the database"""
        corporaID = 'fet open'
        corpusID = '1'
        docID = 'sarajevo'
        ngramID = 'pytextminer'
        occs = 999999
        self.tinasoft.storage.insertCorpora( corpora.Corpora( corporaID ) )
        self.tinasoft.storage.insertCorpus( corpus.Corpus( corpusID ) )
        self.tinasoft.storage.insertDocument( document.Document( {}, \
            docID, 'test title', targets=['document testing content']) )
        self.tinasoft.storage.insertNGram( ngram.NGram( ['test', 'ngram'],\
            ngramID, postag=[['test','TAG1'],['ngram','TAG2']] ))
        # insert
        self.tinasoft.storage.insertAssocCorpus( corpusID, corporaID )
        self.tinasoft.storage.insertAssocDocument( docID, corpusID )
        self.tinasoft.storage.insertAssocNGramCorpus( ngramID, corpusID, occs )

        # tests insertion
        obj = self.tinasoft.storage.loadCorpora( 'fet open' )
        self.assertEqual( ('1' in obj['edges']['Corpus']), True )
        obj = self.tinasoft.storage.loadCorpus( '1' )
        self.assertEqual( ('sarajevo' in obj['edges']['Document']), True )
        self.assertEqual( ('fet open' in obj['edges']['Corpora']), True )
        self.assertEqual( ('pytextminer' in obj['edges']['NGram']), True )
        obj = self.tinasoft.storage.loadDocument('sarajevo')
        self.assertNotEqual( obj, None )
        obj = self.tinasoft.storage.loadNGram('pytextminer')
        self.assertEqual( self.tinasoft.storage.insertAssocNGramDocument( ngramID+'i', docID, occs )
, None )
        self.assertEqual( self.tinasoft.storage.insertAssocNGramCorpus( ngramID, corpusID+'j', occs )
, None )


        #gen = self.tinasoft.storage.select( 'Corpus' )
        #try:
        #    rec = gen.next()
        #    while rec:
        #        print rec
        #        rec = gen.next()
        #except StopIteration, si:
        #    print "end", si
        print "testAssoc ended"

if __name__ == '__main__':
    unittest.main()
