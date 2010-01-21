#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 15 2010 5:29:16 PM$"

# core modules
import unittest
import os, shutil
import random

from tinasoft import TinaApp
from tinasoft.pytextminer import *

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        return

    def testRead(self):
        """fetch all NGram object in db, tests type"""
        self.tinasoft = TinaApp(configFile='config.yaml',storage='tinabsddb://fetopen.bsddb')
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
        self.tinasoft = TinaApp(configFile='config.yaml',storage='tinabsddb://fetopen.bsddb')
        generator = self.tinasoft.storage.select('Ngram')
        try:
            record = generator.next()
            while record:
                self.tinasoft.storage.safedelete(record[0])
                record = generator.next()
        except StopIteration, si:
            print si
            return


    def testWrite(self):
        """tests insert/update"""
        ### TODO TODO
        return
        os.remove('tests/bsddb3.db')
        self.tinasoft = TinaApp(configFile='config.yaml',storage='tinabsddb://tests/bsddb3.db')
        self.tinasoft.storage.insertCorpora( corpora.Corpora( 'test corpora id' ) )
        self.tinasoft.storage.insertCorpus( corpus.Corpus( 'test corpus' ) )
        # updates the previous entry
        self.tinasoft.storage.insertmanyCorpus( [corpus.Corpus( 'test corpus', period_start='2009-06-06' )] )
        self.tinasoft.storage.insertDocument( document.Document({'text':'document testing content'}, '1', 'test title') )
        # updates the previous entry
        self.tinasoft.storage.insertmanyDocument( [document.Document('document testing content', '1', 'test title 2')] )
        self.tinasoft.storage.insertNGram( ngram.NGram( ['test', 'ngram'], postag=[['test','TAG1'],['ngram','TAG2']] ))
        iter=[]
        iditer=[]
        for i in range(0, 1000):
            # updates the first record
            #iter.append( ngram.NGram( ['test', 'ngram'], postag=[['test','TAG1'],['ngram','TAG2'+str(i)]] ) )
            # creates a new record
            ng = ngram.NGram( ['test', 'ngram'+str(i)], postag=[['test','TAG1'],['ngram'+str(i),'TAG2']] )
            iter.append( ng )
            iditer.append( ng.id )
        self.tinasoft.storage.insertmanyNGram( iter )

        assocNGramCorpus=[]
        assocNGramDocument=[]
        for id1 in iditer:
            # creates many associations
            assocNGramCorpus.append(\
                    ( str(id1), 'test corpus', str(random.randint(0,200)) )\
                )
            assocNGramDocument.append(\
                    ( str(id1), '1', str(random.randint(0,2)) )\
                )
        self.tinasoft.storage.insertmanyAssocNGramDocument( assocNGramDocument )
        self.tinasoft.storage.insertmanyAssocNGramCorpus( assocNGramCorpus )
        print self.tinasoft.storage.fetchCorpusNGramID( 'test corpus' )
        print self.tinasoft.storage.fetchDocumentNGramID( '1' )

    def testWriteAssoc(self):
        shutil.copy( 'fetopen.bsddb', 'tests/bsddb3.db' )
        self.tinasoft = TinaApp(configFile='config.yaml',storage='tinabsddb://tests/bsddb3.db')
        corporaID = 'fet open'
        corpusID = '1'
        docID = '000'
        ngramID = '111'
        occs = 999999
        self.tinasoft.storage.insertAssocCorpus( corpusID, corporaID )
        self.tinasoft.storage.insertAssocDocument( docID, corpusID )
        self.tinasoft.storage.insertAssocNGramDocument( ngramID, docID, occs )
        self.tinasoft.storage.insertAssocNGramCorpus( ngramID, corpusID, occs )



if __name__ == '__main__':
    unittest.main()
