#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Jan, 15 2010 5:29:16 PM$"

# core modules
import unittest
import os
import random

from tinasoft import TinaApp
from tinasoft.pytextminer import *

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        os.remove('tests/bsddb3.db')
        self.tinasoft = TinaApp(configFile='config.yaml',storage='tinabsddb://tests/bsddb3.db')
        print self.tinasoft.storage

    def testRead(self):
        self.tinasoft.storage.insertCorpora( corpora.Corpora( 'test corpora id' ) )
        self.tinasoft.storage.insertCorpus( corpus.Corpus( 'test corpus' ) )
        # updates the previous entry
        self.tinasoft.storage.insertmanyCorpus( [corpus.Corpus( 'test corpus', period_start='2009-06-06' )] )
        self.tinasoft.storage.insertDocument( document.Document('document testing content', '1', 'test title') )
        # updates the previous entry
        self.tinasoft.storage.insertmanyDocument( [document.Document('document testing content', '1', 'test title 2')] )
        self.tinasoft.storage.insertNGram( ngram.NGram( ['test', 'ngram'], postag=[['test','TAG1'],['ngram','TAG2']] ))
        iter=[]
        for i in range(0, 100):
            # updates the first record
            iter.append( ngram.NGram( ['test', 'ngram'], postag=[['test','TAG1'],['ngram','TAG2'+str(i)]] ) )
            # creates a new record
            iter.append( ngram.NGram( ['test', 'ngram'+str(i)], postag=[['test','TAG1'],['ngram'+str(i),'TAG2']] ) )
        self.tinasoft.storage.insertmanyNGram( iter )

        assocNGramCorpus=[]
        assocNGramDocument=[]
        for id1 in range(1, 10):
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


if __name__ == '__main__':
    unittest.main()
