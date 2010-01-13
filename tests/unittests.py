#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest
import codecs

from tinasoft import TinaApp
from tinasoft.pytextminer import *
from tinasoft.data import Engine, Handler, Importer, Exporter, sqlapi, sqlite, medline

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        return

    def test_pytextminer(self):
        print TinaApp()
        print corpora.Corpora( 'testName' )
        print corpus.Corpus( 'testId' ).id
        print document.Document( 'testcontent', 'testDocNum', 'testTitle' ).id
        print ngram.NGram( ['test', 'ngram'] ).id
        print tokenizer.RegexpTokenizer()
        print tokenizer.TreeBankWordTokenizer()
        print tagger.TreeBankPosTagger()
        print stopwords.StopWords( arg="file://shared/stopwords/en.txt" )
        print indexer.TinaIndex('tests/')
        print cooccurrences.Simple()
        print cooccurrences.Multiprocessing()
        #print app.TinaApp()
        print "end of pytextminer unit tests";

    def test_import_csv(self):
        csvfile = codecs.open("tests/pubmed_tina_test.csv")

    def test_data(self):
        print Handler()
        print Importer()
        print Exporter()
        print sqlapi.Api()
        print sqlite.Backend("tests/unittests.db")
        print sqlite.Engine("tests/unittests.db")
        print Engine("sqlite://tests/unittests.db")
        print "end of data unit tests";

if __name__ == '__main__':
    unittest.main()
