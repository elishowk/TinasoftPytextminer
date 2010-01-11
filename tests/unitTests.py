#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest
import codecs

# third party modules
#import yaml

from tinasoft.pytextminer import *

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        return

    def test_unit(self):
        print corpora.Corpora( 'testName' )
        print corpus.Corpus( 'testId' ).id
        print document.Document( 'testcontent', 'testDocNum', 'testTitle' ).id
        print ngram.NGram( ['test', 'ngram'] ).id
        print tokenizer.RegexpTokenizer()
        print tokenizer.TreeBankWordTokenizer()
        print cooccurrences.ThreadedAnalysis()
        print tagger.TreeBankPosTagger()
        print stopwords.StopWords( arg="file://shared/stopwords/en.txt" )
        print indexer.TinaIndex('tests/')
        print app.TinaApp()
        print "end of unit tests";

    def test_import_csv(self):
        csvfile = "tests/pubmed_tina_test.csv"

if __name__ == '__main__':
    unittest.main()
