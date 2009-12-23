#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# third party modules
#import yaml

from tinasoft.pytextminer import *

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        return

    def test_unit(self):
        from tinasoft.pytextminer import PyTextMiner
        pytextminer = PyTextMiner()
        print pytextminer
        print corpora.Corpora( name='test' )
        print corpus.Corpus( name='test' )
        print document.Document( rawContent='testcontent' )
        print ngram.NGram( original=['test', 'ngram'] )
        print tokenizer.TreeBankWordTokenizer()
        print coword.ThreadedAnalysis()
        print tagger.TreeBankPosTagger()
        print stopwords.StopWords( arg="file://shared/stopwords/en.txt" )
        print "end of unit tests";

    def test_csv(self):
        csvfile = open("tests/pubmed_tina_test.csv")
        print "end of CSVTextMiner() unit tests";

if __name__ == '__main__':
    unittest.main()
