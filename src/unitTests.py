#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# third party modules
import yaml

import PyTextMiner
from CSVTextMiner import CSVTextMiner

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        return

    def test_unit(self):
        pytextminer = PyTextMiner.PyTextMiner()
        print pytextminer
        corpora = PyTextMiner.Corpora()
        print corpora
        corpus = PyTextMiner.Corpus( name='test' )
        print corpus
        document = PyTextMiner.Document( rawContent='testcontent' )
        print document
        target = PyTextMiner.Target( rawTarget='rawtarget' )
        print target
        ngram = PyTextMiner.NGram( ngram='test ngram' )
        print ngram
        regtokenizer = PyTextMiner.Tokenizer.RegexpTokenizer()
        print regtokenizer
        nltktokenizer = PyTextMiner.Tokenizer.WordPunctTokenizer()
        print nltktokenizer
        threadedanalizer = PyTextMiner.CoWord.SimpleAnalysis()
        print threadedanalizer
        print "end of unit tests";

    def test_csv(self):
        csvfile = open("t/data-proposal.csv")
        csvApp = CSVTextMiner(corpusName="test-csv-corpus", file=csvfile, titleField='proposal_title', timestampField='date', contentField='abstract', corporaID='testCorporaID')
        print csvApp
        print "end of CSVTextMiner() unit tests";

if __name__ == '__main__':
    unittest.main()
