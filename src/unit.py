#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# third party modules
import yaml

# pytextminer modules
#from PyTextMiner import Corpus, Target, Document, NGram, Corpora, Parser
#from PyTextMiner import CSV
#from PyTextMiner import *

import PyTextMiner

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
        parser = PyTextMiner.Tokenizer.RegexpTokenizer()
        print parser
        print "end of unit tests";

#    def setUp(self):
#        try:
#            f = open("src/t/testdata.yml")
#        except:
#            f = open("t/testdata.yml")
#        self.data = yaml.load(f)
#        f.close()
#    
#    def test_ngrams(self):
#        for test in self.data['tests']:
#            target = Target.Target(test['original'])
#            self.assertEqual(target.ngrams, test['ngrams'], test['test'])
#
#    def test_corpus(self):
#        corpora = []
#        for c in self.data['corpus']:
#            corpus = Corpus.Corpus(name=c['name'])
#            corpus.documents = [Document.Document(rawContent=doc, title=doc['title']) for doc in c['documents']]
#            corpora += [corpus]
#        Corpora.Corpora(corpora)
#
#    def test_csv(self):
#        csvfile = open("src/t/data-proposal.csv")
#        csv = CSV.CSV(name="test-csv-corpus", file=csvfile, title=3, timestamp=9)
#        csv.parseDocs()
#        for d in csv.documents:
#            print "\n",d
#            print d.rawContent
#            print d.date

if __name__ == '__main__':
    unittest.main()
