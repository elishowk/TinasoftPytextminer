#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# third party modules
import yaml

# pytextminer modules
from PyTextMiner import Corpus, Target, Document, NGram, Corpora, Parser
from PyTextMiner.Corpus import CSV

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        try:
            f = open("src/t/testdata.yml")
            self.csv = open("src/t/data-proposal.csv")
        except:
            f = open("t/testdata.yml")
        self.data = yaml.load(f)
        f.close()
    
    def test_ngrams(self):
        for test in self.data['tests']:
            target = Target.Target(test['original'])
            self.assertEqual(target.ngrams, test['ngrams'], test['test'])

    def test_corpus(self):
        corpora = []
        for c in self.data['corpus']:
            corpus = Corpus.Corpus(name=c['name'])
            corpus.documents = [Document.Document(rawContent=doc, title=doc['title']) for doc in c['documents']]
            corpora += [corpus]
        Corpora.Corpora(corpora)

    def test_csv(self):
        csv = CSV.CSV("test-csv-corpus", self.csv, 3, 9)
        csv.parseDocs()
        for d in csv.documents:
            print "\n",d
            print d.rawContent
            print d.datetime

if __name__ == '__main__':
    unittest.main()
