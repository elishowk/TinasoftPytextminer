import abc
#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# third party modules
import yaml

# textminer modules
from textminer import *

class  TestsTestCase(unittest.TestCase):
    def setUp(self):
        try:
            f = open("src/t/testdata.yml")
        except:
            f = open("t/testdata.yml")
        self.data = yaml.load(f)
        f.close()
    
    def test_ngrams(self):
        for test in self.data['tests']:
            target = Target(test['original'])
            self.assertEqual(target.ngrams, test['ngrams'], test['test'])

    def test_corpus(self):
        corpora = []
        for c in self.data['corpus']:
            corpus = Corpus(name=c['name'])
            corpus.documents = [Document(rawContent=doc['content'], title=doc['title']) for doc in c['documents']]
            corpora += [corpus]

        #for corpus in corpora:
        #    for document in corpus.documents:
        #        print "document:",document
        #        t = Target(document.content)
        #        print "ngrams:",t.ngrams


 
if __name__ == '__main__':
    unittest.main()

