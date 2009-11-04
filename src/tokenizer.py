#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# third party module
import yaml

# pytextminer package
import PyTextMiner

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        try:
            f = open("src/t/testdata.yml")
        except:
            f = open("t/testdata.yml")
        self.data = yaml.load(f)
        f.close()
    
    def test_corpus(self):
        corpora = PyTextMiner.Corpora()
        for c in self.data['corpus']:
            corpus = PyTextMiner.Corpus( name=c['name'] )
            for doc in c['documents']:
                corpus.documents += [PyTextMiner.Document(
                    rawContent=doc, 
                    title=doc['title'],
                    targets=[PyTextMiner.Target(rawTarget=doc['content'])]
                    )]
            corpora.corpora += [corpus]
        for corpus in corpora.corpora:
            for document in corpus.documents:
                for target in document.targets:
                    target.sanitizedTarget = PyTextMiner.Tokenizer.RegexpTokenizer.sanitize( input=target.rawTarget, separator=target.separator, forbiddenChars=target.forbiddenChars );
                    print target.sanitizedTarget
                    print PyTextMiner.Tokenizer.RegexpTokenizer.tokenize( text=target.sanitizedTarget, min=target.minSize, max=target.maxSize, separator=target.separator )

if __name__ == '__main__':
    unittest.main()
