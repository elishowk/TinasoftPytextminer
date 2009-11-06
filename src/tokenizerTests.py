#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# third party module
import yaml
import pprint

# pytextminer package
import PyTextMiner

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        try:
            f = open("src/t/testdata.yml", 'rU')
        except:
            f = open("t/testdata.yml", 'rU')
        # yaml automatically decodes from utf8
        self.data = yaml.load(f)
        f.close()
    
    def test_tokenizers(self):
        corpora = PyTextMiner.Corpora()
        for c in self.data['corpus']:
            corpus = PyTextMiner.Corpus( name=c['name'] )
            for doc in c['documents']:
                content = doc['content']
                corpus.documents += [PyTextMiner.Document(
                    rawContent=doc, 
                    title=doc['title'],
                    targets=[PyTextMiner.Target(rawTarget=content)]
                    )]
            corpora.corpora += [corpus]
        for corpus in corpora.corpora:
            for document in corpus.documents:
                for target in document.targets:
                    print "----- RegexpTokenizer ----\n"
                    target.sanitizedTarget = PyTextMiner.Tokenizer.RegexpTokenizer.sanitize( input=target.rawTarget, forbiddenChars=target.forbiddenChars, emptyString=target.emptyString );
                    
                    #print target.sanitizedTarget
                    target.tokens = PyTextMiner.Tokenizer.RegexpTokenizer.tokenize( text=target.sanitizedTarget, separator=target.separator )
                    target.ngrams = PyTextMiner.Tokenizer.RegexpTokenizer.ngrams(
                        minSize=target.minSize,
                        maxSize=target.maxSize,
                        tokens=target.tokens,
                        emptyString=target.emptyString,
                    )
                    print "----- NltkTokenizer ----\n"
                    #target.sanitizedTarget = PyTextMiner.Tokenizer.WordPunctTokenizer.sanitize( input=target.rawTarget, forbiddenChars=target.forbiddenChars, emptyString=target.emptyString  );
                    #print target.sanitizedTarget
                    target.tokens = PyTextMiner.Tokenizer.WordPunctTokenizer.tokenize( text=target.sanitizedTarget )
                    target.ngrams = PyTextMiner.Tokenizer.WordPunctTokenizer.ngrams(
                        minSize=target.minSize,
                        maxSize=target.maxSize,
                        tokens=target.tokens,
                        emptyString=target.emptyString,
                    )
                    #print(target.ngrams)
                     

if __name__ == '__main__':
    unittest.main()
