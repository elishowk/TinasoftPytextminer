#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest
import locale

# pytextminer modules
import PyTextMiner
from PyTextMiner.Data import Reader, Writer
from tokenizerTests import TokenizerTests

class TestData(unittest.TestCase):
    def setUp(self):
        # try to determine the locale of the current system
        try:
            self.locale = 'en_US.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'fr_FR.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
        self.stopwords = "file://t/stopwords/en.txt" 

    def test_proposal(self):
        fet = Reader( filepath="fet://t/data-proposal.csv",
            corpusname="fet://t/data-proposal.csv",
            titleField='docTitle',
            datetime='2009-17-11',
            contentField='docAbstract',
            locale=self.locale,
            minSize=1,
            maxSize=4,
        )
        corpora = PyTextMiner.Corpora(id="TestCorpora1")
        corpus = fet.corpus
        corpora.corpora = [corpus]
        print "   corpus:",corpus.name
        for document in corpus.documents:
            print "document:",document.title
        data = None
        tokenizerTester = TokenizerTests( data, self.locale, self.stopwords )
        corpora = tokenizerTester.wordpunct_tokenizer( corpora );
            
if __name__ == '__main__':
    unittest.main()
