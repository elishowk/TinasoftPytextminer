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
            self.locale = 'fr_FR.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'en_US.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
        self.stopwords = "file://t/stopwords/en.txt" 

    def test_proposal(self):
        fet = Reader("fet://t/data-proposal.csv", 'data-proposal', locale='en_US.UTF-8')
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
