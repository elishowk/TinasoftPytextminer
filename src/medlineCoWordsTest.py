#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# pytextminer modules
import PyTextMiner
from PyTextMiner.Data import MedlineFile
from tokenizerTests import TokenizerTests

class TestDataImporter(unittest.TestCase):
    def setUp(self):
        try:
            self.locale = 'en_US.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            pass
        
    def test_basic(self):
        corpus = MedlineFile("t/cyto_50.med.txt").corpus
        corpora = PyTextMiner.Corpora(id="TestCorpora1")
        corpora.corpora += [corpus]
        print "   corpus:",corpus.name
        for document in corpus.documents:
            print "document:",document.title
        tokenizerTester = TokenizerTests( None, self.locale )
        corpora = tokenizerTester.wordpunct_tokenizer( corpora );

class MedLineCoWordsTests():
    def __init__(self):
        pass
        



if __name__ == '__main__':
    unittest.main()
