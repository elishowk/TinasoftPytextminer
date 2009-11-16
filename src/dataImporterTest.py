#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# pytextminer modules
from PyTextMiner.Data import Reader

class TestDataImporter(unittest.TestCase):
    def setUp(self):
        pass
        
    def test_basic(self):
        medline = Reader("medline://t/cyto_50.med.txt")
        corpus = medline.corpus
        print "   corpus:",corpus.name
        for document in corpus.documents:
            print "document:",document.title

if __name__ == '__main__':
    unittest.main()
