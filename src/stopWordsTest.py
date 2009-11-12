#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# third party modules
import yaml

# pytextminer modules
import PyTextMiner.StopWord
#from PyTextMiner import CSV
#from PyTextMiner import *

import PyTextMiner
from CSVTextMiner import CSVTextMiner

class TestsTestCase(unittest.TestCase):
    def setUp(self):
        return

    def test_basic(self):
        
        stopwords = PyTextMiner.StopWord.Collection(['cat','dog'])
        
        string = "I like cats but I have a dog"
        
        print "original string:", string
        cleaned = stopwords.clean(string)
        print "cleaned string:", cleaned
        
        self.assertEquals(cleaned, "I like cats but I have a", "'dog' is removed")

    def test_nltk(self):
        
        stopwords = PyTextMiner.StopWord.NLTKCollection()
        
        string = "I like cats but I have a dog"

        print "original string:", string
        cleaned = stopwords.clean(string)
        print "cleaned string:", cleaned


if __name__ == '__main__':
    unittest.main()
