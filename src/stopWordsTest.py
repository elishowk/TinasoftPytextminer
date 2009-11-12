#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest

# pytextminer modules
import PyTextMiner.StopWord

class TestStopWords(unittest.TestCase):
    def setUp(self):
        pass
        
    def test_basic(self):
        stopwords = PyTextMiner.StopWord.Collection(['cat','I','dogs'])
        string = "I like cats, but I have a dog"
        cleaned = stopwords.clean(string)
        self.assertEquals(cleaned, "like cats, but have a dog")

    def test_nltk(self):
        stopwords = PyTextMiner.StopWord.NLTKCollection()
        string = "Hello, I like cats, but my girlfriend has a dog"
        cleaned = stopwords.clean(string)
        self.assertEquals(cleaned, "Hello, I like cats, girlfriend")

    def test_file_en(self):
        stopwords = PyTextMiner.StopWord.Collection("t/stopwords/en.txt")
        string = "I like cats but I have a dog"
        cleaned = stopwords.clean(string)
        print "cleaned:", cleaned
        
    def test_file_fr(self):
        stopwords = PyTextMiner.StopWord.Collection("t/stopwords/fr.txt")
        string = "J'aime les chats mais ma petite amie a un chien"
        cleaned = stopwords.clean(string)
        print "cleaned:", cleaned
        
if __name__ == '__main__':
    unittest.main()
