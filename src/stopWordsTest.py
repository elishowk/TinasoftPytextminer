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
        stopwords = PyTextMiner.StopWord.Collection(['cat','I','a','dog'])
        self.assertEquals(stopwords.clean(
           "test_1 I like cats, but you have a dog"),
           "test_1 like cats, but you have")
        

        stopwords = PyTextMiner.StopWord.Collection([["I","like"],
                                                     ["you","have"]])
        self.assertEquals(stopwords.clean(
           "test_2 I like cats, but you have a dog"),
           "test_2 I like cats, but you have a dog")
        assert stopwords.contains(["I", "like"])
        assert stopwords.contains(["you", "have"])
                    
        # you can pass a list of n-grams with unconsistent size
        stopwords = PyTextMiner.StopWord.Collection([["dog"],
                                                     ["I","like"],
                                                     ["you","have"]])
        self.assertEquals(stopwords.clean(
           "test_3 I like cats, but you have a dog"), 
           "test_3 I like cats, but you have a")
        assert stopwords.contains(["I", "like"])
        assert stopwords.contains(["you", "have"])
        assert stopwords.contains(["dog"])
        
        # finally, you can pass a full database of ngrams          
        stopwords = PyTextMiner.StopWord.Collection([
            [["cat"],["dog"], ["you"]], # one grams
            [["I","like"],["you","have"]]]) # two-grams

        self.assertEquals(stopwords.clean(
           "test_4 I like cats, but you have a dog" ), 
           "test_4 I like cats, but have a")
        assert stopwords.contains(["I", "like"])
        assert stopwords.contains(["you", "have"])
        assert stopwords.contains(["dog"])
        assert stopwords.contains(["cat"])
        assert stopwords.contains(["you"])
        
    def test_nltk_en(self): 
        stopwords = PyTextMiner.StopWord.NLTKCollection(locale="en_US")
        self.assertEquals(
              stopwords.clean(
                 "Hello, I like cats, but my girlfriend has a dog"),
                 "Hello, like cats, girlfriend dog")
                 
    def test_nltk_fr(self):    
        stopwords = PyTextMiner.StopWord.NLTKCollection(locale="fr_FR")
        self.assertEquals(
              stopwords.clean(
                 "J'aime les chats mais ma petite amie a un chien"),
                 "J'aime les chats petite amie a chien")
        
    def test_file_en(self):
        stopwords = PyTextMiner.StopWord.Collection("t/stopwords/en.txt", locale="en_US")
        string = "I like cats but I have a dog"
        cleaned = stopwords.clean(string)
        self.assertEquals(cleaned, "I cats I a dog")
        
    def test_file_fr(self):
        stopwords = PyTextMiner.StopWord.Collection("t/stopwords/fr.txt", locale="fr_FR")
        string = "J'aime les chats mais ma petite amie a un chien"
        cleaned = stopwords.clean(string)
        self.assertEquals(cleaned, "J'aime chats ma petite amie a un chien")
        assert stopwords.contains(["mm²"]) # utf8 test
        assert stopwords.contains(["pour", "qu'"])
        assert stopwords.contains(["soixante", "et", "onze"])
        
if __name__ == '__main__':
    unittest.main()
