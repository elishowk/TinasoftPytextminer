#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

import unittest

# third party module
import yaml
import locale
import pprint
# tokenizer test class
from tokenizerTests import TokenizerTests

# pytextminer package
import PyTextMiner

class Tests(unittest.TestCase):

    def setUp(self):
        try:
            f = open("src/t/testdata.yml", 'rU')
        except:
            f = open("t/testdata.yml", 'rU')
        # yaml automatically decodes from utf8
        self.data = yaml.load(f)
        f.close()   # try to determine the locale of the current system
        try:
            self.locale = 'fr_FR.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'en_US.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)

#    def test1_regex_tokenizer_storage(self):
#        tokenizerTester = TokenizerTests( self.data )
#        corpora = tokenizerTester.regexp_tokenizer( 0 );
#        storage = StorageTest()
#        storage.test_storage( "json", corpora, "TestCorporaStore1" )
#        retrievedCorpora = storage.test_retrieve( "TestCorporaStore1", "json" )
#        print "---Storage Retrieval---\n"
#        print retrievedCorpora
#        tokenizerTester.print_corpora( retrievedCorpora )

    def test1_wordpunct_tokenizer_storage(self):
        tokenizerTester = TokenizerTests( self.data, self.locale )
        corpora = tokenizerTester.wordpunct_tokenizer( 1 );
        coword = CoWordTest()
        coword.test_cowords( corpora.corpora[0], type='testType' )

class CoWordTest: 
    def test_cowords(self, corpus, type):
        sa = PyTextMiner.CoWord.SimpleAnalysis()
        cowords = sa.getCowords( corpus, type )
        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint( cowords )
    

if __name__ == '__main__':
    unittest.main()
