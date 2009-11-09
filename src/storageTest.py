#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

import unittest

from tina.storage import Storage
# third party module
import yaml

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
        f.close()
    def test1_regex_tokenizer_storage(self):
        tokenizerTester = TokenizerTests( self.data )
        corpora = tokenizerTester.regexp_tokenizer( 0 );
        storage = StorageTest()
        storage.test_storage( "json", corpora, "TestCorporaStore1" )
        retrievedCorpora = storage.test_retrieve( "TestCorporaStore1", "json" )
        print "---Storage Retrieval---\n"
        print retrievedCorpora
        tokenizerTester.print_corpora( retrievedCorpora )

    def test2_wordpunct_tokenizer_storage(self):
        tokenizerTester = TokenizerTests( self.data )
        corpora = tokenizerTester.wordpunct_tokenizer( 100 );
        storage = StorageTest()
        storage.test_storage( "json", corpora, "TestCorporaStore2" )
        retrievedCorpora = storage.test_retrieve( "TestCorporaStore2", "json" )
        print "---Storage Retrieval---\n"
        print retrievedCorpora
        tokenizerTester.print_corpora( retrievedCorpora )

class StorageTest: 

    def test_storage(self, backend, corpora, corporaStoreName):
        # store loaded objects
        storage = Storage("file:///tmp/pytext_tests", serializer=backend)
        storage[corporaStoreName] = corpora
        storage.save()
        # destroy storage object
        del storage
    
    def test_retrieve(self, corporaStoreName, backend):
        # retrieve previously stored objects
        storage2 = Storage("file:///tmp/pytext_tests", serializer=backend)
        return storage2[corporaStoreName]
        



if __name__ == '__main__':
    unittest.main()
