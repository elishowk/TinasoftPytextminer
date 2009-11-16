#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest
import locale

# pytextminer modules
import PyTextMiner
from PyTextMiner.Data import fet, Reader, Writer
from tina.storage import Storage

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

        fet = PyTextMiner.Data.fet.Importer(
            "t/data-proposal.csv",
            corpusName="fet://t/data-proposal.csv",
            corpusNumberField=0,
            titleField='docTitle',
            datetime='2009-11-17',
            contentField='docAbstract',
            locale=self.locale,
            minSize=1,
            maxSize=4,
        )
        corpora = PyTextMiner.Corpora(id="TestCorpora1")
        corpus = fet.corpus
        corpora.corpora = [corpus]
        print "corpus name :",corpus.name
        for document in corpus.documents:
            print "document title:",document.title
        data = None
        tokenizerTester = TokenizerTests( data, self.locale, self.stopwords )
        corpora = tokenizerTester.wordpunct_tokenizer( corpora )
        storage = Storage("file://t/output", serializer="yaml")
        storage[corpora.id] = corpora
        storage.save()
        del storage
        del corpora
        storage2 = Storage("file://t/output", serializer="yaml")
        corpora2 = storage2[ "TestCorpora1" ]
        tokenizerTester.print_corpora( corpora2 )

            
if __name__ == '__main__':
    unittest.main()
