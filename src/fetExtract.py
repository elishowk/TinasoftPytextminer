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
from tina.storage import Storage

from tokenizerTests import TokenizerTests

class TestFetExtract(unittest.TestCase):
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

        fet = Reader("fet://t/data-proposal.csv",
            corpusName="fet_batchs.csv",
            corpusNumberField='corpusID',
            titleField='docTitle',
            datetime='2009-11-17',
            contentField='docAbstract',
            locale=self.locale,
            minSize=1,
            maxSize=4,
        )
#        corpora = PyTextMiner.Corpora(id="FetCorpora1")
        corpora = fet.corpora(corporaID="FetCorpora1")
#        corpora.corpora = [corpus]
        for corpus in corpora.corpora:
            print "corpus name :", corpus.name
            for document in corpus.documents:
                print "document title:",document.title
        data = None
        tokenizerTester = TokenizerTests( data, self.locale, self.stopwords )
        corpora = tokenizerTester.wordpunct_tokenizer( corpora )
        tokenizerTester.print_corpora( corpora )
        corpora = tokenizerTester.clean_corpora( corpora )
                
        storage = Storage("file://t/output", serializer="yaml")
        storage[corpora.id] = corpora
        storage.save()
        
        dump = Writer ("fet://t/output/ngramDocFreq.csv", corpus=corpus, locale=self.locale)
        dump.ngramDocFreq('docAbstract')

            
if __name__ == '__main__':
    unittest.main()
