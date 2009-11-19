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

        fet = Reader("fet://t/pubmed_AIDS.csv",
            titleField='prop_titl',
            datetimeField='date',
            contentField='abst',
            authorField='prop_acrnm',
            corpusNumberField='Batch',
            docNumberField='prop_num',
            sumCostField='sum_cost',
            sumGrantField='sum_grant',
            minSize='1',
            maxSize='4',
            delimiter=',',
            quotechar='"',
            locale='en_US.UTF-8',
        )
        corpora = PyTextMiner.Corpora( name="pubmedTest" )
        corpora = fet.corpora( corpora )
        count = 0
        #for corpus in corpora.corpora:
            #print "corpus.id() = ", corpus
            #while count < 20:
                #for document in corpus.documents:
                #    count += 1
                #    print "document.id() = ", document
                #    for target in targets:
                #        print "target.id() = ", target
        print len(corpora.corpora)
        #data = None
        #tokenizerTester = TokenizerTests( data, self.locale, self.stopwords )
        #corpora = tokenizerTester.wordpunct_tokenizer( corpora )
        #tokenizerTester.print_corpora( corpora )
        #corpora = tokenizerTester.clean_corpora( corpora )
        #storage = Storage("file://t/output", serializer="json")
        #storage[corpora.id] = corpora
        #storage.save()
        #for corpus in corpora.corpora:
        #    dump = Writer ("fet://t/output/"+corpus.number+"ngramDocFreq.csv", corpus=corpus, locale=self.locale)
        #    dump.ngramDocFreq()

            
if __name__ == '__main__':
    unittest.main()
