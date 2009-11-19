#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest
import locale

# pytextminer modules
import PyTextMiner
from PyTextMiner.Data import Reader, Writer, sqlite
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
        self.stopwords = "file://src/t/stopwords/en.txt" 

    def test_proposal(self):

        tina = Reader("tina://src/t/pubmed_AIDS.csv",
            titleField='doc_titl',
            datetimeField='doc_date',
            contentField='doc_abst',
            authorField='doc_acrnm',
            corpusNumberField='corp_num',
            docNumberField='doc_num',
            index_1='index_1',
            index_2='index_2',
            minSize='1',
            maxSize='4',
            delimiter=',',
            quotechar='"',
            locale='en_US.UTF-8',
        )
        corpora = PyTextMiner.Corpora( name="pubmedTest" )
        corpora = tina.corpora( corpora )
        print tina.corpusDict
        sql = Writer("sqlite://src/t/output/"+ corpora.name +".db", locale=self.locale)
        #sql.storeCorpora( corpora, corpora.name )
        for ( corpusNum, corpus ) in tina.corpusDict.iteritems():
            #sql.storeCorpus( corpus, corpusNum )
            #sql.storeAssocCorpus( corpusNum, corpora.name )
            for documentNum in corpus.documents:
                # TODO : check in DB
                # add Assoc if exists
                document = tina.docDict[ documentNum ]
                print "----- WordPunctTokenizer on document %s ----\n" % documentNum
                sanitizedTarget = PyTextMiner.Tokenizer.WordPunctTokenizer.sanitize(
                        document.rawContent,
                        document.forbChars,
                        document.ngramEmpty
                    )
                document.targets.add( sanitizedTarget )
                #print target.sanitizedTarget
                document.tokens = PyTextMiner.Tokenizer.WordPunctTokenizer.tokenize(
                    text = sanitizedTarget,
                    emptyString = document.ngramEmpty,
                    #stopwords = self.stopwords
                )
                #for sentence in sentenceTokens:
                #    target.tokens.append( PyTextMiner.Tokenizer.WordPunctTokenizer.filterBySize( sentence ) )
                #print target.tokens
                
                document.ngrams = PyTextMiner.Tokenizer.WordPunctTokenizer.ngramize(
                    minSize = document.ngramMin,
                    maxSize = document.ngramMax,
                    tokens = document.tokens,
                    emptyString = document.ngramEmpty, 
                    #stopwords=self.stopwords,
                )
                #print document.ngrams
                # DB Storage
                document.rawContent = ""
                document.tokens = []
                #sql.storeDocument( document, documentNum )
                #sql.storeAssocDocument( documentNum, corpusNum )
                del document
                del tina.docDict[ documentNum ]
                print len( tina.docDict )

if __name__ == '__main__':
    unittest.main()
