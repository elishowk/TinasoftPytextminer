#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest
import locale

# initialize the system path with local dependencies and pre-built libraries
import bootstrap

# pytextminer modules
import PyTextMiner
from PyTextMiner.Data import Reader, Writer, sqlite

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

        tina = Reader("tina://src/t/pubmed_AIDS_mini.csv",
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
        #print tina.corpusDict
        sql = Writer("sqlite://src/t/output/"+ corpora.name +".db", locale=self.locale)
        # clean the DB contents
        sql.clear()
        if not sql.fetch_one( PyTextMiner.Corpora, corpora.name ) :
            sql.storeCorpora( corpora.name, corpora )
        for corpusNum in corpora.corpora:
            corpus = tina.corpusDict[ corpusNum ]
            if not sql.fetch_one( PyTextMiner.Corpus, corpusNum ) :
                sql.storeCorpus( corpusNum, corpus )
            sql.storeAssocCorpus( corpusNum, corpora.name )
            for documentNum in corpus.documents:
                # check in DB and insert Assoc if exists
                if not sql.fetch_one( PyTextMiner.Document, documentNum ):
                    document = tina.docDict[ documentNum ]
                    print "----- TreebankWordTokenizer on document %s ----\n" % documentNum
                    sanitizedTarget = PyTextMiner.Tokenizer.TreeBankWordTokenizer.sanitize(
                            document.rawContent,
                            document.forbChars,
                            document.ngramEmpty
                        )
                    document.targets.add( sanitizedTarget )
                    #print target.sanitizedTarget
                    sentenceTokens = PyTextMiner.Tokenizer.TreeBankWordTokenizer.tokenize(
                        text = sanitizedTarget,
                        emptyString = document.ngramEmpty,
                        #stopwords = self.stopwords
                    )
                    for sentence in sentenceTokens:
                        document.tokens.append( PyTextMiner.Tagger.TreeBankPosTagger.posTag( sentence ) )
                    #print document.tokens
                    
                    document.ngrams = PyTextMiner.Tokenizer.TreeBankWordTokenizer.ngramize(
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
                    document.targets = set()
                    sql.storeDocument( documentNum, document )
                    
                    del document
                    del tina.docDict[ documentNum ]
                # insert Doc-Corpus association
                sql.storeAssocDocument( documentNum, corpusNum )
                print ">> %d documents left to analyse\n" % len( tina.docDict )
            del corpus
            del tina.corpusDict[ corpusNum ]
        del corpora
        del tina

if __name__ == '__main__':
    unittest.main()
