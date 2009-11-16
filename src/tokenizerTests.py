#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 5:29:16 PM$"

# core modules
import unittest
import locale

# third party module
import yaml

# pytextminer package
import PyTextMiner


class TestsTestCase(unittest.TestCase):

    def setUp(self):
        try:
            f = open("src/t/testdata.yml", 'rU')
        except:
            f = open("t/testdata.yml", 'rU')
        # yaml automatically decodes from utf8
        self.data = yaml.load(f)
        f.close()

        # try to determine the locale of the current system
        try:
            self.locale = 'fr_FR.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'en_US.UTF-8'
            locale.setlocale(locale.LC_ALL, self.locale)
               
    #def test2_regex_tokenizer(self):
    #    tokenizerTester = TokenizerTests( self.data, self.locale )
    #    corpora = tokenizerTester.init_corpus( 1 )
    #    corpora = tokenizerTester.regexp_tokenizer( corpora );
    def test1_wordpunct_tokenizer(self):
        tokenizerTester = TokenizerTests( self.data, self.locale )
        corpora2 = tokenizerTester.init_corpus( 1 )
        corpora2 = tokenizerTester.wordpunct_tokenizer( corpora2 );

class TokenizerTests: 
    def __init__( self, data, locale ):
        self.data = data
        self.locale = locale
        # initialize stopwords object
        try:
            self.stopwords = PyTextMiner.StopWords("file://t/stopwords/en.txt", locale="en_US")
        except:
            print "unable to find the stopwords file"

    def init_corpus(self, repeatFactor ):
        newcorpora = PyTextMiner.Corpora(id="TestCorpora1")
        corpNum = 0
        for c in self.data['corpus']:
            corpus = PyTextMiner.Corpus( name=c['name'], number=corpNum)
            corpNum += 1
            docnum = 0
            for i in range( 0, repeatFactor ):
                for doc in c['documents']:
                    content = doc['content']
                    corpus.documents += [PyTextMiner.Document(
                        rawContent=doc, 
                        title=doc['title'],
                        targets=[PyTextMiner.Target(
                            rawTarget=content,
                            type='testType',
                            locale=self.locale,
                            minSize=1,
                            maxSize=3)],
                        author=doc['author'],
                        number=docnum,
                        )]
                docnum += 1
            newcorpora.corpora += [corpus]
        return newcorpora

    def regexp_tokenizer( self, corpora ):
        for corpus in corpora.corpora:
            for document in corpus.documents:
                for target in document.targets:
                    
                    print "----- RegexpTokenizer ----\n"
                    target.sanitizedTarget = PyTextMiner.Tokenizer.RegexpTokenizer.sanitize( input=target.rawTarget, forbiddenChars=target.forbiddenChars, emptyString=target.emptyString )
                    #print target.sanitizedTarget
                    
                    tokens = PyTextMiner.Tokenizer.RegexpTokenizer.tokenize( text=target.sanitizedTarget, separator=target.separator, emptyString=target.emptyString, stopwords=self.stopwords )
                    #target.tokens = PyTextMiner.Tokenizer.RegexpTokenizer.filterBySize( tokens )
                    #print target.tokens

                    target.ngrams = PyTextMiner.Tokenizer.RegexpTokenizer.ngramize(
                        minSize=target.minSize,
                        maxSize=target.maxSize,
                        tokens=target.tokens,
                        emptyString=target.emptyString,
                        stopwords=self.stopwords
                    )
                    #print(target.ngrams)
            corpus.ngramDocFreq()
        return corpora

    def wordpunct_tokenizer( self, corpora2 ): 
        for corpus in corpora2.corpora:
            for document in corpus.documents:
                for target in document.targets:

                    print "----- WordPunctTokenizer ----\n"
                    target.sanitizedTarget = PyTextMiner.Tokenizer.WordPunctTokenizer.sanitize(
                        input=target.rawTarget,
                        forbiddenChars=target.forbiddenChars,
                        emptyString=target.emptyString
                    )
                    #print target.sanitizedTarget
                    
                    target.tokens = PyTextMiner.Tokenizer.WordPunctTokenizer.tokenize(
                        text=target.sanitizedTarget,
                        emptyString=target.emptyString,
                        stopwords=self.stopwords
                    )
                    #for sentence in sentenceTokens:
                    #    target.tokens.append( PyTextMiner.Tokenizer.WordPunctTokenizer.filterBySize( sentence ) )
                    print target.tokens
                    
                    target.ngrams = PyTextMiner.Tokenizer.WordPunctTokenizer.ngramize(
                        minSize=target.minSize,
                        maxSize=target.maxSize,
                        tokens=target.tokens,
                        emptyString=target.emptyString, 
                        stopwords=self.stopwords
                    )
                    print target.ngrams 
            corpus.ngramDocFreq( targetType='testType' )
        return corpora2
                     
    def print_corpora(self, corpora):
        for corpus in corpora.corpora:
            print corpus
            for document in corpus.documents:
                print document
                for target in document.targets:
                    print target
                    for ngram in target.ngrams:
                        print ngram

if __name__ == '__main__':
    unittest.main()
