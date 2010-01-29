# -*- coding: utf-8 -*-

__author__="Elias Showk"
__date__ ="$Oct 20, 2009 6:32:44 PM$"

import string, re
# warning : nltk imports it's own copy of pyyaml
import nltk
nltk.data.path = ['shared/nltk_data']
from tinasoft.pytextminer import ngram

class RegexpTokenizer():
    """
    A homemade tokenizer that splits a text into tokens
    given a regexp used as a separator
    """
    @staticmethod
    def sanitize( input, forbiddenChars, emptyString ):
        """sanitized a text

        @return str: text
        """
        striped = string.strip( input )
        #replaces forbidden characters by a separator
        sanitized = re.sub( forbiddenChars, emptyString, striped )
        return sanitized

    @staticmethod
    def cleanPunct( text, emptyString, punct=u'[\,\.\;\:\!\?\"\[\]\{\}\(\)\<\>]' ):
        #print text
        noPunct = re.sub( punct, emptyString, text )
        return noPunct

    ### deprecated
    @staticmethod
    def tokenize( text, separator, emptyString, stopwords=None ):
        noPunct = RegexpTokenizer.cleanPunct( text, emptyString )
        tokens = re.split( separator, noPunct )
        #if stopwords not None
        #cleanTokens = []
        #count=0
        #for tok in tokens:
        #    if stopwords.contains( [tok] ) is False:
        #        count += 1
        #        cleanTokens += [tok]
        #print "tokens stopped :", count
        #return cleanTokens
        return tokens

    ### deprecated
    @staticmethod
    def filterBySize( words, min=2, max=255 ):
        """Filter a list of word by size

        > filterBySize(["a", "aaa", "aa", "aaaa", "aa"], min=2 , max=3)
        ["aaa", "aa", "aa"]
        """
        filtered = []
        for word in words:
            length = len(word)
            if length >= min and length <= max:
                filtered += [ word ]
        return filtered

    @staticmethod
    def ngramize( minSize, maxSize, tokens, emptyString, stopwords=None ):
        """
            returns a dict of NGram instances
            using the optional stopwords object to filter by ngram length
            tokens = [sentence1, sentence2, etc]
            sentences = list of words = [word,postagged_word]
        """
        ngrams = {}
        for n in range( minSize, maxSize +1 ):
            for sent in tokens:
                for i in range(len(sent)):
                    if len(sent) >= i+n:
                        content = [tagged[0] for tagged in sent[i:n+i]]
                        ng = ngram.NGram( content, occs = 1, postag = sent[i:n+i] )
                        if stopwords is None or stopwords.contains( ng ) is False:
                            # if ngrams already exists in document, only increments occs
                            if ng.id in ngrams:
                                ngrams[ ng.id ].occs += 1
                            else:
                                ngrams[ ng.id ] = ng
        return ngrams

class TreeBankWordTokenizer(RegexpTokenizer):
    """
    A tokenizer that divides a text into sentences
    then cleans the punctuation
    before tokenizing using nltk.TreebankWordTokenizer()
    """
    def tokenize( self, text, emptyString, stopwords=None ):
        sentences = nltk.sent_tokenize(text)
        # WARNING : only works on english
        sentences = [nltk.TreebankWordTokenizer().\
            tokenize(RegexpTokenizer.cleanPunct( sent, emptyString )) \
            for sent in sentences]
        return sentences

    def extract( self, doc, content, stopwords ):
        sanitizedTarget = self.sanitize(
            content,
            doc['forbChars'],
            doc['ngramEmpty']
        )
        sentenceTokens = self.tokenize(
            text = sanitizedTarget,
            emptyString = doc['ngramEmpty'],
        )
        for sentence in sentenceTokens:
            doc['tokens'].append( tagger.TreeBankPosTagger.posTag( sentence ) )
        return self.ngramize(
            minSize = doc['ngramMin'],
            maxSize = doc['ngramMax'],
            tokens = doc['tokens'],
            emptyString = doc['ngramEmpty'],
            stopwords = stopwords,
        )
