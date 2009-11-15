# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 6:32:44 PM$"

import string, re, pprint
import nltk
from PyTextMiner import NGram


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
        return sanitized.lower()

    @staticmethod
    def cleanPunct( text, emptyString, punct=u'[\,\.\;\:\!\?\"\'\[\]\{\}\(\)\<\>]' ):
        #print text
        noPunct = re.sub( punct, emptyString, text )
        return noPunct

    @staticmethod
    def tokenize( text, separator, emptyString, stopwords ):
        noPunct = RegexpTokenizer.cleanPunct( text, emptyString )
        tokens = re.split( separator, noPunct )
        cleanTokens = []
        count=0
        for tok in tokens:
            if stopwords.contains( [tok] ) is False:
                count += 1
                cleanTokens += [tok]
        print count
        return cleanTokens

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
    def ngramize( minSize, maxSize, tokens, emptyString, stopwords ):
        ngrams = set()
        count=0
        for n in range( minSize, maxSize +1 ):
            for i in range(len(tokens)): 
                if len(tokens) >= i+n:
                    if stopwords.contains( tokens[i:n+i] ) is False:
                        representation = emptyString.join( tokens[i:n+i] )
                        newngram = NGram(
                                    ngram = tokens[i:n+i],
                                    occs = 1,
                                    strRepr = representation,
                        )
                        if newngram in ngrams:
                            ngrams[ newngram ].occs += 1
                        else:
                            ngrams.add( newngram )
                    else:
                        count += 1
        print count
        return ngrams

class WordPunctTokenizer(RegexpTokenizer):
    """
    A tokenizer that divides a text into sentences
    then into sequences of alphabetic
    and non-alphabetic characters
    """
    @staticmethod
    def tokenize( text, emptyString, stopwords ):
        sentences = nltk.sent_tokenize(text)
        sentences = [nltk.WordPunctTokenizer().tokenize(RegexpTokenizer.cleanPunct( sent, emptyString)) for sent in sentences]
        cleanTokens = []
        count=0
        for tokens in sentences:
            for tok in tokens:
                if stopwords.contains( [tok] ) is False:
                    count += 1
                    cleanTokens += [tok]
        print count
        return sentences
    
    # TODO : keep the sentence structure ?
    @staticmethod
    def ngramize( minSize, maxSize, tokens, emptyString, stopwords ):
        ngrams = set()
        count =0
        for n in range( minSize, maxSize +1 ):
            for sent in tokens:
                for i in range(len(sent)):
                    if len(sent) >= i+n:
                        if stopwords.contains( sent[i:n+i] ) is False:
                            representation = emptyString.join( sent[i:n+i] )
                            newngram = NGram(
                                        ngram = sent[i:n+i],
                                        occs = 1,
                                        strRepr = representation,
                            )
                            if newngram in ngrams:
                                ngrams[ newngram ].occs += 1
                            else:
                                ngrams.add( newngram )
                        else:
                            count +=1
        print count
        return ngrams
