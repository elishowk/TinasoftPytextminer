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
        return sanitized

    @staticmethod
    def tokenize( text, separator, minSize=2, maxSize=255 ):
        tokens = re.split( separator, text )
        tokens = self.filterBySize( tokens, minSize, maxSize )
        return tokens

    @staticmethod
    def filterBySize( words, min, max ):
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
    def ngrams( minSize, maxSize, tokens, emptyString):
        ngrams = set()
        for n in range( minSize, maxSize +1 ):
            for i in range(len(tokens)):
                if len(tokens) >= i+n:
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
        return ngrams

class WordPunctTokenizer(RegexpTokenizer):
    """
    A tokenizer that divides a text into sentences
    then into sequences of alphabetic
    and non-alphabetic characters
    """
    @staticmethod
    def tokenize( text, minSize=2, maxSize=255 ):
        sentences = nltk.sent_tokenize(text)
        sentences = [nltk.WordPunctTokenizer().tokenize(sent) for sent in sentences]
        sentences = self.filterBySize( sentences, minSize, maxSize )
        return sentences
    
    # TODO : keep the sentence structure ?
    @staticmethod
    def ngrams( minSize, maxSize, tokens, emptyString):
        ngrams = set()
        for n in range( minSize, maxSize +1 ):
            for sent in tokens:
                for i in range(len(sent)):
                    if len(sent) >= i+n:
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
        return ngrams
