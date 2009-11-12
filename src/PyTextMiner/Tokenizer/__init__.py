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
    def tokenize( text, separator ):
        tokens = re.split( separator, text )
        return tokens

    @staticmethod
    def ngrams( minSize, maxSize, tokens, emptyString):
        ngrams={}
        for n in range( minSize, maxSize +1 ):
            for i in range(len(tokens)):
                if len(tokens) >= i+n:
                    representation = emptyString.join( tokens[i:n+i] )
                    if ngrams.has_key( representation ):
                        ngrams[ representation ].occs += 1
                    else:
                        ngrams[ representation ] = NGram(
                                ngram = tokens[i:n+i],
                                occs = 1,
                                str = representation,
                        )
        return ngrams

class WordPunctTokenizer(RegexpTokenizer):
    """
    A tokenizer that divides a text into sentences
    then into sequences of alphabetic
    and non-alphabetic characters
    """
    @staticmethod
    def tokenize( text ):
        sentences = nltk.sent_tokenize(text)
        sentences = [nltk.WordPunctTokenizer().tokenize(sent) for sent in sentences]
        return sentences
    
    # TODO : keep the sentence structure ?
    @staticmethod
    def ngrams( minSize, maxSize, tokens, emptyString):
        ngrams={}
        for n in range( minSize, maxSize +1 ):
            for sent in tokens:
                for i in range(len(sent)):
                    if len(sent) >= i+n:
                        representation = emptyString.join( sent[i:n+i] )
                        if ngrams.has_key( representation ):
                            ngrams[ representation ].occs += 1
                        else:
                            ngrams[ representation ] = NGram(
                                    ngram = sent[i:n+i],
                                    occs = 1,
                                    str = representation,
                            )
        return ngrams
