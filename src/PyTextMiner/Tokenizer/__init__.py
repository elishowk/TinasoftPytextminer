# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 6:32:44 PM$"

import string, re, pprint
import nltk


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
