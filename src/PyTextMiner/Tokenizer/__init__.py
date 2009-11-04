# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 6:32:44 PM$"

import string, re, pprint
from nltk import WordPunctTokenizer, sent_tokenize

class RegexpTokenizer():

    @staticmethod
    def sanitize( input, separator, forbiddenChars ):
        """sanitized a text

        @return str: text
        """
        striped = string.strip( input )
        # replaces forbidden characters by a separator
        sanitized = re.sub( forbiddenChars, separator, striped )
        #removes redundant separators
        output = re.sub( separator + '+', separator, sanitized ) 
        return output

    @staticmethod
    def tokenize( text, length, separator ):
        tokens = re.split( separator, text )
        return [tokens[i:length+i] for i in range(len(tokens)) if len(tokens)>=i+length]

class NltkTokenizer(RegexpTokenizer):

    @staticmethod
    def tokenize( text ):
        #sentences = sent_tokenize(text)
        tokens = [WordPunctTokenizer().tokenize(sent) for sent in text]
        return sentences
        #sentences = 
