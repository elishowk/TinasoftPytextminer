# -*- coding: utf-8 -*-

__author__="jbilcke, Elias Showk"
__date__ ="$Oct 20, 2009 6:32:44 PM$"

import string
import re

class RegexpTokenizer():

    @staticmethod
    def sanitize( input, separator, forbiddenChars ):
        """sanitized a text

        @return str: text
        """
        striped = string.strip( input )
        sanitized = re.sub( forbiddenChars, separator, striped )
        output = re.sub( separator + '+', separator, sanitized ) 
        return output

    @staticmethod
    def tokenize( text, min, max, separator ):
        if max >= 1 and max >= min:
            def _ngrams( text, n, separator ):
                tokens = re.split( separator, text )
                return [tokens[i:n+i] for i in range(len(tokens)) if len(tokens)>=i+n]
            return [ _ngrams(text, n, separator) for n in range(min, max+1) ]
