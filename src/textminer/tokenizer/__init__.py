# -*- coding: utf-8 -*-

__author__="jbilcke"
__date__ ="$Oct 20, 2009 6:32:44 PM$"

import string
import re


def sanitize( input, separator, forbidenChars ):
    """sanitized a text

    @return str: text
    """
    striped = string.strip( input )
    sanitized = re.sub( forbidenChars, separator, striped )
    output = re.sub( separator + '+', separator, sanitized ) 
    return output

def tokenize( text, min, max, separator ):
    def _ngrams( text, n, separator ):
        tokens = re.split( separator, text )
        return [tokens[i:n+i] for i in range(len(tokens)) if len(tokens)>=i+n]
    return [ _ngrams(text, n, separator) for n in range(min, max+1) ]
