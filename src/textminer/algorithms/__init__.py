# -*- coding: utf-8 -*-

__author__="jbilcke"
__date__ ="$Oct 20, 2009 6:32:44 PM$"

from chardet import detect
import string

def sanitize(input):
        """sanitized a text

	@return str: text
	"""
        # create an unicode obj from an input charset detected with heuristics
        #output = unicode(input, detect(input)['encoding'])
        output = input#unicode(input, 'utf-8')
        output = string.strip(output, " ")
        #text = strip(text, " .;") # remove spaces or dot before and after the string
        for p in string.punctuation:
            output = output.replace(p,'')
	return output

def ngrams(data, length=3):
    def _ngrams(d, n):
        d = d.split()
        return [d[i:n+i] for i in range(len(d)) if len(d)>=i+n]
    return [_ngrams(data,n) for n in range(length)][1:]


