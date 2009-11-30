# -*- coding: utf-8 -*-

__author__="Elias Showk"
# core modules
import time
from datetime import datetime

class Document:
    """a Document containing targets containing ngrams"""

    def __init__(
            self,
            rawContent,
            docNum=None,
            targets=None,
            tokens=None,
            ngrams=None,
            ngramMin=1,
            ngramMax=2,
            forbChars="[^a-zA-Z\s\@ÂÆÇÈÉÊÎÛÙàâæçèéêîĨôÔùûü\,\.\;\:\!\?\"\'\[\]\{\}\(\)\<\>]",
            ngramSep= u"[\s]+",
            ngramEmpty = " ",
            **opt
        ):
        self.rawContent = rawContent
        self.docNum = docNum
        # sanitized targets
        if targets is None: 
            targets = set([])
        self.targets = targets
        # tokens = words
        if tokens is None:
            tokens = []
        self.tokens = tokens
        # ngrams unique IDs
        #if ngrams is None:
        #    ngrams= set([])
        #self.ngrams = ngrams
        # tokenization params
        self.ngramMin=ngramMin
        self.ngramMax=ngramMax
        self.forbChars=forbChars
        self.ngramSep= ngramSep
        self.ngramEmpty = ngramEmpty
        # optional document fields
        self.load_options( opt )


    def __str__(self):
        #return self.rawContent.encode('utf-8')
        return "%s"%self.rawContent

    def __repr__(self):
        #return "<%s>"%self.rawContent.encode('utf-8')
        return "<%s>"%self.rawContent
    
    def load_options(self, options):
        for attr, value in options.iteritems():
                setattr(self,attr,value)
