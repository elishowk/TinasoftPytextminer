# -*- coding: utf-8 -*-

# core modules
import time
from datetime import datetime

class Document:
    """a Document containing targets containing ngrams"""
    def __init__(
            self,
            rawContent,
            docNum=None,
            date=None,
            targets=None,
            title=None,
            author=None,
            index1=None,
            index2=None,
            metas=None,
            tokens=None,
            ngrams=None,
            ngramMin=1,
            ngramMax=2,
            forbChars="[^a-zA-Z0-9\s\@ÂÆÇÈÉÊÎÛÙàâæçèéêîĨôÔùûü\,\.\;\:\!\?\"\'\[\]\{\}\(\)\<\>]",
            ngramSep= u"[\s]+",
            ngramEmpty = " ",
        ):

        self.rawContent = rawContent
        self.docNum = docNum

        if date is None:
            self.date = None
        else:
            self.date = datetime.strftime(date, "%Y-%m-%d")
        # contains targets auto-generated unique IDs
        if targets is None: 
            targets = set([]) 
        self.targets = targets
        # basic metas
        self.title = title
        self.author = author
        self.index1 = index1
        self.index2 = index2
        self.metas = metas
        if tokens is None:
            tokens = []
        self.tokens = tokens
        # contains ngrams' generated unique IDs
        if ngrams is None:
            ngrams= {}
        self.ngrams = ngrams
        # tokenization params
        self.ngramMin=ngramMin
        self.ngramMax=ngramMax
        self.forbChars=forbChars
        self.ngramSep= ngramSep
        self.ngramEmpty = ngramEmpty


    def __str__(self):
        #return self.rawContent.encode('utf-8')
        return "%s"%self.rawContent

    def __repr__(self):
        #return "<%s>"%self.rawContent.encode('utf-8')
        return "<%s>"%self.rawContent
